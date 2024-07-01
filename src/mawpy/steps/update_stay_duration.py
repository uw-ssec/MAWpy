import psutil
import time
import pandas as pd

from multiprocessing import Pool
from multiprocessing import Lock, cpu_count

from mawpy.constants import UNIX_START_DATE, UNIX_START_T, USER_ID, STAY_DUR, STAY_LAT, STAY_LONG, STAY_UNC
from mawpy.utilities.preprocessing import get_preprocessed_dataframe, get_list_of_chunks_by_column


def init(this_lock: Lock) -> None:
    global lock
    lock = this_lock


def get_stay_duration_for_group(df_per_group: pd.DataFrame) -> float:
    return (df_per_group.iloc[-1][UNIX_START_T] +
            max(0, df_per_group.iloc[-1][STAY_DUR]) -
            df_per_group.iloc[0][UNIX_START_T])


def _run_for_user(df_by_user: pd.DataFrame, duration_constraint: float, order_of_execution: int = 1) -> pd.DataFrame:
    df_by_user[[STAY_LAT, STAY_LONG, STAY_UNC]] = (df_by_user[[STAY_LAT, STAY_LONG, STAY_UNC]]
                                                   .apply(pd.to_numeric))
    df_by_user = df_by_user.sort_values(by=[UNIX_START_DATE], ascending=True)
    df_by_user[STAY_DUR] = df_by_user.apply(lambda x: x if order_of_execution != 1 else -1, axis=1)

    df_by_user['stay_group'] = ((df_by_user[[STAY_LAT, STAY_LONG]] != df_by_user[[STAY_LAT, STAY_LONG]].shift())
                                .any(axis=1).cumsum())
    df_by_user[STAY_DUR] = df_by_user.groupby('stay_group').apply(get_stay_duration_for_group)

    df_by_user.loc[df_by_user[STAY_LAT] == -1, STAY_DUR] = -1
    df_by_user.loc[df_by_user[STAY_DUR] < duration_constraint, [STAY_LAT, STAY_LONG, STAY_UNC, STAY_DUR]] = (
        -1, -1, -1, -1)

    return df_by_user


def _run(args: tuple) -> pd.DataFrame:
    df_by_user, dur_constraint = args
    df_by_user = _run_for_user(df_by_user, dur_constraint)
    return df_by_user


def update_stay_duration(input_file: str, output_file: str, dur_constraint: float) -> None:
    this_lock = Lock()
    pool = Pool(1, initializer=init, initargs=(this_lock,))

    input_df = get_preprocessed_dataframe(input_file)
    user_id_chunks = get_list_of_chunks_by_column(input_df, USER_ID)

    chunk_count = 0
    df_output_list =[]
    for each_chunk in user_id_chunks:
        print(
            f"Start processing bulk: {++chunk_count} at "
            f"time: {time.strftime('%m%d-%H:%M')} memory: {psutil.virtual_memory().percent}"
        )
        tasks = [
            pool.apply_async(_run, (task,))
            for task in [
                (input_df[input_df[USER_ID] == user], dur_constraint)
                for user in each_chunk
            ]
        ]
        df_output_list_per_user = [t.get() for t in tasks]
        df_output_list.extend(df_output_list_per_user)

    pool.close()
    pool.join()

    df_output = pd.concat(df_output_list)
    unique_users = df_output[USER_ID].unique()
    df_output.dropna(how="all")
    df_output.to_csv(output_file, index=False)

