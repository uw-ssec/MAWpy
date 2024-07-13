import logging
import pandas as pd

from mawpy.constants import UNIX_START_T, USER_ID, STAY_DUR, STAY_LAT, STAY_LONG, STAY_UNC, STAY
from mawpy.utilities.preprocessing import get_preprocessed_dataframe, get_list_of_chunks_by_column, execute_parallel

logger = logging.getLogger(__name__)


def _get_stay_duration_for_group(df_per_group: pd.DataFrame) -> pd.DataFrame:
    """
        For the traces with the same STAY, computes the stay duration by calculating the difference between
        timestamps of last and first trace
    """
    df_per_group[STAY_DUR] = (df_per_group.iloc[-1][UNIX_START_T] +
                              max(0, df_per_group.iloc[-1][STAY_DUR]) -
                              df_per_group.iloc[0][UNIX_START_T])
    return df_per_group


def _run_for_user(df_by_user: pd.DataFrame, duration_constraint: float, order_of_execution: int = 1) -> pd.DataFrame:
    """
        Calculate duration of stays for each user.
    """
    if order_of_execution == 1:
        df_by_user[STAY_DUR] = -1

    df_by_user = df_by_user.groupby(STAY).apply(lambda x: _get_stay_duration_for_group(x))
    df_by_user.loc[df_by_user[STAY_LAT] == -1, STAY_DUR] = -1
    df_by_user.loc[df_by_user[STAY_DUR] < duration_constraint, [STAY_LAT, STAY_LONG, STAY_UNC, STAY_DUR]] = (
        -1, -1, -1, -1)

    return df_by_user


def _run(df_by_user: pd.DataFrame, args: tuple) -> pd.DataFrame:
    dur_constraint = args[0]
    df_by_user = _run_for_user(df_by_user, dur_constraint)
    return df_by_user


def update_stay_duration(output_file: str, dur_constraint: float, input_df: pd.DataFrame | None, input_file: str = None) -> pd.DataFrame:

    if input_df is None and input_file is None:
        logger.error("At least one of input file path or input dataframe is required")

    if input_df is None:
        input_df = get_preprocessed_dataframe(input_file)

    user_id_chunks = get_list_of_chunks_by_column(input_df, USER_ID)
    args = (dur_constraint, )
    output_df = execute_parallel(user_id_chunks, input_df, _run, args)
    output_df.dropna(how="all")
    output_df.to_csv(output_file, index=False)
    return output_df
