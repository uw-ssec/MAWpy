import logging
import pandas as pd

from mawpy.constants import UNIX_START_T, USER_ID, STAY_DUR, STAY_LAT, STAY_LONG, STAY_UNC, STAY, UNIX_START_DATE
from mawpy.utilities import (
    get_preprocessed_dataframe,
    get_list_of_chunks_by_column,
    execute_parallel,
    validate_input_args
)

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


def _run_for_user(df_by_user_date: pd.DataFrame, duration_constraint: float, order_of_execution: int = 1) -> pd.DataFrame:
    """
        Calculate duration of stays for each user.
    """
    if order_of_execution == 1:
        df_by_user_date[STAY_DUR] = -1

    df_by_user_stay = df_by_user_date.groupby([STAY, USER_ID]).apply(lambda x: _get_stay_duration_for_group(x))
    df_by_user_stay.loc[df_by_user_stay[STAY_LAT] == -1, STAY_DUR] = -1
    df_by_user_stay.loc[df_by_user_stay[STAY_DUR] < duration_constraint, [STAY_LAT, STAY_LONG, STAY_UNC, STAY_DUR]] = (
        -1, -1, -1, -1)

    return df_by_user_stay


def _run(df_by_user: pd.DataFrame, args: tuple) -> pd.DataFrame:
    assert len(args) == 1, "Expected a single dur_constraint argument"
    dur_constraint = args[0]
    df_by_user_date = df_by_user.groupby(UNIX_START_DATE).apply(lambda x: _run_for_user(x, dur_constraint))
    return df_by_user_date


def update_stay_duration(output_file: str, dur_constraint: float,
                         input_df: pd.DataFrame | None = None, input_file: str = None) -> pd.DataFrame | None:
    if input_df is None and input_file is None:
        logger.error("At least one of input file path or input dataframe is required")
        return None

    if input_df is None:
        input_df = get_preprocessed_dataframe(input_file)

    user_id_chunks = get_list_of_chunks_by_column(input_df, USER_ID)
    validate_input_args(duration_constraint=dur_constraint)
    args = (dur_constraint,)
    output_df = execute_parallel(user_id_chunks, input_df, _run, args)
    output_df.dropna(how="all")
    output_df.to_csv(output_file, index=False)
    return output_df
