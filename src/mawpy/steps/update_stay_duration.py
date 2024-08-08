"""
====================
Update Stay Duration
====================

Update the duration of detected stays in user data by recalculating based on the first and last traces within each stay.
This method processes a user's trace data to accurately compute the duration of each stay. It groups consecutive traces that represent a stay and calculates the total duration by taking the difference between the last and first trace timestamps within each group.

input:
    gps stay information / cellular stay information
    duration constraint threshold : The minimum duration required to consider a set of points as a stay
output:
    A DataFrame with updated stay durations, ensuring that each stay accurately reflects the time spent at that location.

"""
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
    Calculate the stay duration for traces grouped by the same STAY.

    This function computes the stay duration by calculating the difference between the timestamps
    of the last and first trace within the same stay group.

    Parameters
    ----------
    df_per_group : pd.DataFrame
        DataFrame containing traces with the same STAY.

    Returns
    -------
    pd.DataFrame
        The DataFrame with updated stay duration for the group.
    """
    df_per_group[STAY_DUR] = (df_per_group.iloc[-1][UNIX_START_T] +
                              max(0, df_per_group.iloc[-1][STAY_DUR]) -
                              df_per_group.iloc[0][UNIX_START_T])
    return df_per_group


def _run_for_user(df_by_user_date: pd.DataFrame, duration_constraint: float, order_of_execution: int = 1) -> pd.DataFrame:
    """
    Calculate stay durations for a user's trace data.

    This function processes a user's trace data, computing the duration of stays for each stay group.
    Stays with a duration below the threshold are marked as -1, -1.

    Parameters
    ----------
    df_by_user_date : pd.DataFrame
        DataFrame containing the user's trace data for a specific date.
    duration_constraint : float
        The minimum duration required for a group of traces to be considered a valid stay.
    order_of_execution : int, optional
        The execution order for the process, by default 1.

    Returns
    -------
    pd.DataFrame
        The processed DataFrame with stay durations calculated.
    """
    if order_of_execution == 1:
        df_by_user_date[STAY_DUR] = -1

    df_by_user_stay = df_by_user_date.groupby([STAY, USER_ID]).apply(lambda x: _get_stay_duration_for_group(x))
    df_by_user_stay.loc[df_by_user_stay[STAY_LAT] == -1, STAY_DUR] = -1
    df_by_user_stay.loc[df_by_user_stay[STAY_DUR] < duration_constraint, [STAY_LAT, STAY_LONG, STAY_UNC, STAY_DUR]] = (
        -1, -1, -1, -1)

    return df_by_user_stay


def _run(df_by_user: pd.DataFrame, args: tuple) -> pd.DataFrame:
    """
    Process a user's trace data to calculate stay durations.

    This function groups trace data by date and applies the stay duration calculation for each date.
    It expects a single duration constraint argument.

    Parameters
    ----------
    df_by_user : pd.DataFrame
        DataFrame containing trace data for a user.
    args : tuple
        A tuple containing the duration constraint.

    Returns
    -------
    pd.DataFrame
        The processed DataFrame with calculated stay durations.
    """
    assert len(args) == 1, "Expected a single dur_constraint argument"
    dur_constraint = args[0]
    df_by_user_date = df_by_user.groupby(UNIX_START_DATE).apply(lambda x: _run_for_user(x, dur_constraint))
    return df_by_user_date


def update_stay_duration(output_file: str, dur_constraint: float,
                         input_df: pd.DataFrame | None = None, input_file: str = None) -> pd.DataFrame | None:
    """
    Update the stay durations in user trace data based on a duration constraint.

    This function processes user trace data to calculate and update stay durations, applying a duration
    threshold to filter out stays. The resulting data is saved to an output file.

    Parameters
    ----------
    output_file : str
        The path to the output file where the results will be saved.
    dur_constraint : float
        The minimum duration required for a group of traces to be considered a valid stay.
    input_df : pd.DataFrame, optional
        The input DataFrame containing trace data, by default None.
    input_file : str, optional
        The path to the input file containing trace data, by default None.

    Returns
    -------
    pd.DataFrame | None
        The processed DataFrame with updated stay durations, or None if input data is not provided.
    """
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
    output_df.to_csv(output_file, columns=sorted(output_df.columns), index=False)
    return output_df
