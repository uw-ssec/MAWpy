"""
=============================
Trace Segmentation Clustering
=============================

Segment GPS/Cellular traces of a user into stays based on spatial and temporal constraints.
It identifies group of traces within a particular spatial threshold for which the user have stayed at for more than
duration threshold (temporal constraint)

input:
    gps stay information / cellular stay information
    spatial threshold
    duration constraint threshold (for detect common stay)
output:
    potential stays represented by stay locations

"""
import logging

import numpy as np
import pandas as pd

from mawpy.constants import (USER_ID, UNIX_START_DATE, ORIG_LAT, ORIG_LONG, UNIX_START_T,
                             STAY_LAT, STAY_LONG, STAY_DUR, STAY, TSC_COLUMNS)
from mawpy.distance import distance
from mawpy.utilities import (
    get_combined_stay,
    get_stay_groups,
    get_preprocessed_dataframe,
    get_list_of_chunks_by_column,
    execute_parallel,
    validate_input_args
)

logger = logging.getLogger(__name__)


def _get_diameter_constraint_exceed_index(starting_index: int, point_to_check: int,
                                          latitudes_list: list[float], longitudes_list: list[float],
                                          spatial_constraint: float) -> tuple:
    """
       Find the first index where the distance between a point and a reference point exceeds a spatial constraint.

       Starting from the 'starting_index', this function checks the distance between each point in the range
       [starting_index, point_to_check) and the point at 'point_to_check'. It returns the index of the first
       point where this distance exceeds the spatial constraint.

       Parameters
       ----------
       starting_index : int
           The starting index from which to begin the search.
       point_to_check : int
           The index of the reference point for distance comparison.
       latitudes_list : list of float
           List of latitudes for the points.
       longitudes_list : list of float
           List of longitudes for the points.
       spatial_constraint : float
           The spatial constraint distance threshold.

       Returns
       -------
       tuple
           A tuple containing a boolean indicating whether a point exceeding the spatial constraint was found,
           and the index of the point if found, otherwise -1.
    """

    # Map to store distance values to avoid re-computation for duplicate points
    distance_map = {}
    for i in range(starting_index, point_to_check):
        point_key = (latitudes_list[i], longitudes_list[i])

        distance_for_point = distance_map.get(point_key, -1)
        if distance_for_point == -1:
            distance_for_point = distance(latitudes_list[i], longitudes_list[i],
                                               latitudes_list[point_to_check], longitudes_list[point_to_check])
            distance_map[point_key] = distance_for_point
        if spatial_constraint < distance_for_point:
            return True, i

    return False, -1


def _does_duration_threshold_exceed(point_i: int, point_j: int, timestamps_list: list, duration_constraint: float) \
        -> bool:
    """
    Check if the duration between two points exceeds a specified threshold.

    This function compares the timestamps of two points and determines if the time difference between them
    exceeds the given duration constraint.

    Parameters
    ----------
    point_i : int
        The index of the first point.
    point_j : int
        The index of the second point.
    timestamps_list : list
        List of timestamps corresponding to the points.
    duration_constraint : float
        The duration threshold.

    Returns
    -------
    bool
        True if the duration exceeds the threshold, False otherwise.
    """
    return timestamps_list[point_j] - timestamps_list[point_i] >= duration_constraint


def _get_df_with_stays(each_day_df: pd.DataFrame, spatial_constraint: float, dur_constraint: float) -> pd.DataFrame:
    """
    Identify and group traces into stays for a user on a given day.

    This function analyzes the trace of a user for a specific day and groups consecutive points into 'stays'
    based on the spatial and duration constraints. It assigns the stay's latitude, longitude, and duration
    to each trace.

    Parameters
    ----------
    each_day_df : pd.DataFrame
        DataFrame containing the user's trace for a single day.
    spatial_constraint : float
        The spatial constraint for grouping traces into stays.
    dur_constraint : float
        The minimum duration required for a group of traces to be considered a stay.

    Returns
    -------
    pd.DataFrame
        The DataFrame with added columns for stay latitude, stay longitude, and stay duration.
    """
    latitudes_for_day = each_day_df[ORIG_LAT].to_numpy()
    longitudes_for_day = each_day_df[ORIG_LONG].to_numpy()
    timestamps_for_day = each_day_df[UNIX_START_T].to_numpy()
    number_of_traces_for_day = len(each_day_df)

    stay_lat = np.full(number_of_traces_for_day, -1.0)
    stay_long = np.full(number_of_traces_for_day, -1.0)
    stay_dur = np.zeros(number_of_traces_for_day)

    start = 0
    end = start
    group_found = False
    while start <= end < number_of_traces_for_day:
        has_exceeded, exceed_index = _get_diameter_constraint_exceed_index(start, end, latitudes_for_day,
                                                                           longitudes_for_day,
                                                                           spatial_constraint)
        if not group_found:

            if has_exceeded:
                start = exceed_index + 1
                continue

            if _does_duration_threshold_exceed(start, end, timestamps_for_day, dur_constraint):
                group_found = True
            end += 1
        else:
            if has_exceeded:
                stay_lat[start: end] = np.mean(latitudes_for_day[start: end], dtype=float)
                stay_long[start: end] = np.mean(longitudes_for_day[start: end], dtype=float)
                stay_dur[start: end] = timestamps_for_day[end - 1] - timestamps_for_day[start]
                start = end
                group_found = False
            else:
                end += 1

    if group_found:
        stay_lat[start: end] = np.mean(latitudes_for_day[start: end], dtype=float)
        stay_long[start: end] = np.mean(longitudes_for_day[start: end], dtype=float)
        stay_dur[start: end] = timestamps_for_day[end - 1] - timestamps_for_day[start]

    each_day_df[STAY_LAT] = stay_lat
    each_day_df[STAY_LONG] = stay_long
    each_day_df[STAY_DUR] = stay_dur

    return each_day_df


def _run_for_user(df_by_user: pd.DataFrame, spatial_constraint: float, dur_constraint: float) -> pd.DataFrame:
    """
    Process trace data for a single user to identify stays.

    This function groups trace data by day and applies stay detection logic to each group based on spatial and
    duration constraints. It then combines stays across days.

    Parameters
    ----------
    df_by_user : pd.DataFrame
        DataFrame containing trace data for a single user.
    spatial_constraint : float
        The spatial constraint for grouping traces into stays.
    dur_constraint : float
        The minimum duration required for a group of traces to be considered a stay.

    Returns
    -------
    pd.DataFrame
        The processed DataFrame with identified stays and combined stay information.
    """
    df_by_user = df_by_user.sort_values(by=[UNIX_START_T], ascending=True)
    df_with_stay = df_by_user.groupby(UNIX_START_DATE).apply(
        lambda x: _get_df_with_stays(x, spatial_constraint, dur_constraint))
    df_with_stay[STAY] = get_stay_groups(df_with_stay)

    df_with_stay_added = get_combined_stay(df_with_stay)

    return df_with_stay_added


def _run(df_by_user_chunk: pd.DataFrame, args: tuple) -> pd.DataFrame:
    """
    Process a chunk of user trace data for stay detection.

    This function applies the stay detection process to a chunk of trace data for multiple users,
    grouped by user ID.

    Parameters
    ----------
    df_by_user_chunk : pd.DataFrame
        A chunk of the DataFrame containing trace data for multiple users.
    args : tuple
        A tuple containing the spatial and duration constraints.

    Returns
    -------
    pd.DataFrame
        The processed DataFrame with identified stays for each user in the chunk.
    """
    spatial_constraint, dur_constraint = args
    df_by_user_chunk = (df_by_user_chunk.groupby(USER_ID)
                        .apply(lambda x: _run_for_user(x, spatial_constraint, dur_constraint)))
    return df_by_user_chunk


def trace_segmentation_clustering(output_file: str, spatial_constraint: float, dur_constraint: float,
                                  input_df: pd.DataFrame | None = None, input_file: str = None) -> pd.DataFrame | None:
    """
    Perform trace segmentation clustering based on spatial and duration constraints.

    This function processes user trace data to identify and cluster stays based on spatial and duration
    constraints. The resulting DataFrame is saved to a specified output file.

    Parameters
    ----------
    output_file : str
        The path to the output file where the results will be saved.
    spatial_constraint : float
        The spatial constraint for grouping traces into stays.
    dur_constraint : float
        The minimum duration required for a group of traces to be considered a stay.
    input_df : pd.DataFrame, optional
        The input DataFrame containing trace data, by default None.
    input_file : str, optional
        The path to the input file containing trace data, by default None.

    Returns
    -------
    pd.DataFrame | None
        The processed DataFrame with identified stays, or None if input data is not provided.
    """
    if input_df is None and input_file is None:
        logger.error("At least one of input file path or input dataframe is required")
        return None

    if input_df is None:
        input_df = get_preprocessed_dataframe(input_file)

    user_id_chunks = get_list_of_chunks_by_column(input_df, USER_ID)
    validate_input_args(duration_constraint=dur_constraint, spatial_constraint=spatial_constraint)
    args = (spatial_constraint, dur_constraint)
    output_df = execute_parallel(user_id_chunks, input_df, _run, args)

    output_columns = list(set(TSC_COLUMNS) & set(output_df.columns))
    output_df = output_df[output_columns]
    output_df.dropna(how="all")
    output_df.to_csv(output_file, columns=sorted(output_df.columns), index=False)
    return output_df
