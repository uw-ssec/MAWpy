import logging

import numpy as np
import pandas as pd

from mawpy.constants import (USER_ID, UNIX_START_DATE, ORIG_LAT, ORIG_LONG, UNIX_START_T,
                             STAY_LAT, STAY_LONG, STAY_DUR, STAY)
from mawpy.distance import distance
from mawpy.utilities.common import get_combined_stay, get_stay_groups
from mawpy.utilities.preprocessing import get_preprocessed_dataframe, get_list_of_chunks_by_column, execute_parallel

logger = logging.getLogger(__name__)


def _get_diameter_constraint_exceed_index(starting_index: int, point_to_check: int,
                                          latitudes_list: list[float], longitudes_list: list[float],
                                          spatial_constraint: float) -> tuple:
    """
        Starting from the 'starting_index', find the first index position for which distance between
        point 'i' and the point at index 'point_to_check' is more than the spatial constraint.

        If no such points are found between [starting_index, point_to_check) then return -1
    """
    for i in range(starting_index, point_to_check):
        if spatial_constraint < distance(latitudes_list[i], longitudes_list[i],
                                         latitudes_list[point_to_check], longitudes_list[point_to_check]):
            return True, i

    return False, -1


def _does_duration_threshold_exceed(point_i: int, point_j: int, timestamps_list: list, duration_constraint: float) \
        -> bool:
    """
        Checks if the observed time difference between point j and point i is greater than duration_constraint.
    """
    if int(timestamps_list[point_j]) - int(timestamps_list[point_i]) > duration_constraint:
        return True
    return False


def _get_df_with_stays(each_day_df: pd.DataFrame, spatial_constraint: float, dur_constraint: float) -> pd.DataFrame:

    """
        For the trace of a user on a given day, the function calculates and assigns stay_lat and stay_long
        to each of the daily trace.

        It groups together traces into a 'stay' for which time difference between
            all the calculated diameter is within the spatial constraint and
            the first and the last point exceeds duration_threshold
    """
    latitudes_for_day = each_day_df[ORIG_LAT].to_numpy()
    longitudes_for_day = each_day_df[ORIG_LONG].to_numpy()
    timestamps_for_day = each_day_df[UNIX_START_T].to_numpy()
    number_of_traces_for_day = len(each_day_df)
    stay_lat = np.full(number_of_traces_for_day, -1.0)
    stay_long = np.full(number_of_traces_for_day, -1.0)
    stay_dur = np.full(number_of_traces_for_day, -1)

    start = 0
    end = start + 1
    group_found = False
    while start < end < number_of_traces_for_day:
        has_exceeded, exceed_index = _get_diameter_constraint_exceed_index(start, end, latitudes_for_day, longitudes_for_day,
                                                             spatial_constraint)
        if not group_found:

            if has_exceeded:
                start = exceed_index + 1
                if start == end:
                    end += 1
                continue

            if _does_duration_threshold_exceed(start, end, timestamps_for_day, dur_constraint):
                group_found = True
            end += 1
        else:
            if has_exceeded:
                stay_lat[start: end] = np.mean(latitudes_for_day[start: end])
                stay_long[start: end] = np.mean(longitudes_for_day[start: end])
                stay_dur[start: end] = timestamps_for_day[end - 1] - timestamps_for_day[start]
                start = end
                end = start + 1
                group_found = False
            else:
                end += 1

    each_day_df[STAY_LAT] = stay_lat
    each_day_df[STAY_LONG] = stay_long
    each_day_df[STAY_DUR] = stay_dur

    return each_day_df


def _run_for_user(df_by_user: pd.DataFrame, spatial_constraint: float, dur_constraint: float) -> pd.DataFrame:
    df_with_stay = df_by_user.groupby(UNIX_START_DATE).apply(lambda x: _get_df_with_stays(x, spatial_constraint, dur_constraint))
    df_with_stay[STAY] = get_stay_groups(df_with_stay)

    df_with_stay_added = get_combined_stay(df_with_stay)

    return df_with_stay_added


def _run(df_by_user_chunk: pd.DataFrame, args: tuple) -> pd.DataFrame:
    spatial_constraint, dur_constraint = args
    df_by_user_chunk = (df_by_user_chunk.groupby(USER_ID)
                        .apply(lambda x: _run_for_user(x, spatial_constraint, dur_constraint)))
    return df_by_user_chunk


def trace_segmentation_clustering(input_file: str, output_file: str, spatial_constraint: float,
                                  dur_constraint: float) -> pd.DataFrame:


    input_df = get_preprocessed_dataframe(input_file)
    user_id_chunks = get_list_of_chunks_by_column(input_df, USER_ID)
    # input_df.set_index(keys=[USER_ID], inplace=True)
    args = (spatial_constraint, dur_constraint)
    output_df = execute_parallel(user_id_chunks, input_df, _run, args)
    output_df.dropna(how="all")
    output_df.to_csv(output_file, index=False)
    return output_df
