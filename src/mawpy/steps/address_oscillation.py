"""
===================
Address Oscillation
===================

Identify and address oscillations in traces to improve the accuracy of detected stays.
This method processes a user's trace data to detect and correct oscillating traces—where the user
appears to move back and forth between locations within a short time frame. The corrected data helps in accurately identifying true stays by reducing noise from oscillations.

input:
    gps stay information / cellular stay information
    duration constraint threshold (required to consider a set of points as a stay.)
output:
    A DataFrame with oscillations addressed, improving the accuracy of detected stays and overall trace quality.

"""
import logging

import numpy as np
import pandas as pd

from mawpy.constants import USER_ID, UNIX_START_T, STAY_DUR, STAY_LAT, ORIG_LAT, ORIG_LONG, STAY_LONG, AO_COLUMNS
from mawpy.utilities import (get_preprocessed_dataframe, get_list_of_chunks_by_column, execute_parallel,
                             validate_input_args)

logger = logging.getLogger(__name__)


def _transform_trace(row: pd.Series) -> pd.Series:
    """
    Transform a row of the DataFrame by determining the duration, latitude, longitude,
    and whether it's a stay point or not.

    Parameters
    ----------
    row : pd.Series
        A row from the DataFrame.

    Returns
    -------
    pd.Series
        Transformed row with duration, latitude, longitude, and stay/not stay indicator.
    """
    # Determine duration, defaulting to 1 if not present or stay_dur
    duration = 1 if STAY_DUR not in row or row[STAY_DUR] == -1 else row[STAY_DUR]

    # Determine latitude, defaulting to original if not present or stay_lat
    lat = row[ORIG_LAT] if STAY_LAT not in row or row[STAY_LAT] == -1 else row[STAY_LAT]

    # Determine longitude, defaulting to original if not present or stay_long
    long = row[ORIG_LONG] if STAY_LONG not in row or row[STAY_LONG] == -1 else row[STAY_LONG]

    # Determine if it's a stay point, defaulting to 0 if not present or 1
    stay_not = 0 if STAY_LAT not in row or row[STAY_LAT] == -1 else 1

    return pd.Series([duration, lat, long, stay_not])


def _get_stays_by_lat_long_dur(df_by_user: pd.DataFrame) -> np.ndarray:
    """
    Group consecutive traces that have the same values for latitude, longitude, and duration.

    Parameters
    ----------
    df_by_user : pd.DataFrame
        DataFrame per user.

    Returns
    -------
    np.ndarray
        Array of stay group indices.
    """
    lat = df_by_user['lat'].to_numpy()
    long = df_by_user['long'].to_numpy()
    duration = df_by_user['duration'].to_numpy()

    n = len(lat)
    stays = np.zeros(n, dtype=int)
    current_stay = 0

    for i in range(1, n):
        # Increment stay group if lat, long, or duration changes
        if lat[i] != lat[i - 1] or long[i] != long[i - 1] or duration[i] != duration[i - 1]:
            current_stay += 1
        stays[i] = current_stay

    return stays


def _get_duration_by_unique_stay(lat_lon: np.array, dur: np.array) -> dict:
    """
    Compute the total duration for each unique latitude and longitude pair.

    Parameters
    ----------
    lat_lon : np.ndarray
        Array of latitude and longitude pairs with shape (n, 2).
    dur : np.ndarray
        Array of durations corresponding to each pair.

    Returns
    -------
    dict
        Dictionary mapping latitude/longitude pairs to total duration.
    """
    unique_lat_long = np.unique(lat_lon, axis=0)
    unique_lat_long = list(map(tuple, unique_lat_long))

    lat_lon_as_tuples = list(map(tuple, lat_lon))
    gps_dur_dict = dict.fromkeys(unique_lat_long, 0)

    for i in range(len(dur)):
        duration = dur[i]
        if duration == 0:
            gps_dur_dict[lat_lon_as_tuples[i]] += 1
        else:
            gps_dur_dict[lat_lon_as_tuples[i]] += duration

    return gps_dur_dict


def _get_oscillating_traces(lat_lon: np.array, dur: np.array, timestamp_list: np.array, time_window: float) -> list:
    """
    Identify oscillating traces within a given time window.

    Parameters
    ----------
    lat_lon : np.ndarray
        Array of latitude and longitude pairs with shape (n, 2).
    dur : np.ndarray
        Array of durations corresponding to each pair.
    timestamp_list : np.ndarray
        Array of timestamps corresponding to each pair.
    time_window : float
        Time window for identifying oscillations.

    Returns
    -------
    list
        List of lists containing indices of oscillating traces.
    """
    oscillating_traces = []

    lat_lon_as_tuples = list(map(tuple, lat_lon))
    number_of_traces = len(lat_lon_as_tuples)
    trace_index = 0

    while trace_index < number_of_traces:
        loop_found = False
        traces_within_time_window = [trace_index]
        for i in range(trace_index + 1, number_of_traces):
            # Check if the trace is within the time window
            if timestamp_list[i] <= timestamp_list[trace_index] + dur[trace_index] + time_window:
                traces_within_time_window.append(i)
                if lat_lon_as_tuples[i] == lat_lon_as_tuples[trace_index]:
                    loop_found = True
                    break
            else:
                break

        # Add to oscillating traces if a loop is found and there are more than 2 traces
        if loop_found and len(traces_within_time_window) > 2:
            oscillating_traces.append(traces_within_time_window)
            trace_index = traces_within_time_window[-1]
        else:
            trace_index += 1

    return oscillating_traces


def _get_replacement_for_traces(lat_lon: np.array,
                                stay_indicator: np.array, oscillating_traces: list,
                                duration_by_unique_stay_dict: dict) -> np.array:
    """
     Determine replacement indices for oscillating traces based on stay indicators and duration.

     Parameters
     ----------
     lat_lon : np.ndarray
         Array of latitude and longitude pairs with shape (n, 2).
     stay_indicator : np.ndarray
         Array of stay indicators with shape (n,).
     oscillating_traces : list
         List of lists containing indices of oscillating traces.
     duration_by_unique_stay_dict : dict
         Dictionary mapping latitude/longitude pairs to total duration.

     Returns
     -------
     np.ndarray
         Array of replacement indices with shape (n,).
     """

    replacement_for_traces = np.full(len(lat_lon), -1)  # Dictionary to capture oscillation
    for trace_list in oscillating_traces:
        # Check if any index position in the trace list is a stay point
        is_stay = np.any(stay_indicator[trace_list] == 1)
        if is_stay:  # If at least one of the indices corresponds to a stay point
            # Get the coordinates for the trace with the maximum time spent
            replacement = max(trace_list, key=lambda x: duration_by_unique_stay_dict[tuple(lat_lon[x])])
        else:
            replacement = trace_list[0]  # Take first index in pair
        replacement_for_traces[trace_list] = replacement

    return replacement_for_traces


def _run_for_user(df_by_user: pd.DataFrame, time_window: float) -> pd.DataFrame:
    """
    Process the DataFrame for a single user to address oscillations in traces.

    Parameters
    ----------
    df_by_user : pd.DataFrame
        DataFrame filtered by user.
    time_window : float
        Time window for identifying oscillations.

    Returns
    -------
    pd.DataFrame
        Processed DataFrame after accounting for trace oscillations.
    """
    # Sort dataframe by start time
    df_by_user = df_by_user.sort_values(by=[UNIX_START_T], ascending=True)

    # Apply transformation to add temporary columns
    temporary_columns = ['duration', 'lat', 'long', 'stay_not']
    df_by_user[temporary_columns] = df_by_user.apply(lambda x: _transform_trace(x), axis=1)

    # Group stays by latitude, longitude, and duration
    temporary_columns.append('stay_group_by_dur')
    df_by_user['stay_group_by_dur'] = _get_stays_by_lat_long_dur(df_by_user)
    df_by_user_unique_stay = df_by_user.drop_duplicates(subset=['stay_group_by_dur'], keep="first")

    lat_lon = df_by_user_unique_stay[['lat', 'long']].to_numpy()
    dur = df_by_user_unique_stay['duration'].to_numpy().astype(int)
    timestamps = df_by_user_unique_stay[UNIX_START_T].to_numpy()
    stay_indicator = df_by_user_unique_stay['stay_not'].to_numpy()
    duration_by_unique_stay_dict = _get_duration_by_unique_stay(lat_lon, dur)

    # Identify oscillating traces
    oscillating_traces = _get_oscillating_traces(lat_lon, dur, timestamps, time_window)

    replacement_for_traces = _get_replacement_for_traces(lat_lon, stay_indicator,
                                                         oscillating_traces, duration_by_unique_stay_dict)

    orig_lat_lon = df_by_user[[ORIG_LAT, ORIG_LONG]].to_numpy()
    if STAY_LAT not in df_by_user or STAY_LONG not in df_by_user:
        stay_lat_lon = [[-1, -1] for _ in range(len(orig_lat_lon))]
    else:
        stay_lat_lon = df_by_user[[STAY_LAT, STAY_LONG]].to_numpy()
    stay_groups = df_by_user['stay_group_by_dur'].to_numpy()

    for i in range(len(stay_lat_lon)):
        stay = stay_groups[i]
        candidate_trace_index = replacement_for_traces[stay]
        if candidate_trace_index != -1 and stay_indicator[candidate_trace_index] == 1:  # If stay point
            stay_lat_lon[i] = lat_lon[candidate_trace_index]  # Update columns stay_lat, stay_long
            orig_lat_lon[i] = lat_lon[candidate_trace_index]
        elif candidate_trace_index != -1:
            orig_lat_lon[i] = lat_lon[candidate_trace_index]

    df_by_user[[STAY_LAT, STAY_LONG]] = stay_lat_lon
    df_by_user[[ORIG_LAT, ORIG_LONG]] = orig_lat_lon

    return df_by_user


def _run(df_by_user_chunk: pd.DataFrame, args: tuple) -> pd.DataFrame:
    """
    Wrapper function to process the DataFrame for a single user with given arguments.

    Parameters
    ----------
    df_by_user_chunk : pd.DataFrame
        DataFrame filtered by user.
    args : tuple
        Tuple contains dur_constraint)

    Returns
    -------
    pd.DataFrame
        Processed DataFrame.
    """
    dur_constraint = args[0]
    df_by_user_chunk = df_by_user_chunk.groupby(USER_ID).apply(lambda x: _run_for_user(x, dur_constraint))
    return df_by_user_chunk


def address_oscillation(output_file: str, dur_constraint: float, input_df: pd.DataFrame | None = None,
                        input_file: str = None) -> pd.DataFrame:
    """
    Address fluctuation/oscillations in traces data for all users and save the result to a file.

    Parameters
    ----------
    output_file : str
        Path to the output file.
    dur_constraint : float
        Duration constraint for identifying oscillations.
    input_df : pd.DataFrame, optional
        Input DataFrame, by default None.
    input_file : str, optional
        Path to the input file, by default None.

    Returns
    -------
    pd.DataFrame
        Processed DataFrame with addressed oscillations.
    """
    if input_df is None and input_file is None:
        logger.error("At least one of input file path or input dataframe is required")

    if input_df is None:
        input_df = get_preprocessed_dataframe(input_file)

    user_id_chunks = get_list_of_chunks_by_column(input_df, USER_ID)
    validate_input_args(duration_constraint=dur_constraint)
    args = (dur_constraint,)
    output_df = execute_parallel(user_id_chunks, input_df, _run, args)
    output_columns = list(set(AO_COLUMNS) & set(output_df.columns))
    output_df = output_df[output_columns]
    output_df.dropna(how="all")
    output_df.to_csv(output_file, columns=sorted(output_df.columns), index=False)
    return output_df
