import numpy as np
import pandas as pd

from mawpy.constants import STAY_LAT, STAY_LONG, STAY_LAT_LONG, STAY
from mawpy.distance import distance


def _mean_ignore_minus_ones(series: pd.Series) -> float:
    """
        Calculates the mean of the column without considering -1 entries in the column
    """
    return series[series != -1].mean()


def _merge_stays(stay_to_update: int, updated_stay: int, df_by_user: pd.DataFrame, group_avgs: pd.DataFrame,
                 group_avgs_index_to_update: int) -> pd.DataFrame:
    """
        Merges two stays for a user and updates the mean_lat and mean_long of the stay.
    """
    df_by_user.loc[df_by_user[STAY] == stay_to_update, STAY] = updated_stay
    merged_values = df_by_user[df_by_user[STAY] == updated_stay][STAY_LAT_LONG]
    new_avg = merged_values.apply(_mean_ignore_minus_ones).fillna(-1)

    group_avgs.loc[group_avgs[STAY] == updated_stay, STAY_LAT_LONG] = new_avg.values
    df_by_user.loc[df_by_user[STAY] == updated_stay, STAY_LAT_LONG] = new_avg.values

    group_avgs.loc[group_avgs_index_to_update, STAY] = updated_stay
    group_avgs.loc[group_avgs_index_to_update, STAY_LAT_LONG] = new_avg.values

    return df_by_user


def get_combined_stay(df_by_user: pd.DataFrame, threshold: float = 0.2) -> pd.DataFrame:
    """
        Merges chronologically sorted stays for a user where distance between the mean_lat and mean_long of two
        consecutive stays for a user is less than the threshold.

        NOTE:
            # if this_stay has -1, -1 as mean_lat, mean_long values then it is not merged.
            # if this_stay and next_stay have valid mean values for lat, long
                    and distance between them is less than threshold, then this_stay, next_stay are merged.
            # if this_stay has valid mean_lat and mean_long but next_stay -1, -1 mean values for lat, long
                    if next_to_next_stay exists and has valid value for mean_lat, mean_long
                        and distance between them is less than threshold, then this_stay, next_stay, next_to_next_stay
                         are merged.
    """

    df_by_user[STAY_LAT] = pd.to_numeric(df_by_user[STAY_LAT])
    df_by_user[STAY_LONG] = pd.to_numeric(df_by_user[STAY_LONG])

    # Calculate the average values for each group
    group_avgs = df_by_user.groupby(STAY)[STAY_LAT_LONG].mean().reset_index()

    total_groups = len(group_avgs)

    # Iterate over the group averages to find groups to merge
    for i in range(total_groups - 1):
        this_row = group_avgs.loc[i]
        this_stay = this_row[STAY]
        this_stay_lat = this_row[STAY_LAT]
        this_stay_long = this_row[STAY_LONG]

        if this_stay_lat == -1 and this_stay_long == -1:
            continue

        next_row = group_avgs.loc[i + 1]
        next_stay = next_row[STAY]
        next_stay_lat = next_row[STAY_LAT]
        next_stay_long = next_row[STAY_LONG]

        if next_stay_lat == -1 and next_stay_long == -1:

            if i + 2 < total_groups:
                next_to_next_row = group_avgs.loc[i + 2]
                next_to_next_stay = next_to_next_row[STAY]
                next_to_next_stay_lat = next_to_next_row[STAY_LAT]
                next_to_next_stay_long = next_to_next_row[STAY_LONG]

                if threshold > distance(this_stay_lat, this_stay_long, next_to_next_stay_lat, next_to_next_stay_long):
                    df_by_user = _merge_stays(next_stay, this_stay, df_by_user, group_avgs, i + 1)
                    df_by_user = _merge_stays(next_to_next_stay, this_stay, df_by_user, group_avgs, i + 2)
                i += 1
                continue

        if distance(this_stay_lat, this_stay_long, next_stay_lat, next_stay_long) < threshold:
            df_by_user = _merge_stays(next_stay, this_stay, df_by_user, group_avgs, i + 1)

    return df_by_user


def get_stay_groups(df_with_stay_added):
    """
        Groups together consecutive traces that have the same value for lat and lon
    """
    lat = df_with_stay_added[STAY_LAT].to_numpy()
    long = df_with_stay_added[STAY_LONG].to_numpy()

    n = len(lat)
    stays = np.zeros(n, dtype=int)
    current_stay = 0

    for i in range(1, n):
        if lat[i] != lat[i - 1] or long[i] != long[i - 1]:
            current_stay += 1
        stays[i] = current_stay

    return stays
