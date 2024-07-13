"""
Perform clustering on multiple locations of one user based on a spatial threshold
Each cluster of locations represents a potential stay

input:
    gps stay information / celluar stay information
    spatial threshold
    duration constraint threshold (for detect common stay)
output:
    potential stays represented by clusters of locations
"""
import logging

import numpy as np
import pandas as pd

from sklearn.cluster import KMeans

from mawpy.constants import (USER_ID, STAY_DUR, ORIG_LAT, STAY_LAT, STAY_LONG, STAY_UNC, ORIG_LONG, ORIG_UNC, STAY,
                             UNIX_START_T, UNIX_START_DATE)
from mawpy.distance import distance
from mawpy.utilities.common import get_combined_stay, get_stay_groups
from mawpy.utilities.cluster import Cluster
from mawpy.utilities.preprocessing import get_preprocessed_dataframe, get_list_of_chunks_by_column, execute_parallel

logger = logging.getLogger(__name__)


def _get_cluster_center(row: pd.Series, mapping: dict, dur_constr: float) -> tuple[str, str, str]:
    """
    Returns the lat and long of the cluster to which the trace(row) belongs.
    """
    if dur_constr:
        lat_long = (row[STAY_LAT], row[STAY_LONG])
        unc = row[STAY_UNC]
    else:
        lat_long = (row[ORIG_LAT], row[ORIG_LONG])
        unc = row[ORIG_UNC]

    if lat_long in mapping:
        cluster_lat, cluster_long, cluster_radius = mapping[lat_long]
        return cluster_lat, cluster_long, max(unc, cluster_radius)
    else:
        return lat_long[0], lat_long[1], unc


def _k_means_cluster_lloyd(cluster_list: list[Cluster]) -> list[Cluster]:
    """
    Lloyd's Algorithm for K-Means Clustering
    """
    uniq_month_gps_list = []
    for each_cluster in cluster_list:
        uniq_month_gps_list.extend(
            each_cluster.pList
        )  # add everything to this list, plist is some property associated with c - it has only unique location elements from loc4cluster

    k_cluster = [each_cluster.pList for each_cluster in cluster_list]
    k = len(k_cluster)

    ##project coordinates on to plane
    ##search "python lat long onto plane": https://pypi.org/project/stateplane/
    ##search "python project lat long to x y": https://gis.stackexchange.com/questions/212723/how-can-i-convert-lon-lat-coordinates-to-x-y
    ###The above methods not used
    y_center = np.mean(
        [point[0] for point in uniq_month_gps_list]
    )  # it may mean c.pList will be a list structure
    x_center = np.mean([point[1] for point in uniq_month_gps_list])

    distance_coord = np.empty((0, 2))
    for point in uniq_month_gps_list:
        x_distance = distance(y_center, x_center, y_center, point[1])  # distance in km for great arc circle
        y_distance = distance(y_center, x_center, point[0], x_center)
        if point[0] < y_center:  # p is to south of y_center
            y_distance = -y_distance
        if point[1] < x_center:  # p is to the west of x_center
            x_distance = -x_distance
        distance_coord = np.append(
            distance_coord, np.array([[y_distance, x_distance]]), axis=0
        )  # adding coordinates

    initial_centers = np.empty((0, 2))
    i = 0
    """
    Get initial cluster centers as the mean of points.
    """
    for each_cluster in cluster_list:
        num_point = len(each_cluster.pList)
        points = distance_coord[i: (i + num_point)]
        ctr = np.mean(points, axis=0, keepdims=True)
        initial_centers = np.append(initial_centers, ctr, axis=0)
        i = i + num_point
    """
    Assign points to cluster labels after k means clustering.
    """
    kmeans = KMeans(n_clusters=k, init=initial_centers).fit(distance_coord)
    lab = kmeans.labels_  # cluster labels
    membership = {clus: [] for clus in set(lab)}
    for j in range(0, len(uniq_month_gps_list)):
        membership[lab[j]].append(uniq_month_gps_list[j])

    """
    Using cluster class to transform membership dictionary into a cluster object as defined previously.
    All cluster objects are appended to L_new.
    """
    cluster_list_new = []
    for a_cluster in membership:
        new_cluster = (
            Cluster()
        )  # every label from k means - algorithm is assigned a  class cluster
        new_cluster.add_points(membership[a_cluster])
        cluster_list_new.append(new_cluster)

    return cluster_list_new


def _get_clusters(locations_for_clustering: list[tuple[float, float]], spat_constr: float) -> list[Cluster]:
    """
        Get list of clusters from trace locations based on the spatial constraint.
    """
    clusters_list = []

    new_cluster = Cluster()
    new_cluster.add_point(locations_for_clustering[0])  # add first coordinate to this cluster

    clusters_list.append(new_cluster)  # add cluster with one just point to clusters_list
    current_cluster = new_cluster
    # Note: Clusters do not take into considearion time of the day - unique clusters are there but no info on how many times was visited by the device at the the cluster location.
    ### Go from second loc. in locations_for_clustering and if it is below the spatial constraint add it to current cluster
    for loc in locations_for_clustering[1:]:  # start iterating from second coordinate in loc4cluster
        if current_cluster.get_distance_from_center(loc) < spat_constr:
            current_cluster.add_point(loc)  # add if smaller than spatial constraint
        ### Check for other clusters existing in the list and add point abiding to the spatial constraint to this cluster
        else:  # if point is away from spatial constraint
            current_cluster = None
            for this_cluster in clusters_list:
                if (
                        this_cluster.get_distance_from_center(loc) < spat_constr
                ):  # check again the spatial constraint parameter
                    this_cluster.add_point(loc)
                    current_cluster = this_cluster  # make this_cluster as the current_cluster
                    break  # loop breaks if for the point the suitable cluster is found
            ### (Are we not checking duration here) If no cluster is found where the duration constraint is found, then create a new cluster and append it to L

            if current_cluster is None:  # still if Ccurrent is none
                new_cluster = Cluster()
                new_cluster.add_point(loc)  # add the point
                clusters_list.append(new_cluster)  # add cluster on top of others
                current_cluster = new_cluster
    return clusters_list


"""
Note: Do not put the duration constraint parameter if you are running incremental clustering as the first
step since there would be no stay points if this is used as the first step.

Step1: Form clusters based on spatial constraint. If duration constraint is provided,
then work with only stay points else all.
Step2: Perform k means to apply the correction on Step1, since Step1 does the clustering based on
order of traces.
Step3: Once clusters are prepared in Step2, add traces to clusters which are within 0.2 km of cluster centre.

To Do: Figure out the data structure used to perform all actions.
"""


# if a duration constraint is provided, then get loc4cluster as latitude and longitude coordinates
def _get_locations_to_cluster_center_map(clusters_list: list[Cluster]) -> dict:
    """
        Getting a mapping for each point in the cluster to the cluster center and cluster radius.
    """
    locations_to_cluster_center_map = {}
    for this_cluster in clusters_list:  # for each cluster in L
        cluster_radius = int(1000 * this_cluster.get_cluster_radius())  # get radius of each cluster
        cluster_center = [
            np.mean([each_point[0] for each_point in this_cluster.pList]),
            np.mean([each_point[1] for each_point in this_cluster.pList]),
        ]  # calculate center
        for each_point in this_cluster.pList:  # for each coordinate in c.pList
            locations_to_cluster_center_map[(each_point[0], each_point[1])] = (
                cluster_center[0],
                cluster_center[1],
                cluster_radius,
            )  # store the center with cluster radius
    return locations_to_cluster_center_map


def _run_for_user(df_by_user: pd.DataFrame, spat_constr: float, dur_constr: float | None = None) -> pd.DataFrame:
    """
        Function to perform incremental clustering on a dataframe containing traces for a single user
    """
    df_by_user = df_by_user.sort_values(by=[UNIX_START_T], ascending=True)

    if dur_constr:  # cluster locations of stays to obtain aggregated stayes
        # get unique GPS stay points if stay duration is greater than duration constraint

        stay_lat_long_df = df_by_user.loc[df_by_user[STAY_DUR] >= dur_constr, [STAY_LAT, STAY_LONG]]
        # Convert to list of tuples
        locations_for_clustering = list(zip(stay_lat_long_df[STAY_LAT], stay_lat_long_df[STAY_LONG]))
    else:  # cluster original locations (orig_lat and orgi_long) to obtain stays
        # get GPS original points
        orig_lat_long_df = df_by_user[[ORIG_LAT, ORIG_LONG]]
        # Convert to list of tuples
        locations_for_clustering = list(zip(orig_lat_long_df[ORIG_LAT], orig_lat_long_df[ORIG_LONG]))
    if len(locations_for_clustering) == 0:
        return df_by_user

    clusters_list = _get_clusters(locations_for_clustering, spat_constr)
    # make Ccurrent as Cnew

    ### apply k means clustering with k same as length of L
    clusters_list = _k_means_cluster_lloyd(
        clusters_list
    )  # correct an order issue related to incremental clustering # clusters get appended to list L

    ### create a dictionary which takes each point and keeps information of its cluster center and radius
    ## centers of each locations that are clustered
    locations_to_cluster_center_map = _get_locations_to_cluster_center_map(clusters_list)

    ### Update trace itself using clustre center and max(radius, uncertainty)
    df_by_user[[STAY_LAT, STAY_LONG, STAY_UNC]] = df_by_user.apply(
        lambda row: _get_cluster_center(row, locations_to_cluster_center_map, dur_constr), axis=1, result_type='expand')

    df_by_user[STAY] = get_stay_groups(df_by_user)

    df_by_user = get_combined_stay(df_by_user)

    return df_by_user


def _run(df_by_user_chunk: pd.DataFrame, args: tuple) -> pd.DataFrame:
    spatial_constraint, dur_constraint = args

    if dur_constraint <= 0 or dur_constraint is None:
        df_by_user_chunk = (df_by_user_chunk.groupby(USER_ID)
                            .apply(lambda x: _run_for_user(x, spatial_constraint)))

    else:
        df_by_user_chunk = (df_by_user_chunk.groupby(USER_ID)
                            .apply(lambda x: _run_for_user(x, spatial_constraint, dur_constraint)))

    return df_by_user_chunk


def incremental_clustering(input_file: str, output_file: str, spatial_constraint: float,
                           dur_constraint: float) -> pd.DataFrame:
    input_df = get_preprocessed_dataframe(input_file)
    user_id_chunks = get_list_of_chunks_by_column(input_df, USER_ID)
    args = (spatial_constraint, dur_constraint)
    output_df = execute_parallel(user_id_chunks, input_df, _run, args)
    output_df = output_df[[USER_ID, UNIX_START_T, UNIX_START_DATE,
                           ORIG_LAT, ORIG_LONG, ORIG_UNC,
                           STAY_LAT, STAY_LONG, STAY_UNC, STAY_DUR, STAY]]
    output_df.dropna(how="all")
    output_df.to_csv(output_file, index=False)
    return output_df
