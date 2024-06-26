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

import sys
from datetime import datetime

import psutil
import time
import numpy as np
import pandas as pd

from multiprocessing import Pool
from multiprocessing import Lock, cpu_count

from geopy.distance import distance
from sklearn.cluster import KMeans

from mawpy.cluster import Cluster
from mawpy.constants import USER_ID, STAY_DUR, ORIG_LAT, STAY_LAT, STAY_LONG, UNIX_START_T, UNIX_START_DATE, STAY_UNC, \
    ORIG_LONG, ORIG_UNC, STAY, STAY_LAT_PRE_COMBINED, STAY_LONG_PRE_COMBINED, STAY_PRE_COMBINED


def init(this_lock):
    global lock
    lock = this_lock


def _get_cluster_center(row, mapping, dur_constr):
    """
    Returns the lat and long of the cluster to which the trace(row) belongs.
    """
    if dur_constr:
        lat_long = (str(row[STAY_LAT]), str(row[STAY_LONG]))
        unc = row[STAY_UNC]
    else:
        lat_long = (str(row[ORIG_LAT]), str(row[ORIG_LONG]))
        unc = row[ORIG_UNC]

    if lat_long in mapping:
        cluster_lat, cluster_long, cluster_radius = mapping[lat_long]
        return cluster_lat, cluster_long, max(unc, cluster_radius)
    else:
        return lat_long[0], lat_long[1], unc


def _mean_ignore_negative(series):
    """
        Calculates the mean of the column without considering -1 entries in the column
    """
    return series[series != -1].mean()


def _merge_stays(stay_to_update, updated_stay, df_by_user, group_avgs, group_avgs_index_to_update):
    """
        Merges two stays for a user and updates the mean_lat and mean_long of the stay.
    """
    df_by_user.loc[df_by_user[STAY] == stay_to_update, STAY] = updated_stay
    merged_values = df_by_user[df_by_user[STAY] == updated_stay][[STAY_LAT, STAY_LONG]]
    new_avg = merged_values.apply(_mean_ignore_negative).fillna(-1)

    group_avgs.loc[group_avgs[STAY] == updated_stay, [STAY_LAT, STAY_LONG]] = new_avg.values
    df_by_user.loc[df_by_user[STAY] == updated_stay, STAY_LAT] = new_avg.values[0]
    df_by_user.loc[df_by_user[STAY] == updated_stay, STAY_LONG] = new_avg.values[1]

    group_avgs.loc[group_avgs_index_to_update, STAY] = updated_stay
    group_avgs.loc[group_avgs_index_to_update, STAY_LAT] = new_avg.values[0]
    group_avgs.loc[group_avgs_index_to_update, STAY_LONG] = new_avg.values[1]

    return df_by_user


def _get_combined_stay(df_by_user, threshold=1):
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

    # Calculate the average values for each group
    df_by_user[STAY_LAT_PRE_COMBINED] = df_by_user[STAY_LAT]
    df_by_user[STAY_LONG_PRE_COMBINED] = df_by_user[STAY_LONG]
    df_by_user[STAY_PRE_COMBINED] = df_by_user[STAY]

    df_by_user[STAY_LAT] = pd.to_numeric(df_by_user[STAY_LAT])
    df_by_user[STAY_LONG] = pd.to_numeric(df_by_user[STAY_LONG])

    group_avgs = df_by_user.groupby(STAY)[[STAY_LAT, STAY_LONG]].mean().reset_index()

    # Initialize an empty list to store the groups to merge
    merge_indices = []

    total_groups = len(group_avgs)

    group_avgs_pre_merges = group_avgs
    # Iterate over the group averages to find groups to merge
    for i in range(total_groups - 1):
        this_stay = group_avgs.loc[i, STAY]
        this_stay_lat = group_avgs.loc[i, STAY_LAT]
        this_stay_long = group_avgs.loc[i, STAY_LONG]

        if this_stay_lat == -1 and this_stay_long == -1:
            continue

        next_stay = group_avgs.loc[i + 1, STAY]
        next_stay_lat = group_avgs.loc[i + 1, STAY_LAT]
        next_stay_long = group_avgs.loc[i + 1, STAY_LONG]

        if next_stay_lat == -1 and next_stay_long == -1:

            if i + 2 < total_groups:
                next_to_next_stay = group_avgs.loc[i + 2, STAY]
                next_to_next_stay_lat = group_avgs.loc[i + 2, STAY_LAT]
                next_to_next_stay_long = group_avgs.loc[i + 2, STAY_LONG]

                if threshold > distance(tuple([this_stay_lat, this_stay_long]),
                                        tuple([next_to_next_stay_lat, next_to_next_stay_long])).km:
                    df_by_user = _merge_stays(next_stay, this_stay, df_by_user, group_avgs, i + 1)
                    df_by_user = _merge_stays(next_to_next_stay, this_stay, df_by_user, group_avgs, i + 2)
                i += 1
                continue

        if distance(tuple([this_stay_lat, this_stay_long]), tuple([next_stay_lat, next_stay_long])).km < threshold:
            df_by_user = _merge_stays(next_stay, this_stay, df_by_user, group_avgs, i + 1)

    return df_by_user


def _k_means_cluster_lloyd(cluster_list):
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
        x_distance = distance(
            (y_center, x_center), (y_center, point[1])
        ).km  # distance in km for great arc circle
        y_distance = distance((y_center, x_center), (point[0], x_center)).km
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
        for a_location in membership[a_cluster]:
            new_cluster.addPoint(a_location)
        cluster_list_new.append(new_cluster)

    return cluster_list_new


def _get_clusters(locations_for_clustering, spat_constr):
    """
        Get list of clusters from trace locations based on the spatial constraint.
    """
    clusters_list = []

    new_cluster = Cluster()
    new_cluster.addPoint(locations_for_clustering[0])  # add first coordinate to this cluster

    clusters_list.append(new_cluster)  # add cluster with one just point to clusters_list
    current_cluster = new_cluster
    # Note: Clusters do not take into considearion time of the day - unique clusters are there but no info on how many times was visited by the device at the the cluster location.
    ### Go from second loc. in locations_for_clustering and if it is below the spatial constraint add it to current cluster
    for i in range(
            1, len(locations_for_clustering)
    ):  # start iterating from second coordinate in loc4cluster
        if current_cluster.distance_C_point(locations_for_clustering[i]) < spat_constr:
            current_cluster.addPoint(locations_for_clustering[i])  # add if smaller than spatial constraint
        ### Check for other clusters existing in the list and add point abiding to the spatial constraint to this cluster
        else:  # if point is away from spatial constraint
            current_cluster = None
            for this_cluster in clusters_list:
                if (
                        this_cluster.distance_C_point(locations_for_clustering[i]) < spat_constr
                ):  # check again the spatial constraint parameter
                    this_cluster.addPoint(locations_for_clustering[i])
                    current_cluster = this_cluster  # make this_cluster as the current_cluster
                    break  # loop breaks if for the point the suitable cluster is found
            ### (Are we not checking duration here) If no cluster is found where the duration constraint is found, then create a new cluster and append it to L

            if current_cluster is None:  # still if Ccurrent is none
                new_cluster = Cluster()
                new_cluster.addPoint(locations_for_clustering[i])  # add the point
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
def _get_locations_to_cluster_center_map(clusters_list):
    """
        Getting a mapping for each point in the cluster to the cluster center and cluster radius.
    """
    locations_to_cluster_center_map = {}
    for this_cluster in clusters_list:  # for each cluster in L
        cluster_radius = int(1000 * this_cluster.radiusC())  # get radius of each cluster
        cluster_center = [
            str(np.mean([each_point[0] for each_point in this_cluster.pList])),
            str(np.mean([each_point[1] for each_point in this_cluster.pList])),
        ]  # calculate center
        for each_point in this_cluster.pList:  # for each coordinate in c.pList
            locations_to_cluster_center_map[(str(each_point[0]), str(each_point[1]))] = (
                cluster_center[0],
                cluster_center[1],
                cluster_radius,
            )  # store the center with cluster radius
    return locations_to_cluster_center_map


def _run_for_user(df_by_user, spat_constr, dur_constr=None):

    """
        Function to perform incremental clustering on a dataframe containing traces for a single user
    """

    if dur_constr:  # cluster locations of stays to obtain aggregated stayes
        # get unique GPS stay points if stay duration is greater than duration constraint

        locations_for_clustering = ((df_by_user[df_by_user[STAY_DUR] >= dur_constr]
                                     .apply(lambda x: (x[STAY_LAT], x[STAY_LONG]), axis=1))
                                    .tolist())
    else:  # cluster original locations (orig_lat and orgi_long) to obtain stays
        # get GPS original points
        locations_for_clustering = (df_by_user.apply(lambda x: (x[ORIG_LAT], x[ORIG_LONG]), axis=1)).tolist()

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

    df_by_user = df_by_user.sort_values(by=[UNIX_START_DATE], ascending=True)

    df_by_user[STAY] = ((df_by_user[[STAY_LAT, STAY_LONG]] != df_by_user[[STAY_LAT, STAY_LONG]].shift())
                          .any(axis=1).cumsum())

    df_by_user = _get_combined_stay(df_by_user)

    return df_by_user


def _run(args):
    name, df_by_user, spatial_constraint, dur_constraint, outputFile = args

    if dur_constraint == -1:
        df_by_user = _run_for_user(df_by_user, spatial_constraint)

    else:
        df_by_user = _run_for_user(df_by_user, spatial_constraint, dur_constraint)

    return df_by_user


if __name__ == "__main__":
    """
    param:
        inputFile
        partitionThreshold
    """
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    spatial_constraint = float(sys.argv[3])
    dur_constraint = int(sys.argv[4])


def _divide_chunks(user_id_list, n):
    user_id_chunks = []
    for i in range(0, len(user_id_list), n):  # looping till length usernamelist
        user_id_chunks.append(user_id_list[i: i + n])
    return user_id_chunks


def incremental_clustering(input_file, output_file, spatial_constraint, dur_constraint):

    this_lock = Lock()  # thread locker

    pool = Pool(cpu_count(), initializer=init, initargs=(this_lock,))

    user_num_in_mem = 1000
    input_df = pd.read_csv(input_file)
    input_df[UNIX_START_DATE] = input_df[UNIX_START_T].apply(
        lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d'))
    user_id_list = input_df[USER_ID].unique().tolist()
    print("total number of users to be processed: ", len(user_id_list))

    user_id_chunks = list(_divide_chunks(user_id_list, user_num_in_mem))
    print("number of chunks to be processed", len(user_id_chunks))

    chunk_count = 0
    tasks = []
    for each_chunk in user_id_chunks:
        print(
            "Start processing bulk: ",
            ++chunk_count,
            " at time: ",
            time.strftime("%m%d-%H:%M"),
            " memory: ",
            psutil.virtual_memory().percent,
        )
        tasks = [
            pool.apply_async(_run, (task,))
            for task in [
                (user, input_df[input_df[USER_ID] == user], spatial_constraint, dur_constraint, output_file)
                for user in each_chunk
            ]
        ]

    pool.close()
    pool.join()
    df_results = [t.get() for t in tasks]

    final_df = pd.concat(df_results)
    final_df.to_csv(output_file)
