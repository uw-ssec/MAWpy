"""
    incremental clustering developed in 2018 TRC paper
    together with k-means to correct an order issue related to incremental clustering
:param user:
:param spat_constr:
:param dur_constr:
:return: modified user traces
"""


import sys, json,os, psutil, csv, time
import numpy as np
#from distance import distance
from class_cluster import cluster
from collections import defaultdict

from geopy.distance import distance
from sklearn.cluster import KMeans

def K_meansClusterLloyd(L):
    uniqMonthGPSList = []
    for c in L:
        uniqMonthGPSList.extend(c.pList)
        
    Kcluster = [c.pList for c in L]
    k = len(Kcluster)
    
    ##project coordinates on to plane
    ##search "python lat long onto plane": https://pypi.org/project/stateplane/
    ##search "python project lat long to x y": https://gis.stackexchange.com/questions/212723/how-can-i-convert-lon-lat-coordinates-to-x-y
    ###The above methods not used
    y_center = np.mean([p[0] for p in uniqMonthGPSList])
    x_center = np.mean([p[1] for p in uniqMonthGPSList])
   
    distance_coord = np.empty((0, 2))
    for p in uniqMonthGPSList:
        x_distance = distance((y_center,x_center),(y_center,p[1])).km
        y_distance = distance((y_center,x_center),(p[0],x_center)).km
        if p[0] < y_center:
            y_distance = - y_distance
        if p[1] < x_center:
            x_distance = - x_distance
        distance_coord = np.append(distance_coord, np.array([[y_distance,x_distance]]), axis=0)
        
    initial_centers = np.empty((0, 2))
    i=0
    for c in L:
        num_point = len(c.pList)
        points = distance_coord[i:(i+num_point)]
        ctr = np.mean(points,axis=0,keepdims=True)
        initial_centers = np.append(initial_centers, ctr, axis=0)
        i=i+num_point
    
    kmeans = KMeans(n_clusters=k,init=initial_centers).fit(distance_coord)
    lab = kmeans.labels_
    membership = {clus:[] for clus in set(lab)}
    for j in range(0,len(uniqMonthGPSList)):
        membership[lab[j]].append(uniqMonthGPSList[j])
        
    L_new = []
    for a_cluster in membership:
        newC = cluster()
        for a_location in membership[a_cluster]:
            newC.addPoint(a_location)
        L_new.append(newC)
        
    return L_new


def cluster_incremental(user, spat_constr, dur_constr=None):
    # spat_constr #200.0/1000 #0.2Km
    # dur_constr # 0 or 300second

    if dur_constr:  # cluster locations of stays to obtain aggregated stayes
        loc4cluster = list(set([(trace[6], trace[7]) for d in user for trace in user[d] if int(trace[9]) >= dur_constr]))
    else:  # cluster original locations to obtain stays
        loc4cluster = list(set([(trace[3], trace[4]) for d in user for trace in user[d]]))

    if len(loc4cluster) == 0:
        return (user)

    ## start clustering
    L = []
    Cnew = cluster()
    Cnew.addPoint(loc4cluster[0])
    L.append(Cnew)
    Ccurrent = Cnew
    for i in range(1, len(loc4cluster)):
        if Ccurrent.distance_C_point(loc4cluster[i]) < spat_constr:
            Ccurrent.addPoint(loc4cluster[i])
        else:
            Ccurrent = None
            for C in L:
                if C.distance_C_point(loc4cluster[i]) < spat_constr:
                    C.addPoint(loc4cluster[i])
                    Ccurrent = C
                    break
            if Ccurrent == None:
                Cnew = cluster()
                Cnew.addPoint(loc4cluster[i])
                L.append(Cnew)
                Ccurrent = Cnew

    L = K_meansClusterLloyd(L) # correct an order issue related to incremental clustering

    ## centers of each locations that are clustered
    mapLocation2cluCenter = {}
    for c in L:
        r = int(1000*c.radiusC()) #
        cent = [str(np.mean([p[0] for p in c.pList])), str(np.mean([p[1] for p in c.pList]))]
        for p in c.pList:
            mapLocation2cluCenter[(str(p[0]),str(p[1]))] = (cent[0], cent[1], r)

    if dur_constr:  # modify locations of stays to aggregated centers of stays
        for d in user.keys():
            for trace in user[d]:
                if (trace[6], trace[7]) in mapLocation2cluCenter:
                    trace[6], trace[7], trace[8] = mapLocation2cluCenter[(trace[6], trace[7])][0], \
                                                   mapLocation2cluCenter[(trace[6], trace[7])][1], \
                                                   max(mapLocation2cluCenter[(trace[6], trace[7])][2], int(trace[8]))
    else:  # record stay locations of original locations
        for d in user.keys():
            for trace in user[d]:
                if (trace[3], trace[4]) in mapLocation2cluCenter:
                    trace[6], trace[7], trace[8] = mapLocation2cluCenter[(trace[3], trace[4])][0], \
                                                   mapLocation2cluCenter[(trace[3], trace[4])][1], \
                                                   max(mapLocation2cluCenter[(trace[3], trace[4])][2], int(trace[5]))
    
    ## Recombine stays that (1) don't have transit points between them and (2) are within the distance threshold.
    stays_combined = []
    all_stays = []
    
    day_set = list(user.keys())
    day_set.sort()
    
    for a_day in day_set:
        for a_location in user[a_day]:
            if len(all_stays) == 0:
                all_stays.append([a_location])
            else:
                last_stay = (all_stays[-1][-1][6], all_stays[-1][-1][7])
                if a_location[6] == last_stay[0] and a_location[7] == last_stay[1]:
                    all_stays[-1].append(a_location)
                else:
                    all_stays.append([a_location])
                    
    stay_index = 0
    stays_combined.append(all_stays[0])
    all_stays.pop(0)
    
    update_lat = float(stays_combined[-1][-1][6])
    update_long = float(stays_combined[-1][-1][7])
    
    while len(all_stays) > 0:
        current_stay = all_stays.pop(0)
        if tuple(stays_combined[-1][-1][6:8]) == ('-1','-1'):
            stays_combined.append(current_stay)
            update_lat = float(stays_combined[-1][-1][6])
            update_long = float(stays_combined[-1][-1][7])
        else:
            if tuple(current_stay[-1][6:8]) != ('-1','-1'):
                if distance(tuple([float(x) for x in current_stay[-1][6:8]]), tuple([update_lat,update_long])).km < 0.2:
                    stays_combined[-1].extend(current_stay)
                    lat_set = set([float(x[6]) for x in stays_combined[-1]])
                    long_set = set([float(x[7]) for x in stays_combined[-1]])
                    update_lat = np.mean(list(lat_set))
                    update_long = np.mean(list(long_set))
                else:
                    stays_combined.append(current_stay)
                    update_lat = float(stays_combined[-1][-1][6])
                    update_long = float(stays_combined[-1][-1][7])
            else:
                if len(all_stays) == 0:
                    stays_combined.append(current_stay)
                else:
                    next_stay = all_stays.pop(0)
                    if distance(tuple([float(x) for x in next_stay[-1][6:8]]), tuple([update_lat,update_long])).km < 0.2:
                        stays_combined[-1].extend(current_stay)
                        stays_combined[-1].extend(next_stay)
                        lat_set = set([float(x[6]) for x in stays_combined[-1]])
                        long_set = set([float(x[7]) for x in stays_combined[-1]])
                        lat_set = [x for x in lat_set if x != -1.0]
                        long_set = [x for x in long_set if x != -1.0]
                        update_lat = np.mean(lat_set)
                        update_long = np.mean(long_set)
                    else:
                        stays_combined.append(current_stay)
                        stays_combined.append(next_stay)
                        update_lat = float(stays_combined[-1][-1][6])
                        update_long = float(stays_combined[-1][-1][7])
    
    stays_output = []
    for a_stay in stays_combined:
        lat_set = set([float(x[6]) for x in a_stay])
        long_set = set([float(x[7]) for x in a_stay])
        lat_set = [x for x in lat_set if x != -1.0]
        long_set = [x for x in long_set if x != -1.0]
        if len(lat_set) > 0 and len(long_set) > 0:
            new_lat = np.mean(lat_set)
            new_long = np.mean(long_set)
        else:
            new_lat = -1
            new_long = -1
        for i in range(0, len(a_stay)):
            a_stay[i][6] = str(new_lat)
            a_stay[i][7] = str(new_long)
        stays_output.append(a_stay)
        
    ##Convert stays into a disctionary
    dict_output = {}
    for a_stay in stays_output:
        for a_record in a_stay:
            start_date = a_record[-1][0:6]
            if start_date in dict_output:
                dict_output[start_date].append(a_record)
            else:
                dict_output[start_date] = [a_record]
        
    return (dict_output)
