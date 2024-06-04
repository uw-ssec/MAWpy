'''
Performe clustering on multiple locations of one user based on a spatial threshold
Each cluster of locations represents a potential stay 

input:
    gps stay information / celluar stay information
    spatial threshold
    duration constraint threshold (for detect common stay)
outout:
    potential stays represented by clusters of locations
'''

import sys, json,os, psutil, csv, time
import numpy as np
#from distance import distance
from class_cluster import cluster
from collections import defaultdict
from multiprocessing import Pool
from multiprocessing import current_process, Lock, cpu_count

from geopy.distance import distance
from sklearn.cluster import KMeans

def init(l):
    global lock
    lock = l

def K_meansClusterLloyd(L):
    uniqMonthGPSList = []
    for c in L:
        uniqMonthGPSList.extend(c.pList) # add everything to this list, plist is some property associated with c - it has only unique location elements from loc4cluster
        
    Kcluster = [c.pList for c in L]
    k = len(Kcluster)
    
    ##project coordinates on to plane
    ##search "python lat long onto plane": https://pypi.org/project/stateplane/
    ##search "python project lat long to x y": https://gis.stackexchange.com/questions/212723/how-can-i-convert-lon-lat-coordinates-to-x-y
    ###The above methods not used
    y_center = np.mean([p[0] for p in uniqMonthGPSList]) # it may mean c.pList will be a list structure
    x_center = np.mean([p[1] for p in uniqMonthGPSList])
   
    distance_coord = np.empty((0, 2))
    for p in uniqMonthGPSList:
        x_distance = distance((y_center,x_center),(y_center,p[1])).km # distance in km for great arc circle
        y_distance = distance((y_center,x_center),(p[0],x_center)).km
        if p[0] < y_center: #p is to south of y_center
            y_distance = - y_distance
        if p[1] < x_center: #p is to the west of x_center
            x_distance = - x_distance
        distance_coord = np.append(distance_coord, np.array([[y_distance,x_distance]]), axis=0) # adding coordinates
        
    initial_centers = np.empty((0, 2))
    i=0
    """
    Get initial clustre centers as the mean of points.
    """
    for c in L:
        num_point = len(c.pList)
        points = distance_coord[i:(i+num_point)]
        ctr = np.mean(points,axis=0,keepdims=True)
        initial_centers = np.append(initial_centers, ctr, axis=0)
        i=i+num_point
    """
    Assign points to cluster labels after k means clustering.
    """
    kmeans = KMeans(n_clusters=k,init=initial_centers).fit(distance_coord)
    lab = kmeans.labels_ # cluster labels
    membership = {clus:[] for clus in set(lab)}
    for j in range(0,len(uniqMonthGPSList)):
        membership[lab[j]].append(uniqMonthGPSList[j])

    """
    Using cluster class to transform membership dictionary into a cluster object as defined previously. 
    All cluster objects are appended to L_new.
    """   
    L_new = []
    for a_cluster in membership:
        newC = cluster() # every label from k means - algorithm is assigned a  class cluster
        for a_location in membership[a_cluster]:
            newC.addPoint(a_location)
        L_new.append(newC)
        
    return L_new
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
def cluster_incremental(user, spat_constr, dur_constr=None):
    # spat_constr #200.0/1000 #0.2Km
    # dur_constr # 0 or 300second

    if dur_constr:  # cluster locations of stays to obtain aggregated stayes
        # get unique GPS stay points if stay duration is greater than duration constraint
        loc4cluster = list(set([(trace[6], trace[7]) for d in user for trace in user[d] if int(trace[9]) >= dur_constr]))
    else:  # cluster original locations (orig_lat and orgi_long) to obtain stays
        # get GPS original points
        loc4cluster = list(set([(trace[3], trace[4]) for d in user for trace in user[d]]))

    if len(loc4cluster) == 0:
        return (user)


### If for the first cluster, wee find that the subsequnt point agress to the spatial constraint, add it.
### Else make a new cluster -> Still if no points are there make a new cluster with leeft out points

    ## start clustering
    L = []
    Cnew = cluster()
    Cnew.addPoint(loc4cluster[0]) # add first coordinate to this cluster
    L.append(Cnew) # add cluster with one just point to L
    Ccurrent = Cnew # cCURRENT = Cnew
# Note: Clusters do not take into considearion time of the day - unique clusters are there but no info on how many times was visited by the device at the the cluster location.
### Go from second loc. in loc4cluster and if it is below the spatial constraint add it to current cluster
    for i in range(1, len(loc4cluster)): # start iterating from second coordinate in loc4cluster
        if Ccurrent.distance_C_point(loc4cluster[i]) < spat_constr:
            Ccurrent.addPoint(loc4cluster[i]) # add if smaller than spatial constraint
### Check for other clusters existing in L and add point abiding to the spatial constraint to this cluster
        else: # if point is away from spaitiial constraint
            Ccurrent = None # make Ccurrent none
            for C in L: # l has Cnew with the initial cluster
                if C.distance_C_point(loc4cluster[i]) < spat_constr: # check again the spatial constraint parameter
                    C.addPoint(loc4cluster[i])
                    Ccurrent = C # make Ccurrent as a new cluster
                    break # loop breaks if for the point the suitable cluster is found
### (Are we not checking duration here) If no cluster is found where the duration constraint is found, then create a new cluster and append it to L

            if Ccurrent == None: # still if Ccurrent is none
                Cnew = cluster()
                Cnew.addPoint(loc4cluster[i]) # add the point
                L.append(Cnew)  # add cluster on top of others
                Ccurrent = Cnew # make Ccurrent as Cnew

### apply k means clustering with k same as lenth of L
    L = K_meansClusterLloyd(L) # correct an order issue related to incremental clustering # clusters get appeneded to list L

### create a dictionary which takes each point and keeps information of its cluster center and radius
    ## centers of each locations that are clustered
    mapLocation2cluCenter = {}
    for c in L: # for each cluster in L
        r = int(1000*c.radiusC()) # get radius of each cluster
        cent = [str(np.mean([p[0] for p in c.pList])), str(np.mean([p[1] for p in c.pList]))] # calculate center
        for p in c.pList:# for each coordinate in c.pList
            mapLocation2cluCenter[(str(p[0]),str(p[1]))] = (cent[0], cent[1], r) # store the center with cluster radius
### Update trace itself using clustre center and max(radius, uncertainity)
    if dur_constr:  # modify locations of stays to aggregated centers of stays

        
        ## replace stay_lat, stay_long and stay_unc with the cluster lat, cluster_long and max between radius and uncertainity
        ## trace[6] is stay_lat and trace[7] is stay_long, trace[3] is orig_lat, trace[4] is orig_long
        
        for d in user.keys():
            for trace in user[d]:
                if (trace[6], trace[7]) in mapLocation2cluCenter: # if trace is a dictionary keey
                    #add infor on the index
                    trace[6], trace[7], trace[8] = mapLocation2cluCenter[(trace[6], trace[7])][0], \
                                                   mapLocation2cluCenter[(trace[6], trace[7])][1], \
                                                   max(mapLocation2cluCenter[(trace[6], trace[7])][2], int(trace[8]))
            
    elif dur_constr is None or dur_constr ==  0:  # Do the above for original coordinates if no duration constraint provided
        for d in user.keys():
            for trace in user[d]:
                if (trace[3], trace[4]) in mapLocation2cluCenter:
                    trace[6], trace[7], trace[8] = mapLocation2cluCenter[(trace[3], trace[4])][0], \
                                                   mapLocation2cluCenter[(trace[3], trace[4])][1], \
                                                   max(mapLocation2cluCenter[(trace[3], trace[4])][2], int(trace[5]))

 
    ## Recombine stays that (1) don't have transit points between them and (2) are within the distance threshold.
    stays_combined = []
    all_stays = []

### get unique date time stamps
    day_set = list(user.keys())
    day_set.sort() # get unique date time stamps for users

### for the dates in day_set, we add the first date trace to all_stays
### after that if the current trace coordinate matches the most recent entry of all_stays, then append the trace to last entry of all_stays
### else add the trace as a new entry to all_stays
### all stays will be a nested list structure, like [[[],[],...],[[],[],...],[[],[],[]...],...] -> Here every second inside list will correspond for same location and date
    for a_day in day_set: # for each datetime stamp
        for a_location in user[a_day]: # for each trace
            if len(all_stays) == 0: # only append the first trace sorted by date time
                all_stays.append([a_location]) # confirm if trace is added or latitude or longitude
            else:
                last_stay = (all_stays[-1][-1][6], all_stays[-1][-1][7]) # get the latitude and longitude of the most recent addition
                if a_location[6] == last_stay[0] and a_location[7] == last_stay[1]: #  check if the latitudes and longitudes of last_stay and a_location are the same
                    all_stays[-1].append(a_location) # if it is true then add to last element of all_stays
                else:
                    all_stays.append([a_location]) # add as a seperate stay
### as soon as the nested entry from all_stays is added to stays_combined, it is removed from all_stays                   
    stay_index = 0
    stays_combined.append(all_stays[0]) # append first entry to stays_combined, having one element
    all_stays.pop(0) # remove the above added entry from the all_stays
    
    """
    Convert to float
    """
### below coordinares are conveerted to float and have corrdinates for last entry in stays_combined
    update_lat = float(stays_combined[-1][-1][6])
    update_long = float(stays_combined[-1][-1][7])
    
    while len(all_stays) > 0:
        current_stay = all_stays.pop(0) # get the first entry from all_stays nested list
        #[6:8] means stay_lat, stay_long, stay_unc
        if tuple(stays_combined[-1][-1][6:8]) == ('-1','-1'): # check if the last entry in stays_combined is a transient point
            stays_combined.append(current_stay) # add current_stay as list element
            update_lat = float(stays_combined[-1][-1][6]) # update last added element to float
            update_long = float(stays_combined[-1][-1][7])
        else: # if stays_combined not a transient point
            if tuple(current_stay[-1][6:8]) != ('-1','-1'): # if current_stay is a transient point
                # below checks is total distance between current_stay from line 201 and these coordinates from line 197 < 0.2 km
                if distance(tuple([float(x) for x in current_stay[-1][6:8]]), tuple([update_lat,update_long])).km < 0.2: # why 0.2?
                    stays_combined[-1].extend(current_stay) # expand the last entry to have current_stay in same nestes list
                    lat_set = set([float(x[6]) for x in stays_combined[-1]])
                    long_set = set([float(x[7]) for x in stays_combined[-1]])
                    update_lat = np.mean(list(lat_set)) # get mean for most current list in update_lat
                    update_long = np.mean(list(long_set)) # get mean
                else: # below checks is total distance between current_stay from line 201 and these coordinates from line 197 > 0.2 km 
                    stays_combined.append(current_stay) # add as a new nested entry list rather than extensing the previous nest
                    update_lat = float(stays_combined[-1][-1][6])
                    update_long = float(stays_combined[-1][-1][7])
            else: # if not a stay point for current_stay
                if len(all_stays) == 0: # if no more entries in all_stays
                    stays_combined.append(current_stay) # add as a new list to stays_combined
                else: # if there are still points left in all_stays
                    next_stay = all_stays.pop(0) # get next element in all_stays ie.e first entry
                    # check distance betweeen next_stay and update_lat and update_long
                    if distance(tuple([float(x) for x in next_stay[-1][6:8]]), tuple([update_lat,update_long])).km < 0.2:
                        # extend stays_combined with current_stay and next_stay
                        # discuss with Grace
                        stays_combined[-1].extend(current_stay)
                        stays_combined[-1].extend(next_stay)
                        lat_set = set([float(x[6]) for x in stays_combined[-1]])
                        long_set = set([float(x[7]) for x in stays_combined[-1]])
                        # get points which are not -1
                        lat_set = [x for x in lat_set if x != -1.0] # input is int
                        long_set = [x for x in long_set if x != -1.0]
                        # calculate mean for latitude and longitude
                        update_lat = np.mean(lat_set)
                        update_long = np.mean(long_set)
                    else: # if > 0.2 km
                        stays_combined.append(current_stay) # add as seperate entry
                        stays_combined.append(next_stay) # add as seperate entry
                        update_lat = float(stays_combined[-1][-1][6])
                        update_long = float(stays_combined[-1][-1][7])
    ### this goes through each nest in stays_combined, gets the mean latitude and longitude and appends these to stays_output
    stays_output = []
    for a_stay in stays_combined:
        # again get unique locations and convert them to float
        lat_set = set([float(x[6]) for x in a_stay])
        long_set = set([float(x[7]) for x in a_stay])
        # exclude -1 i.e. transient locations
        lat_set = [x for x in lat_set if x != -1.0]
        long_set = [x for x in long_set if x != -1.0]
        # get new_lat and new_long as mean if positive non -1 points are there
        if len(lat_set) > 0 and len(long_set) > 0:
            new_lat = np.mean(lat_set)
            new_long = np.mean(long_set)
        # if no other points rather than -1 are there then they are assigned as -1 itself
        else:
            new_lat = -1
            new_long = -1
        for i in range(0, len(a_stay)):
            # update the stay_lat and stay_long to new_lat and new_long IN TRACES
            a_stay[i][6] = str(new_lat)
            a_stay[i][7] = str(new_long)
        stays_output.append(a_stay)
        
    ##Convert stays into a disctionary
    dict_output = {}
    for a_stay in stays_output:
        for a_record in a_stay:
            start_date = a_record[-1][0:6] # need to confirm but this is taken from column human_start_t
            if start_date in dict_output:
                dict_output[start_date].append(a_record) # append if start date in output
            else:
                dict_output[start_date] = [a_record] # else make a new record
        
    return (dict_output) # for every user get the new updates traces with latitude asnd longitude as mean of clusters


def func(args):

    name, user, spatial_constraint, dur_constraint, outputFile = args

    if dur_constraint == -1:
        user = cluster_incremental(user, spatial_constraint)

    else:
        user = cluster_incremental(user, spatial_constraint, dur_constraint)
        

    with lock:
        f = open(outputFile, 'a')
        writeCSV = csv.writer(f, delimiter=',')

        for day in sorted(user.keys()):
            for trace in user[day]:
                trace[1] = name
                writeCSV.writerow(trace)
        
        f.close()

def main_function(inputFile, outputFile, spatial_constraint, dur_constraint):
    # Read and process traces for one bulk
    UserList = {name: defaultdict(list) for name in namechunk}

    with open(inputFile, 'r') as readfile:
        readCSV = csv.reader(readfile, delimiter=',')
        next(readCSV)  # Skip the header
        for row in readCSV:
            name = row[1]
            if name in UserList:
                UserList[name][row[-1][:6]].append(row)

    print("End reading")

    # Process user data
    for name in UserList:
        UserList[name] = cluster_incremental(UserList[name], spatial_constraint, dur_constraint)

    with lock:
        f = open(outputFile, 'a')
        writeCSV = csv.writer(f, delimiter=',')

        for day in sorted(UserList.keys()):
            for trace in UserList[day]:
                trace[1] = name
                writeCSV.writerow(trace)

        f.close()

# Rest of the script...


if __name__ == '__main__':
    '''
    param: 
        inputFile
        partitionThreshold
    '''
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    spatial_constraint = float(sys.argv[3])
    dur_constraint = int(sys.argv[4])

    # # tmp file
    # outputFile = outputFile.replace('.csv','_tmp.csv')

    # f = open(outputFile, 'w')
    # f.write('unix_start_t,user_ID,mark_1,orig_lat,orig_long,orig_unc,stay_lat,stay_long,stay_unc,stay_dur,stay_ind,human_start_t\n')
    # f.close()


    # l = Lock() # thread locker
    # pool = Pool(cpu_count(), initializer=init, initargs=(l,))

    # # fixed param
    # user_num_in_mem = 1000

    # usernamelist = set() # user names
    # with open(inputFile,'r') as csvfile:
    #     readCSV = csv.reader(csvfile, delimiter=',')
    #     for row in readCSV:
    #         #if not len(row) ==12 : continue
    #         usernamelist.add(row[1])  # get ID list; the second colume is userID
    # usernamelist = list(usernamelist)

    # print('total number of users to be processed: ', len(usernamelist))

    # def divide_chunks(usernamelist, n):
    #     for i in range(0, len(usernamelist), n): # looping till length usernamelist
    #         yield usernamelist[i:i + n]

    # usernamechunks = list(divide_chunks(usernamelist, user_num_in_mem))

    # print('number of chunks to be processed', len(usernamechunks))

    # ## read and process traces for one bulk
    # while (len(usernamechunks)):
    #     namechunk = usernamechunks.pop()
    #     print("Start processing bulk: ", len(usernamechunks) + 1, ' at time: ', time.strftime("%m%d-%H:%M"), ' memory: ', psutil.virtual_memory().percent)

    #     UserList = {name: defaultdict(list) for name in namechunk}

    #     with open(inputFile,'r') as readfile:
    #         readCSV = csv.reader(readfile, delimiter=',')
    #         next(readCSV)
    #         for row in readCSV:
    #             #if not len(row) ==12 : continue
    #             #if '.' not in row[3] or '.' not in row[4]: continue # debug a data issue: not '.' in lat or long
    #             #if(len(row[6].split('.'))>2 or len(row[7].split('.'))>2): continue
    #             #if row[6] == '-' or row[7] == '-': continue 
    #             name = row[1]
    #             if name in UserList:
    #                 UserList[name][row[-1][:6]].append(row)

    #     print("End reading")

    #     # pool 
    #     tasks = [pool.apply_async(func, (task,)) for task in [(name, UserList[name], spatial_constraint, dur_constraint, outputFile) for name in UserList]]

    #     finishit = [t.get() for t in tasks]
    #     '''
    #     for name in UserList:
    #         func((name, UserList[name], spatial_constraint, dur_constraint, outputFile))
    #     '''       
    # pool.close()
    # pool.join()


    # outputFile_real = outputFile.replace('_tmp.csv','.csv')
    # if os.path.isfile(outputFile_real):
    #     os.remove(outputFile_real)
    # os.rename(outputFile,outputFile_real)

def IC(inputFile,outputFile,spatial_constraint,dur_constraint):
        # tmp file
    outputFile = outputFile.replace('.csv','_tmp.csv')

    f = open(outputFile, 'w')
    f.write('unix_start_t,user_ID,mark_1,orig_lat,orig_long,orig_unc,stay_lat,stay_long,stay_unc,stay_dur,stay_ind,human_start_t\n')
    f.close()


    l = Lock() # thread locker
    pool = Pool(cpu_count(), initializer=init, initargs=(l,))

    # fixed param
    user_num_in_mem = 1000

    usernamelist = set() # user names
    with open(inputFile,'r') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            #if not len(row) ==12 : continue
            usernamelist.add(row[1])  # get ID list; the second colume is userID
    usernamelist = list(usernamelist)

    print('total number of users to be processed: ', len(usernamelist))

    def divide_chunks(usernamelist, n):
        for i in range(0, len(usernamelist), n): # looping till length usernamelist
            yield usernamelist[i:i + n]

    usernamechunks = list(divide_chunks(usernamelist, user_num_in_mem))

    print('number of chunks to be processed', len(usernamechunks))

    ## read and process traces for one bulk
    while (len(usernamechunks)):
        namechunk = usernamechunks.pop()
        print("Start processing bulk: ", len(usernamechunks) + 1, ' at time: ', time.strftime("%m%d-%H:%M"), ' memory: ', psutil.virtual_memory().percent)

        UserList = {name: defaultdict(list) for name in namechunk}

        with open(inputFile,'r') as readfile:
            readCSV = csv.reader(readfile, delimiter=',')
            next(readCSV)
            for row in readCSV:
                #if not len(row) ==12 : continue
                #if '.' not in row[3] or '.' not in row[4]: continue # debug a data issue: not '.' in lat or long
                #if(len(row[6].split('.'))>2 or len(row[7].split('.'))>2): continue
                #if row[6] == '-' or row[7] == '-': continue 
                name = row[1]
                if name in UserList:
                    UserList[name][row[-1][:6]].append(row)

        print("End reading")

        # pool 
        tasks = [pool.apply_async(func, (task,)) for task in [(name, UserList[name], spatial_constraint, dur_constraint, outputFile) for name in UserList]]

        finishit = [t.get() for t in tasks]
        '''
        for name in UserList:
            func((name, UserList[name], spatial_constraint, dur_constraint, outputFile))
        '''       
    pool.close()
    pool.join()


    outputFile_real = outputFile.replace('_tmp.csv','.csv')
    if os.path.isfile(outputFile_real):
        os.remove(outputFile_real)
    os.rename(outputFile,outputFile_real)
