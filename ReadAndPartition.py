'''
Read the user information(device location information), Timestamp, ID, ID Type, Latitude, Longitude, Accuracy(uncertainty radius), Human Time, 
then partition them into different parts by apply partition threshold on uncertainty radius of each record

input:
    raw user information
outout:
    partitioned user information: gps stay information / celluar stay information 

'''
from __future__ import print_function

import csv, time, os,  copy, psutil, sys
from collections import defaultdict
import shutil, json

from multiprocessing import Pool
from multiprocessing import current_process, Lock, cpu_count

def init(l):
    global lock
    lock = l

def partition(user, partition_Threshold):
    ## split into gps traces and cellular traces
    user_gps = {}
    user_cell = {}
    for d in user.keys():
        user_gps[d] = []
        user_cell[d] = []
        for trace in user[d]:
            if int(trace[5]) <= partition_Threshold:
                user_gps[d].append(trace)
            else:
                user_cell[d].append(trace)

    return user_gps, user_cell

def func(args):
    name, user, partitionThreshold, outputFileGps, outputFileCell = args

    userGps, userCell = partition(user,partitionThreshold)
    if(not len(userGps) or not len(userCell)): return

    # IO
    with lock:
        f1 = open(outputFileGps, 'a')
        writeCSV1 = csv.writer(f1, delimiter=',')

        f2 = open(outputFileCell, 'a')
        writeCSV2 = csv.writer(f2, delimiter=',')

        for day in sorted(userGps.keys()):
            for trace in userGps[day]:
                trace[1] = name
                writeCSV1.writerow(trace)
        f1.close()

        for day in sorted(userCell.keys()):
            for trace in userCell[day]:
                trace[1] = name
                writeCSV2.writerow(trace)
        f2.close()


if __name__ == '__main__':
    inputFile = sys.argv[1]
    outputFileGps = sys.argv[2]
    outputFileCell = sys.argv[3]
    partitionThreshold = int(sys.argv[4])


    f = open(outputFileGps, 'w')
    f.write('unix_start_t,user_ID,mark_1, \
            orig_lat,orig_long,orig_unc, \
            stay_lat,stay_long,stay_unc,\
            stay_dur,stay_ind,human_start_t\n')
    
    f.close()

    f = open(outputFileCell, 'w')
    f.write('unix_start_t,user_ID,mark_1,orig_lat,orig_long,orig_unc,stay_lat,stay_long,stay_unc,stay_dur,stay_ind,human_start_t\n')
    f.close()


    l = Lock() # thread locker
    pool = Pool(cpu_count(), initializer=init, initargs=(l,))

    # fixed param
    user_num_in_mem = 1000

    ## get time period covered by the data and user ID from file
    #day_list = set() # time period covered by the data
    usernamelist = set() # user names
    with open(inputFile,'r+') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            #day_list.add(row[-1][:6])  # the last colume is humantime, in format 200506082035
            usernamelist.add(row[1])  # get ID list; the second colume is userID
    #day_list = sorted(list(day_list))
    usernamelist = list(usernamelist)

    print('total number of users to be processed: ', len(usernamelist))


    def divide_chunks(usernamelist, n):
        for i in range(0, len(usernamelist), n): # looping till length usernamelist
            yield usernamelist[i:i + n]

    ## user_num_in_mem: How many elements each chunk should have
    usernamechunks = list(divide_chunks(usernamelist, user_num_in_mem))

    print('number of chunks to be processed', len(usernamechunks))

    ## read and process traces for one bulk
    while (len(usernamechunks)):
        namechunk = usernamechunks.pop()
        print("Start processing bulk: ", len(usernamechunks) + 1, ' at time: ', time.strftime("%m%d-%H:%M"), ' memory: ', psutil.virtual_memory().percent)

        UserList = {name: defaultdict(list) for name in namechunk}

        with open(inputFile,'r+') as readfile:
            readCSV = csv.reader(readfile, delimiter=',')
            for row in readCSV:
                if '.' not in row[3] or '.' not in row[4]: continue # debug a data issue: not '.' in lat or long
                name = row[1]
                if name in UserList:
                    UserList[name][row[-1][:6]].append(row)

        for name in UserList:
            for day in UserList[name]:
                for row in UserList[name][day]:
                    row[1] = None  # save memory: user id is long and cost memory
                    row[5] = int(float(row[5]))  # convert uncertainty radius to integer
                    row.extend([-1, -1, -1, -1, -1])# standardizing data structure; add -1 will be filled by info of stays
                    row[6], row[-1] = row[-1], row[6]  # push human time to the last column

        print("End reading")

        # pool 
        tasks = [pool.apply_async(func, (task,)) for task in [(name, UserList[name], partitionThreshold, outputFileGps, outputFileCell) for name in UserList]]

        finishit = [t.get() for t in tasks]
        '''
        for name in UserList:
            func((name, UserList[name], partitionThreshold, outputFileGps, outputFileCell))
        '''
    pool.close()
    pool.join()
