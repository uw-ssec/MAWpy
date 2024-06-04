'''
Update the duration of each stay information of one user
if the duration is smaller than duration constraint threshold, remove the point from the cluster center 

input:
    user stay information
    duration constraint threshold 
outout:
    updated user stay information
'''

import sys, json,os, psutil, csv, time, func_timeout
import numpy as np
from distance import distance
from class_cluster import cluster
from collections import defaultdict
from multiprocessing import Pool
from multiprocessing import current_process, Lock, cpu_count

def init(l):
    global lock
    lock = l
"""
As per my assumption that data structure is of the form of a nested list, the key being the user id and outer list all traces for the
user, whereas inner traces being a list appended to it like {user1}:[[trace1], [trace2],[trace3],[trace4]...]
"""
"""
i and j are used to define the lower and upper bounds of a stay block. 
The first if statement calculates the stay duration for the last segment left when j reaches the last trace
The second if statement checks if i and j share the same position, when j does not reach the end of the trace and increments j
The else block gets executed when i and j share different positions i.e. we enter the next block of stays and this else calculates duration for the previous stay block

The next for loop then checks if the stay duration calculated in the first for loop < the duration constraint and updates the column accordingly
"""
def update_duration(user, dur_constr, order_of_execution = 1):
    """
        :param user:
        :return:
    """
    for d in user.keys():
        # trace[9] is the column stay_dur and all values are made as -1
        for trace in user[d]:
            if order_of_execution != 1: # 1 means first step -> change made on 05/31
                trace[9] = trace[9]
            else:
                trace[9] = -1  # clear needed! #modify grid
        i = 0
        j = i
        while i < len(user[d]): # number of traces for a given user
            if j >= len(user[d]):  # a day ending with a stay, j goes beyond the last observation
                #user[d][i][0] -> This is the start time of the trace for the user
                #user[d][j - 1][0]-> This is the start time of the trace before the next set of stays
                dur = str(int(user[d][j - 1][0]) + max(0, int(user[d][j - 1][9])) - int(user[d][i][0])) # 0 corresponds to unix_start_t and 9 the stay_dur
                for k in range(i, j, 1):
                    user[d][k][9] = dur # update all rows between i and j for the user with the dur calculated above i.e. all values b/w i and j have same stay
                break # since no more traces are left now
            if user[d][j][6] == user[d][i][6] and user[d][j][7] == user[d][i][7] and j < len(user[d]): # [6] is stay_lat and [7] is stay_long
                j += 1 # increment the value of j to +1
            else: # when coordinates for i and j change this block code executes; the below code calculates the duration for previous stay
                #Why is it -1 everywhere?
                dur = str(int(user[d][j - 1][0]) + max(0, int(user[d][j - 1][9])) - int(user[d][i][0])) # 0 corresponds to unix_start_t and 9 the stay_dur
                for k in range(i, j, 1):
                    user[d][k][9] = dur # update all rows between i and j for the user with the dur calculated above
                i = j

    for d in user.keys():
        for trace in user[d]:
            # those trace with gps as -1,-1 (not clustered) should not assign a duration
            # print(trace)
            # if(str(trace[6])[-2]=='.'): continue
            if float(trace[6]) == -1: trace[9] = -1 # if stay_lat is -1 make stay_dur as -1
            ## our default output format: give -1 to non-stay records
            if float(trace[9]) < dur_constr: # change back keep full trajectory: do not use center for those are not stays
                # make everything -1 is stay_dur < constarint
                trace[6], trace[7], trace[8], trace[9] = -1, -1, -1, -1  # for no stay, do not give center

    return user


def func(args):
    name, user, dur_constraint, outputFile = args
    try:
        user = update_duration(user,dur_constraint)
        
        with lock:
            f = open(outputFile, 'a')
            writeCSV = csv.writer(f, delimiter=',')

            for day in sorted(user.keys()):
                for trace in user[day]:
                    trace[1] = name
                    writeCSV.writerow(trace)
            f.close()
    except:
        return

if __name__ == '__main__':
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    duration_constraint = int(sys.argv[3])


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
    #         if not len(row) ==12 : continue
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
    #             #if not len(row) ==12 or len(row[0])==0: continue
    #             #if '.' not in row[3] or '.' not in row[4]: continue # debug a data issue: not '.' in lat or long
    #             #if(len(row[6].split('.'))>2 or len(row[7].split('.'))>2): continue
    #             #if (('-' in row[6]) and (not row[6][0]=='-')) or (('-' in row[7]) and (not row[7][0]=='-')): continue
    #             name = row[1]
    #             if name in UserList:
    #                 UserList[name][row[-1][:6]].append(row)

    #     print("End reading")

    #     # pool 
    #     tasks = [pool.apply_async(func, (task,)) for task in [(name, UserList[name], duration_constraint, outputFile) for name in UserList]]

    #     finishit = [t.get() for t in tasks]

    #     '''
    #     for name in UserList:
    #         func((name, UserList[name], duration_constraint, outputFile))
    #     '''

    # pool.close()
    # pool.join()

    # outputFile_real = outputFile.replace('_tmp.csv','.csv')
    # if os.path.isfile(outputFile_real):
    #     os.remove(outputFile_real)
    # os.rename(outputFile,outputFile_real)


def USD(inputFile,outputFile,duration_constraint):
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
            if not len(row) ==12 : continue
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
                #if not len(row) ==12 or len(row[0])==0: continue
                #if '.' not in row[3] or '.' not in row[4]: continue # debug a data issue: not '.' in lat or long
                #if(len(row[6].split('.'))>2 or len(row[7].split('.'))>2): continue
                #if (('-' in row[6]) and (not row[6][0]=='-')) or (('-' in row[7]) and (not row[7][0]=='-')): continue
                name = row[1]
                if name in UserList:
                    UserList[name][row[-1][:6]].append(row)

        print("End reading")

        # pool 
        tasks = [pool.apply_async(func, (task,)) for task in [(name, UserList[name], duration_constraint, outputFile) for name in UserList]]

        finishit = [t.get() for t in tasks]

        '''
        for name in UserList:
            func((name, UserList[name], duration_constraint, outputFile))
        '''

    pool.close()
    pool.join()

    outputFile_real = outputFile.replace('_tmp.csv','.csv')
    if os.path.isfile(outputFile_real):
        os.remove(outputFile_real)
    os.rename(outputFile,outputFile_real)