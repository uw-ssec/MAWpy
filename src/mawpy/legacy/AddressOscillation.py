import sys
import os
import psutil
import csv
import time
from collections import defaultdict
from multiprocessing import Pool
from multiprocessing import Lock, cpu_count

from mawpy.legacy.oscillation_type1 import oscillation_h1_oscill


def init(l):
    global lock
    lock = l


"""
The below code runs a try-exception block and runs the function from oscillation_type_1 putting output in the output file.
"""


def func(args):
    name, user, dur_constraint, outputFile = args
    try:
        user = oscillation_h1_oscill(user, dur_constraint)
        with lock:
            f = open(outputFile, "a")
            writeCSV = csv.writer(f, delimiter=",")

            for day in sorted(user.keys()):
                for trace in user[day]:
                    trace[1] = name
                    writeCSV.writerow(trace)
            f.close()
    except:
        return


if __name__ == "__main__":
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
    #     next(readCSV)
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

"""
Below function is creating the user dictionary to be used in the function in below structure -

{ user_id : {'datestamp1': trace, 'datestamp2': trace, 'datestamp3':trace'...}}
"""


def AO(inputFile, outputFile, duration_constraint):
    outputFile = outputFile.replace(".csv", "_tmp.csv")

    f = open(outputFile, "w")
    f.write(
        "unix_start_t,user_ID,mark_1,orig_lat,orig_long,orig_unc,stay_lat,stay_long,stay_unc,stay_dur,stay_ind,human_start_t\n"
    )
    f.close()

    l = Lock()  # thread locker
    pool = Pool(cpu_count(), initializer=init, initargs=(l,))

    # fixed param
    user_num_in_mem = 1000

    # Get user ids to be processed
    usernamelist = set()  # user names
    with open(inputFile, "r") as csvfile:
        readCSV = csv.reader(csvfile, delimiter=",")
        next(readCSV)
        for row in readCSV:
            # if not len(row) ==12 : continue
            usernamelist.add(row[1])  # get ID list; the second colume is userID
    usernamelist = list(usernamelist)

    print("total number of users to be processed: ", len(usernamelist))

    # Define the chunks or bins to be used for processing.
    def divide_chunks(usernamelist, n):
        for i in range(0, len(usernamelist), n):  # looping till length usernamelist
            yield usernamelist[i : i + n]

    usernamechunks = list(divide_chunks(usernamelist, user_num_in_mem))

    print("number of chunks to be processed", len(usernamechunks))

    ## read and process traces for one bulk
    while len(usernamechunks):
        namechunk = usernamechunks.pop()
        print(
            "Start processing bulk: ",
            len(usernamechunks) + 1,
            " at time: ",
            time.strftime("%m%d-%H:%M"),
            " memory: ",
            psutil.virtual_memory().percent,
        )

        UserList = {name: defaultdict(list) for name in namechunk}

        with open(inputFile, "r") as readfile:
            readCSV = csv.reader(readfile, delimiter=",")
            next(readCSV)
            for row in readCSV:
                # if not len(row) ==12 : continue
                # if '.' not in row[3] or '.' not in row[4]: continue # debug a data issue: not '.' in lat or long
                name = row[1]
                if name in UserList:
                    UserList[name][row[-1][:6]].append(row)

        print("End reading")

        # pool
        tasks = [
            pool.apply_async(func, (task,))
            for task in [
                (name, UserList[name], duration_constraint, outputFile)
                for name in UserList
            ]
        ]

        finishit = [t.get() for t in tasks]
        """
        for name in UserList:
            func((name, UserList[name], duration_constraint, outputFile))
        """
    pool.close()
    pool.join()

    outputFile_real = outputFile.replace("_tmp.csv", ".csv")
    if os.path.isfile(outputFile_real):
        os.remove(outputFile_real)
    os.rename(outputFile, outputFile_real)
