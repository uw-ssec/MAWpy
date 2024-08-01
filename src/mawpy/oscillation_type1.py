##9/11/2021 diagnosis
# 1. If a pair of traces appears multiple times in a trajectory, and one of them is identified as oscillation, then all occurrences of this pair will be "corrected" (even if other occurrences are not osscilation).
# 2. Only stay locations are corercted. Traces with oscillations are not dealt with.
# 3. Oscilation is required to bound back to the exact same location.
# 4. Line 115 should also consider trace locations (i.e. adding a trace to a stay)


"""
Things to cross check in the future:
1. While creating orig_index, the current trace is deleted from tracelist if the just previous trace has same GPS coordinates as well as duration. The difference in time stampsis not noted.
2. For creating the replacement dictionary, if a stay point is not found we just take the first index of SuspSequence as the replacing index - Why?
3. Time Complexity

"""

"""
remove oscillation traces
:param user:
:param dur_constr:
:return: oscill gps pair list
"""
# import geopy.distance

# -1 signifies it is not a stay location


def oscillation_h1_oscill(user, dur_constr):
    # user = user#arg[0]
    TimeWindow = dur_constr  # arg[1]#5 * 60
    """
    The below block goes through each trace in the user dictionary and appends each trace as a list in tracelist by transforming some columns of the traces.
    """
    tracelist = []  # we will be working on tracelist not user
    for d in sorted(
        user.keys()
    ):  # need to understand how this data is fed in the function
        user[d].sort(
            key=lambda x: int(x[0])
        )  # sorting takes place based on x[0] which is the date time
        for trace in user[
            d
        ]:  # iterating through all rows for the dictionary key (need to confirm if it is user or datetime?)
            dur_i = (
                1 if int(trace[9]) == -1 else int(trace[9])
            )  # trace[9] means the column stay_dur, which tells if it is a stay point or not; -1 signifies no stay point so a duration of 1 second is assigned here
            lat = (
                trace[3] if float(trace[6]) == -1 else trace[6]
            )  # trace[6] is stay_lat and lat is assigned the same as actual lat i.e trace[3] if stay_lat is -1 i.e. a latitude corresponding to a stay point
            long = (
                trace[4] if float(trace[7]) == -1 else trace[7]
            )  # trace[7] is stay_long and long is assigned the same as actual long i.e trace[4] if stay_long is -1 i.e. a longitude corresponding to a stay point
            rad = (
                trace[5] if int(trace[8]) == -1 else trace[8]
            )  # trace[8] is stay_unc and rad is assigned the same as actual uncertainty i.e trace[5] if stay_unc is -1
            stay_not = (
                0 if float(trace[6]) == -1 else 1
            )  # trace[6] is stay_lat and if it is -1 the stay_not = 0 which means it is not a stay point
            tracelist.append(
                [trace[1], trace[0], dur_i, lat, long, rad, stay_not]
            )  # all above parameters are added in a nested list tracelist
            # format of each record: [ID, time, duration, lat, long, uncertainty_radius]

    """
    Original index only keeps tab of the index of those traces where consequent traces do not share the same latitude - longitude pair and duration. The entry from tracelist is deleted if the current trace has same coordinates and duration as the previous entry.
    """
    # integrate: only one record representing one stay (i-i records)
    orig_index = []  # blank list to capture the indices
    i = 0
    while i < len(tracelist) - 1:
        orig_index.append(i)
        if (
            tracelist[i + 1][2:5] == tracelist[i][2:5]
        ):  # in case for the entries in tracelist nested list, if the GPS coordinates of two consequent lists are same, then delete one
            del tracelist[i + 1]
        else:
            i += 1
    orig_index.append(
        i
    )  # only keep the index where the next trace is not at the same location

    """
    gps_dur_count tracks the total time in seconds spent at each unique latitude-longitude combination.
    """
    # get gps list from tracelist
    gpslist = [
        (trace[3], trace[4]) for trace in tracelist
    ]  # get GPS coordinates in terms of latitude and longitude for each entry in tracelist
    # unique gps list
    uniqList = list(set(gpslist))  # take a set to get unique values
    gps_dur_count = {
        item: 0 for item in uniqList
    }  # create a dictionary where the key is the unique GPS coordinates i.e (lat, long) -> the value will be the duration stayed at that location
    for tr in tracelist:
        if (
            int(tr[2]) == 0
        ):  # if value of dur_i from tracelist is 0 then we add 1 second to the lat-long pair (Need to confirm if it will be 0?)
            gps_dur_count[(tr[3], tr[4])] += 1
        else:
            gps_dur_count[(tr[3], tr[4])] += int(
                tr[2]
            )  # else we add the actual duration

    # All prepared
    oscillation_pairs = []
    t_start = 0

    """
    SuspSequnce tracks those traces in the tracelist list where the time difference between any two traces is below a minimum time requirement. SuspSequence is then appended to Oscillation pairs
    if the length of SuspSequence for a given starting index is more than two and flag_circle is True. flag_circle becomes True if for two traces falling within the time requirement share the same location.
    If a SuspSequence is added to oscillation pair, we start searching from the trace in tracelist whose index is the last index in SuspSequence.
    """
    # replace pong by ping; be aware that "tracelistno_original==tracelist"
    flag_find_circle = False
    while t_start < len(tracelist):
        flag_find_circle = (
            False  # initially set up to False to check breaking of loop down
        )
        suspSequence = []
        suspSequence.append(t_start)  # append the first index from tracelist
        for t in range(t_start + 1, len(tracelist)):  # get the suspicious sequence
            if (
                int(tracelist[t][1])
                <= int(tracelist[t_start][1]) + int(tracelist[t_start][2]) + TimeWindow
            ):  # if for a given index from tracelist this condition meets
                suspSequence.append(
                    t
                )  # add the index of the trace from tracelist (each index corresponds to one nested list in tracelist)
                # loc1 = (float(tracelist[t][3]),float(tracelist[t][4]))
                # loc2 = (float(tracelist[t_start][3]),float(tracelist[t_start][4]))
                if (
                    tracelist[t][3:5] == tracelist[t_start][3:5]
                ):  # geopy.distance.geodesic(loc1, loc2).meters < 300:   # if the GPS coordinates match then break the loop where time condition meets from the above
                    flag_find_circle = True
                    break
            else:
                break

        # check circles
        if (
            flag_find_circle == True and len(suspSequence) > 2
        ):  # not itself ## it checks if flag is true and if the list susSequence has more than 2 index
            oscillation_pairs.append(
                suspSequence
            )  # append the index to oscialltion pairs as a list i.e. oscialltion pair will also be a nested list structure
            t_start = suspSequence[
                -1
            ]  # + 1 # start t_start from the last index in suspSequence
        else:
            t_start += 1  # else start from next index in tracelist

    """
    For each SuspSequence added to oscillation pair, we go through individual indices in Suspsequence and check if any index has a stay point associated using tracelist.
    If we do find an index corresponding to a stay point, we sort the indices in SuspSequence based on GPS coordinates total duration and take the replacing
    index to be the one with largest GPS duration from gps_dur_count. If a stay point is not found, we set replacing equal to the first element of Suspsequence.
    We treat each element in oscillation pair as the key for the replacement dictionary, and we set the value to be equal to replacing.
    """
    replacement = {}  # dictionary to capture oscillation
    for (
        pair
    ) in oscillation_pairs:  # go through each nested list captured in oscillation pairs
        stay_indicators = [
            tracelist[x][-1] for x in pair
        ]  # for a given nested list, it will go to each index in this nested list, retrieve the element at that index and get the saty_not variable
        if 1 in stay_indicators:  # if one of the indices correspond to a stay point
            replacing = sorted(
                pair, key=lambda x: gps_dur_count[(tracelist[x][3], tracelist[x][4])]
            )[-1]  # for the highest numbered index, get the GPS coordinates
        else:
            replacing = pair[0]  # take first index in pair
        for to_be_replaced in pair:
            replacement[to_be_replaced] = (
                replacing  # replace each index in oscialltion pair with the to_be_replaced value from above
            )

    # find pong in trajactory, and replace it with ping
    # this part is original outside this function
    # OscillationPairList is in format: {, (ping[0], ping[1]): (pong[0], pong[1])}
    """
    We go through each trace in the original input file and for each trace, if the index is present in orig_index, we replace it with the coordinates of the trace based on the replacement dictionary and return the
    updated traces.
    """
    ind = 0
    for d in sorted(user.keys()):
        for trace in user[d]:
            if orig_index[ind] in replacement:
                candidate_trace = tracelist[
                    replacement[orig_index[ind]]
                ]  # get the replacement trace
                if candidate_trace[-1] == 1:  # if stay point
                    trace[6], trace[7] = (
                        candidate_trace[3],
                        candidate_trace[4],
                    )  # update columns stay_lat, stay_long
                    trace[3], trace[4] = candidate_trace[3], candidate_trace[4]
                else:
                    trace[3], trace[4] = (
                        candidate_trace[3],
                        candidate_trace[4],
                    )  # else update the original lat and long
            ind += 1

    return user  # return updated traces


# deel trace list without checking diff in time stamp
# trace[0] taken or not say logic
# time and apace complexity
