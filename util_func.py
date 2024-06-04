


def update_duration(user, dur_constr):
    """

        :param user:
        :return:
        """
    for d in user.keys():
        for trace in user[d]: trace[9] = -1  # clear needed! #modify grid
        i = 0
        j = i
        while i < len(user[d]):
            if j >= len(user[d]):  # a day ending with a stay, j goes beyond the last observation
                dur = str(int(user[d][j - 1][0]) + max(0, int(user[d][j - 1][9])) - int(user[d][i][0]))
                for k in range(i, j, 1):
                    user[d][k][9] = dur
                break
            if user[d][j][6] == user[d][i][6] and user[d][j][7] == user[d][i][7] and j < len(user[d]):
                j += 1
            else:
                dur = str(int(user[d][j - 1][0]) + max(0, int(user[d][j - 1][9])) - int(user[d][i][0]))
                for k in range(i, j, 1):
                    user[d][k][9] = dur
                i = j

    for d in user.keys():
        for trace in user[d]:
            # those trace with gps as -1,-1 (not clustered) should not assign a duration
            #print(trace)
            #if(str(trace[6])[-2]=='.'): continue
            if float(trace[6]) == -1: trace[9] = -1
            ## our default output format: give -1 to non-stay records
            if float(trace[9]) < dur_constr: # change back keep full trajectory: do not use center for those are not stays
                trace[6], trace[7], trace[8], trace[9] = -1, -1, -1, -1  # for no stay, do not give center

    return user


