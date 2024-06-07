from operator import itemgetter

from src.mawpy.oscillation_type1 import oscillation_h1_oscill
from incremental_clustering import cluster_incremental
from src.mawpy.util_func import update_duration

import sys
import os
import psutil
import csv
import time
from distance import distance
from collections import defaultdict
from multiprocessing import Pool
from multiprocessing import Lock, cpu_count


def init(l):
    global lock
    lock = l


def combineGPSandPhoneStops(arg):
    user_gps, user_cell, dur_constr, spat_constr_gps, spat_cell_split = arg

    # combine cellular stay if it is close to a gps stay
    cell_stays = list(
        set(
            [
                (trace[6], trace[7])
                for d in user_cell
                for trace in user_cell[d]
                if int(trace[9]) >= dur_constr
            ]
        )
    )
    gps_stays = list(
        set(
            [
                (trace[6], trace[7])
                for d in user_gps
                for trace in user_gps[d]
                if int(trace[9]) >= dur_constr
            ]
        )
    )
    pairs_close = set()
    for cell_stay in cell_stays:
        for gps_stay in gps_stays:
            if (
                distance(cell_stay[0], cell_stay[1], gps_stay[0], gps_stay[1])
                <= spat_constr_gps
            ):
                pairs_close.add((gps_stay[0], gps_stay[1], cell_stay[0], cell_stay[1]))
                break
    # find all pair[1]s in list, and replace it with pair[0]
    for pair in list(pairs_close):
        for d in user_cell.keys():
            for trace in user_cell[d]:
                if trace[6] == pair[2] and trace[7] == pair[3]:
                    trace[5], trace[6], trace[7] = (
                        99,
                        pair[0],
                        pair[1],
                    )  # pretend as gps

    user = user_gps
    for d in user.keys():
        if len(user_cell[d]):
            user[d].extend(user_cell[d])
            user[d] = sorted(user[d], key=itemgetter(0))

    # address oscillation
    user = oscillation_h1_oscill(
        user, dur_constr
    )  # OscillationPairList = oscillation_h1_oscill(user, dur_constr)
    # ## when replaced, can only replaced with a gps stay; so let modify exchange ping-pong pair in the pairList
    # gpslist_temp = {(trace[6], trace[7]):int(trace[5]) for d in user.keys() for trace in user[d]}
    # for pair_i in range(len(OscillationPairList)):
    #     if gpslist_temp[(OscillationPairList[pair_i][0],OscillationPairList[pair_i][1])] <= spat_constr_gps:# wrong(2,3)
    #         OscillationPairList[pair_i] = [OscillationPairList[pair_i][2],OscillationPairList[pair_i][3],
    #                                        OscillationPairList[pair_i][0],OscillationPairList[pair_i][1]]
    ## find pong in trajactory, and replace it with ping
    ## this part is now integreted into the function itself
    ## OscillationPairList is in format: {, (ping[0], ping[1]): (pong[0], pong[1])}
    # for d in user.keys():
    #     for trace in user[d]:
    #         if (trace[6], trace[7]) in OscillationPairList:
    #             trace[6], trace[7] = OscillationPairList[(trace[6], trace[7])]

    # update duration
    user = update_duration(user, dur_constr)

    for d in user:
        phone_index = [
            k for k in range(len(user[d])) if int(user[d][k][5]) > spat_cell_split
        ]
        if len(phone_index) == 0:  # if no phone trace
            continue
        for i in range(len(user[d])):
            if (
                int(user[d][i][5]) > spat_cell_split and int(user[d][i][9]) < dur_constr
            ):  # passing phone observe
                user[d][i].append("checked")
        # combine consecutive obsv on a phone stay into two observe
        i = min(phone_index)  # i has to be a phone index
        j = i + 1
        while i < len(user[d]) - 1:
            if j >= len(
                user[d]
            ):  # a day ending with a stay, j goes beyond the last observation
                for k in range(i + 1, j - 1, 1):
                    user[d][k] = []
                break
            if (
                int(user[d][j][5]) > spat_cell_split
                and user[d][j][6] == user[d][i][6]
                and user[d][j][7] == user[d][i][7]
                and j < len(user[d])
            ):
                j += 1
            else:
                for k in range(i + 1, j - 1, 1):
                    user[d][k] = []
                phone_index = [
                    k
                    for k in range(j, len(user[d]))
                    if int(user[d][k][5]) > spat_cell_split
                ]
                if len(phone_index) < 3:  # if no phone trace
                    break
                i = min(phone_index)  ##i has to be a phone index
                j = i + 1
        i = 0  # remove []
        while i < len(user[d]):
            if len(user[d][i]) == 0:
                del user[d][i]
            else:
                i += 1
        # address phone stay one by one
        flag_changed = True
        phone_list_check = []
        while flag_changed:
            # print('while........')
            flag_changed = False
            gps_list = []
            phone_list = []
            for i in range(len(user[d])):
                if (
                    int(user[d][i][5]) <= spat_cell_split
                ):  # or user[d][i][2] == 'addedphonestay': #changed on 0428
                    gps_list.append(user[d][i])
                else:
                    phone_list.append(user[d][i])

            phone_list.extend(phone_list_check)
            # when updating duration for phone stay, we have to put back passing obs
            phone_list = sorted(phone_list, key=itemgetter(0))
            # update phone stay
            i = 0
            j = i
            while i < len(phone_list):
                if j >= len(
                    phone_list
                ):  # a day ending with a stay, j goes beyond the last observation
                    dur = str(int(phone_list[j - 1][0]) - int(phone_list[i][0]))
                    for k in range(i, j, 1):
                        if int(phone_list[k][9]) >= dur_constr:
                            # we don't want to change a pssing into a stay; as  we have not process the combine this stay
                            # this is possible when a stay that prevents two passing is mergeed into gps as gps points
                            phone_list[k][9] = dur
                    break
                if (
                    phone_list[j][6] == phone_list[i][6]
                    and phone_list[j][7] == phone_list[i][7]
                    and j < len(phone_list)
                ):
                    j += 1
                else:
                    dur = str(int(phone_list[j - 1][0]) - int(phone_list[i][0]))
                    for k in range(i, j, 1):
                        if int(phone_list[k][9]) >= dur_constr:
                            phone_list[k][9] = dur
                    i = j
            for trace in phone_list:  # those trace with gps as -1,-1 (not clustered) should not assign a duration
                if float(trace[6]) == -1:
                    trace[9] = -1
            if len(phone_list) == 1:
                phone_list[0][9] = -1

            # update check label
            for i in range(len(phone_list)):
                if (
                    int(phone_list[i][5]) > spat_cell_split
                    and int(phone_list[i][9]) < dur_constr
                    and phone_list[i][-1] != "checked"
                ):
                    # passing phone observe
                    phone_list[i].append("checked")

            # put those not checked together with gps
            user[d] = gps_list
            phone_list_check = []
            for i in range(len(phone_list)):
                if phone_list[i][-1] == "checked":
                    phone_list_check.append(phone_list[i])
                else:
                    user[d].append(phone_list[i])
            user[d] = sorted(user[d], key=itemgetter(0))

            # find a stay which is not checked
            flag_phonestay_notchecked = False
            phonestay_left, phonestay_right = -1, -1
            for i in range(max(0, phonestay_right + 1), len(user[d])):
                phonestay_left, phonestay_right = -1, -1
                if (
                    int(user[d][i][5]) > spat_cell_split
                    and int(user[d][i][9]) >= dur_constr
                    and user[d][i][-1] != "checked"
                ):
                    phonestay_left = phonestay_right
                    phonestay_right = i
                if (
                    phonestay_left != -1
                    and phonestay_right != -1
                    and user[d][phonestay_left][9] == user[d][phonestay_right][9]
                ):
                    flag_phonestay_notchecked = True

                ## modified on 04152019
                if (
                    flag_phonestay_notchecked == False or len(phone_list) == 0
                ):  # if all phone observation are checked, end
                    break
                # if they are not two consecutive observation
                if (
                    phonestay_right != phonestay_left + 1
                ):  # attention: only phonestay_left is addressed
                    # not consecutive two observations
                    if any(
                        [
                            int(user[d][j][9]) >= dur_constr
                            for j in range(phonestay_left + 1, phonestay_right, 1)
                        ]
                    ):
                        # found a gps stay in betw
                        # print('23: found a gps stay in betw, just use one gps stay trade one phone stay')
                        temp = user[d][phonestay_left][6:]
                        user[d][phonestay_left][6:] = [
                            -1,
                            -1,
                            -1,
                            -1,
                            -1,
                            -1,
                        ]  # phone disappear
                        # user[d][phonestay_left].extend(temp)
                        user[d][phonestay_left].append("checked")
                        # del user[d][phonestay_left]  # phone disappear
                        flag_changed = True
                    else:  # find close gps
                        # print('24: do not found a gps stay in betw')
                        phone_uncernt = max(
                            [
                                int(user[d][phonestay_left][8]),
                                int(user[d][phonestay_left][5]),
                                int(user[d][phonestay_right][5]),
                            ]
                        )
                        if all(
                            [
                                (phone_uncernt + int(user[d][j][5]))
                                > 1000
                                * distance(
                                    user[d][j][3],
                                    user[d][j][4],
                                    user[d][phonestay_left][6],
                                    user[d][phonestay_left][7],
                                )
                                for j in range(phonestay_left + 1, phonestay_right, 1)
                            ]
                        ):
                            # total uncerty larger than distance
                            # this case should be rare, as those close gps may be clustered
                            # print('241: all gps falling betw are close with phone stay')
                            temp = user[d][phonestay_left][3:]  # copy neighbor gps
                            user[d][phonestay_left][3:] = user[d][phonestay_left + 1][
                                3:
                            ]
                            user[d][phonestay_left][11] = temp[8]
                            # user[d][phonestay_left].extend(temp)
                            flag_changed = True
                        else:
                            # print('242: find a gps in betw,
                            # which is far away with phone stay, contradic with a stay (with phone obsv)')
                            temp = user[d][phonestay_left][6:]
                            user[d][phonestay_left][6:] = [
                                -1,
                                -1,
                                -1,
                                -1,
                                -1,
                                -1,
                            ]  # phone disappear
                            # user[d][phonestay_left].extend(temp)
                            user[d][phonestay_left].append("checked")
                            # del user[d][phonestay_left]  # phone disappear
                            flag_changed = True
                else:  # if they are two consecutive traces
                    # two consecutive observation
                    # if phonestay_left != 0 and phonestay_right < len(user[d]) - 1:
                    # ignore if they are at the beginning or the end of traj
                    prev_gps = next_gps = 0  # find previous and next gps
                    found_prev_gps = False
                    found_next_gps = False
                    for prev in range(phonestay_left - 1, -1, -1):
                        # if int(user[d][prev][5]) <= spat_cell_split: ########## changed on 04282018
                        if (
                            int(user[d][prev][5]) <= spat_cell_split
                            and int(user[d][prev][9]) >= dur_constr
                        ):
                            prev_gps = prev
                            found_prev_gps = True
                            break
                    for nxt in range(phonestay_right + 1, len(user[d])):
                        if (
                            int(user[d][nxt][5]) <= spat_cell_split
                            and int(user[d][nxt][9]) >= dur_constr
                        ):
                            next_gps = nxt
                            found_next_gps = True
                            break

                    if (
                        found_prev_gps
                        and found_next_gps
                        and user[d][prev_gps][6] == user[d][next_gps][6]
                    ):
                        # this is a phone stay within a gps stay
                        phone_uncernt = max(
                            [
                                int(user[d][phonestay_left][8]),
                                int(user[d][phonestay_left][5]),
                                int(user[d][phonestay_right][5]),
                            ]
                        )
                        gps_uncernt = int(user[d][prev_gps][8])
                        dist = 1000 * distance(
                            user[d][prev_gps][6],
                            user[d][prev_gps][7],
                            user[d][phonestay_left][6],
                            user[d][phonestay_left][7],
                        )
                        speed_dep = (
                            (dist - phone_uncernt - gps_uncernt)
                            / (
                                int(user[d][phonestay_left][0])
                                - int(user[d][prev_gps][0])
                            )
                            * 3.6
                        )
                        speed_retn = (
                            (dist - phone_uncernt - gps_uncernt)
                            / (
                                int(user[d][next_gps][0])
                                - int(user[d][phonestay_right][0])
                            )
                            * 3.6
                        )
                        if (
                            (dist - phone_uncernt - gps_uncernt) > 0
                            and dist > 1000 * spat_constr_gps
                            and speed_dep < 200
                            and speed_retn < 200
                        ):
                            # print('1111: distance larger than acc, and can travel, add phone stay, shorten gps stay')
                            # leave phone stay there, we later update duration for the gps stay
                            user[d][phonestay_left].append("checked")
                            # those phone stay not removed have to be marked with 'checked'!
                            user[d][phonestay_right].append("checked")
                            user[d][phonestay_left][2] = "addedphonestay"
                            user[d][phonestay_right][2] = "addedphonestay"
                            flag_changed = True
                        else:  # merge into gps stay
                            # print('1112: distance less than acc, or cannot travel, merge into gps stay')
                            temp = user[d][phonestay_left][3:]
                            user[d][phonestay_left][3:] = user[d][prev_gps][3:]
                            user[d][phonestay_left][11] = temp[8]
                            # user[d][phonestay_left].extend(temp)
                            temp = user[d][phonestay_right][3:]
                            user[d][phonestay_right][3:] = user[d][prev_gps][3:]
                            user[d][phonestay_right][11] = temp[8]
                            # user[d][phonestay_right].extend(temp)
                            flag_changed = True
                    elif (
                        found_prev_gps
                        and found_next_gps
                        and user[d][prev_gps][6] != user[d][next_gps][6]
                    ):
                        phone_uncernt_l = max(
                            [
                                int(user[d][phonestay_left][8]),
                                int(user[d][phonestay_left][5]),
                                int(user[d][phonestay_right][5]),
                            ]
                        )
                        gps_uncernt_l = int(user[d][prev_gps][8])
                        dist_l = 1000 * distance(
                            user[d][prev_gps][6],
                            user[d][prev_gps][7],
                            user[d][phonestay_left][6],
                            user[d][phonestay_left][7],
                        )
                        speed_dep = (
                            (dist_l - phone_uncernt_l - gps_uncernt_l)
                            / (
                                int(user[d][phonestay_left][0])
                                - int(user[d][prev_gps][0])
                            )
                            * 3.6
                        )
                        phone_uncernt_r = max(
                            [
                                int(user[d][phonestay_left][8]),
                                int(user[d][phonestay_left][5]),
                                int(user[d][phonestay_right][5]),
                            ]
                        )
                        gps_uncernt_r = int(user[d][next_gps][8])
                        dist_r = 1000 * distance(
                            user[d][next_gps][6],
                            user[d][next_gps][7],
                            user[d][phonestay_right][6],
                            user[d][phonestay_right][7],
                        )
                        speed_retn = (
                            (dist_r - phone_uncernt_r - gps_uncernt_r)
                            / (
                                int(user[d][next_gps][0])
                                - int(user[d][phonestay_right][0])
                            )
                            * 3.6
                        )
                        comb_l = 0  # revised on 03202019 to pick up one gps stay to combine with; if spatial conti with multi
                        comb_r = 0
                        if (
                            (dist_l - phone_uncernt_l - gps_uncernt_l) < 0
                            or dist_l < 1000 * spat_constr_gps
                            or speed_dep > 200
                        ):
                            comb_l = 1
                        if (
                            (dist_r - phone_uncernt_r - gps_uncernt_r) < 0
                            or dist_r < 1000 * spat_constr_gps
                            or speed_retn > 200
                        ):
                            comb_r = 1
                        if comb_l * comb_r == 1:
                            if dist_l < dist_r:
                                comb_r = 0
                            else:
                                comb_l = 0
                        if comb_l:
                            temp = user[d][phonestay_left][3:]
                            user[d][phonestay_left][3:] = user[d][prev_gps][3:]
                            user[d][phonestay_left][11] = temp[8]
                            # user[d][phonestay_left].extend(temp)
                            temp = user[d][phonestay_right][3:]
                            user[d][phonestay_right][3:] = user[d][prev_gps][3:]
                            user[d][phonestay_right][11] = temp[8]
                            # user[d][phonestay_right].extend(temp)
                            flag_changed = True
                        elif comb_r:
                            temp = user[d][phonestay_left][3:]
                            user[d][phonestay_left][3:] = user[d][next_gps][3:]
                            user[d][phonestay_left][11] = temp[8]
                            # user[d][phonestay_left].extend(temp)
                            temp = user[d][phonestay_right][3:]
                            user[d][phonestay_right][3:] = user[d][next_gps][3:]
                            user[d][phonestay_right][11] = temp[8]
                            # user[d][phonestay_right].extend(temp)
                            flag_changed = True
                        else:
                            user[d][phonestay_left].append("checked")
                            # those phone stay not removed have to be marked with 'checked'!
                            user[d][phonestay_right].append("checked")
                            user[d][phonestay_left][2] = "addedphonestay"
                            user[d][phonestay_right][2] = "addedphonestay"
                            flag_changed = True
                    elif found_prev_gps:  # a gps stay #right# before
                        # print('113: before phone stay, we have gps stay')
                        phone_uncernt = max(
                            [
                                int(user[d][phonestay_left][8]),
                                int(user[d][phonestay_left][5]),
                                int(user[d][phonestay_right][5]),
                            ]
                        )
                        gps_uncernt = int(user[d][prev_gps][8])
                        dist = 1000 * distance(
                            user[d][prev_gps][6],
                            user[d][prev_gps][7],
                            user[d][phonestay_left][6],
                            user[d][phonestay_left][7],
                        )
                        speed_dep = (
                            (dist - phone_uncernt - gps_uncernt)
                            / (
                                int(user[d][phonestay_left][0])
                                - int(user[d][prev_gps][0])
                            )
                            * 3.6
                        )
                        if (
                            (dist - phone_uncernt - gps_uncernt) > 0
                            and dist > 1000 * spat_constr_gps
                            and speed_dep < 200
                        ):
                            # spatially separate enough and can travel, add in gps
                            # print('1132: dist>low_acc, add phone stay')
                            # leave phone stay there
                            user[d][phonestay_left].append("checked")
                            user[d][phonestay_right].append("checked")
                            user[d][phonestay_left][2] = "addedphonestay"
                            user[d][phonestay_right][2] = "addedphonestay"
                            flag_changed = True
                        else:
                            # print('1131: low_acc > dist, merge with gps stay, meaning extend gps dur')
                            temp = user[d][phonestay_left][3:]
                            user[d][phonestay_left][3:] = user[d][prev_gps][3:]
                            user[d][phonestay_left][11] = temp[8]
                            # user[d][phonestay_left].extend(temp)
                            temp = user[d][phonestay_right][3:]
                            user[d][phonestay_right][3:] = user[d][prev_gps][3:]
                            user[d][phonestay_right][11] = temp[8]
                            # user[d][phonestay_right].extend(temp)
                            flag_changed = True
                    elif found_next_gps:  # a gps stay #right# after
                        # print('112: after phone stay, we have gps stay')
                        phone_uncernt = max(
                            [
                                int(user[d][phonestay_left][8]),
                                int(user[d][phonestay_left][5]),
                                int(user[d][phonestay_right][5]),
                            ]
                        )
                        gps_uncernt = int(user[d][next_gps][8])
                        dist = 1000 * distance(
                            user[d][next_gps][6],
                            user[d][next_gps][7],
                            user[d][phonestay_right][6],
                            user[d][phonestay_right][7],
                        )
                        speed_retn = (
                            (dist - phone_uncernt - gps_uncernt)
                            / (
                                int(user[d][next_gps][0])
                                - int(user[d][phonestay_right][0])
                            )
                            * 3.6
                        )
                        if (
                            (dist - phone_uncernt - gps_uncernt) > 0
                            and dist > 1000 * spat_constr_gps
                            and speed_retn < 200
                        ):
                            # spatially separate enough and can travel, add in gps
                            # print('1122: dist>low_acc, add phone stay')
                            # leave phone stay there, we later update duration for the gps stay
                            user[d][phonestay_left].append("checked")
                            user[d][phonestay_right].append("checked")
                            user[d][phonestay_left][2] = "addedphonestay"
                            user[d][phonestay_right][2] = "addedphonestay"
                            flag_changed = True
                        else:  # remain phone observe, but use gps location
                            # print('1121: low_acc > dist, merge with gps stay, meaning extend gps dur')
                            temp = user[d][phonestay_left][3:]
                            user[d][phonestay_left][3:] = user[d][next_gps][3:]
                            user[d][phonestay_left][11] = temp[8]
                            # user[d][phonestay_left].extend(temp)
                            temp = user[d][phonestay_right][3:]
                            user[d][phonestay_right][3:] = user[d][next_gps][3:]
                            user[d][phonestay_right][11] = temp[8]
                            # user[d][phonestay_right].extend(temp)
                            flag_changed = True
                    else:  # if don't match any case, just add it
                        # print('donot match any case, just add it (e.g., consecutive two phone stays)')
                        # leave phone stay there
                        user[d][phonestay_left].append("checked")
                        user[d][phonestay_right].append("checked")
                        user[d][phonestay_left][2] = "addedphonestay"
                        user[d][phonestay_right][2] = "addedphonestay"
                        flag_changed = True

        # user[d].extend(phone_list_check)
        for trace in phone_list_check:
            if trace[2] == "addedphonestay":
                user[d].append(trace[:])
        # remove passingby cellular traces
        i = 0
        while i < len(user[d]):
            if user[d][i][5] == 99 and float(user[d][i][9]) < dur_constr:
                del user[d][i]
            else:
                i += 1
        # remove passing traces
        ## Flag_changed = True
        ## while (Flag_changed):
        ## Flag_changed = False
        # i = 0
        # while i < len(user[d]):
        #     if int(user[d][i][5]) > spat_cell_split and int(user[d][i][9]) < dur_constr:
        #         # Flag_changed = True
        #         del user[d][i]
        #     else:
        #         i += 1
        user[d] = sorted(user[d], key=itemgetter(0))
        # update duration
        i = 0
        j = i
        while i < len(user[d]):
            if j >= len(
                user[d]
            ):  # a day ending with a stay, j goes beyond the last observation
                dur = str(int(user[d][j - 1][0]) - int(user[d][i][0]))
                for k in range(i, j, 1):
                    user[d][k][9] = dur
                break
            if (
                user[d][j][6] == user[d][i][6]
                and user[d][j][7] == user[d][i][7]
                and j < len(user[d])
            ):
                j += 1
            else:
                dur = str(int(user[d][j - 1][0]) - int(user[d][i][0]))
                for k in range(i, j, 1):
                    user[d][k][9] = dur
                i = j
        for trace in user[
            d
        ]:  # those trace with gps as -1,-1 (not clustered) should not assign a duration
            if float(trace[6]) == -1:
                trace[9] = -1
        if len(user[d]) == 1:
            user[d][0][9] = -1
        # remove and add back; because phone stays are destroyed as multiple, should be combined as one
        i = 0
        while i < len(user[d]):
            if user[d][i][2] == "addedphonestay":
                del user[d][i]
            else:
                i += 1
        # add back and sort
        for trace in phone_list_check:
            if trace[2] == "addedphonestay":
                user[d].append(trace)

        user[d] = sorted(user[d], key=itemgetter(0))

        #  remove temp marks
        user[d] = [trace[:12] for trace in user[d]]

    #  oscillation
    #  modify grid
    for day in user.keys():
        for trace in user[day]:
            if float(trace[6]) == -1:
                found_stay = False
                if found_stay == False:
                    trace[6] = trace[3] + "000"  # in case do not have enough digits
                    trace[7] = trace[4] + "000"
                    digits = (trace[6].split("."))[1]
                    digits = digits[:2] + str(int(digits[2]) / 2)
                    trace[6] = (trace[6].split("."))[0] + "." + digits
                    # trace[6] = trace[6][:5] + str(int(trace[6][5]) / 2)  # 49.950 to 49.952 220 meters
                    digits = (trace[7].split("."))[1]
                    digits = digits[:2] + str(int(digits[2:4]) / 25)
                    trace[7] = (trace[7].split("."))[0] + "." + digits
                    # trace[7] = trace[7][:7] + str(int(trace[7][7:9]) / 25)  # -122.3400 to -122.3425  180 meters

    # added to address oscillation
    user = oscillation_h1_oscill(user, dur_constr)
    ## find pong in trajactory, and replace it with ping
    ## this part is now integreted into the function itself
    ## OscillationPairList is in format: {, (ping[0], ping[1]): (pong[0], pong[1])}
    # for d in user.keys():
    #     for trace in user[d]:
    #         if (trace[6], trace[7]) in OscillationPairList:
    #             trace[6], trace[7] = OscillationPairList[(trace[6], trace[7])]

    # update duration
    user = update_duration(user, dur_constr)

    #  end addressing oscillation
    #  those newly added stays should be combined with close stays
    user = cluster_incremental(user, spat_constr_gps, dur_constr=dur_constr)
    #  update duration
    user = update_duration(user, dur_constr)
    #  use only one record for one stay
    for d in user:
        i = 0
        while i < len(user[d]) - 1:
            if (
                user[d][i + 1][6] == user[d][i][6]
                and user[d][i + 1][7] == user[d][i][7]
                and user[d][i + 1][9] == user[d][i][9]
                and int(user[d][i][9]) >= dur_constr
            ):
                del user[d][i + 1]
            else:
                i += 1
    # mark stay
    staylist = set()  # get unique staylist
    for d in user.keys():
        for trace in user[d]:
            if float(trace[9]) >= dur_constr:
                staylist.add((trace[6], trace[7]))
            else:  # change back keep full trajectory: do not use center for those are not stays
                trace[6], trace[7], trace[8], trace[9] = (
                    -1,
                    -1,
                    -1,
                    -1,
                )  # for non stay, do not give center
    staylist = list(staylist)
    for d in user.keys():
        for trace in user[d]:
            for i in range(len(staylist)):
                if trace[6] == staylist[i][0] and trace[7] == staylist[i][1]:
                    trace[10] = "stay" + str(i)
                    break

    return user


def func(args):
    (
        name,
        userGps,
        userCell,
        duration_constraint,
        spatial_constraint_gps,
        partitionThreshold,
        outputFile,
    ) = args

    user = combineGPSandPhoneStops(
        (
            userGps,
            userCell,
            duration_constraint,
            spatial_constraint_gps,
            partitionThreshold,
        )
    )

    with lock:
        f = open(outputFile, "a")
        writeCSV = csv.writer(f, delimiter=",")

        for day in sorted(user.keys()):
            for trace in user[day]:
                trace[1] = name
                trace[6], trace[7] = (
                    round(float(trace[6]), 7),
                    round(float(trace[7]), 7),
                )
                writeCSV.writerow(trace)
        f.close()


if __name__ == "__main__":
    inputFile1 = sys.argv[1]
    inputFile2 = sys.argv[2]
    outputFile = sys.argv[3]
    duration_constraint = int(sys.argv[4])
    spatial_constraint_gps = float(sys.argv[5])
    partitionThreshold = int(sys.argv[6])

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

    usernamelist = set()  # user names
    with open(inputFile1, "rU") as csvfile:
        readCSV = csv.reader(csvfile, delimiter=",")
        readCSV.next()
        for row in readCSV:
            if not len(row) == 12:
                continue
            usernamelist.add(row[1])  # get ID list; the second colume is userID
    usernamelist = list(usernamelist)

    print("total number of users to be processed: ", len(usernamelist))

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
        # gps
        UserList1 = {name: defaultdict(list) for name in namechunk}
        with open(inputFile1, "rU") as readfile:
            readCSV = csv.reader(readfile, delimiter=",")
            readCSV.next()
            for row in readCSV:
                # if not len(row) ==12 or len(row[0])==0: continue
                # if '.' not in row[3] or '.' not in row[4]: continue # debug a data issue: not '.' in lat or long
                # if(len(row[6].split('.'))>2 or len(row[7].split('.'))>2 or len(row[3].split('.'))>2 or len(row[4].split('.'))>2): continue
                # if (('-' in row[6]) and (not row[6][0]=='-')) or (('-' in row[7]) and (not row[7][0]=='-')): continue
                name = row[1]
                if name in UserList1:
                    UserList1[name][row[-1][:6]].append(row)

        # cell
        UserList2 = {name: defaultdict(list) for name in namechunk}
        with open(inputFile2, "rU") as readfile:
            readCSV = csv.reader(readfile, delimiter=",")
            readCSV.next()
            for row in readCSV:
                # if not len(row) ==12 or len(row[0])==0: continue
                # if '.' not in row[3] or '.' not in row[4]: continue # debug a data issue: not '.' in lat or long
                # if(len(row[6].split('.'))>2 or len(row[7].split('.'))>2 or len(row[3].split('.'))>2 or len(row[4].split('.'))>2): continue
                # if (('-' in row[6]) and (not row[6][0]=='-')) or (('-' in row[7]) and (not row[7][0]=='-')): continue
                name = row[1]
                if name in UserList2:
                    UserList2[name][row[-1][:6]].append(row)

        print("End reading")

        # pool
        tasks = [
            pool.apply_async(func, (task,))
            for task in [
                (
                    name,
                    UserList1[name],
                    UserList2[name],
                    duration_constraint,
                    spatial_constraint_gps,
                    partitionThreshold,
                    outputFile,
                )
                for name in UserList1
            ]
        ]

        finishit = [t.get() for t in tasks]
        """
        for name in UserList2:
            func((name, UserList1[name], UserList2[name], duration_constraint, spatial_constraint_gps, partitionThreshold, outputFile))
        """
    pool.close()
    pool.join()

    outputFile_real = outputFile.replace("_tmp.csv", ".csv")
    if os.path.isfile(outputFile_real):
        os.remove(outputFile_real)
    os.rename(outputFile, outputFile_real)
