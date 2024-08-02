import sys
import csv
import os


def writeFile(user, name, output):
    f = open(output, "a")
    writeCSV = csv.writer(f, delimiter=",")

    userinfo = None
    if len(user) == 1 and list(user)[0] in ["userGps", "userCell"]:
        for k in user.keys():
            word = k
        userinfo = user[word]

    if userinfo:
        user = userinfo

    for day in sorted(user.keys()):
        if len(user[day]):
            for trace in user[day]:
                trace[1] = name
                trace[6], trace[7] = (
                    round(float(trace[6]), 7),
                    round(float(trace[7]), 7),
                )

                writeCSV.writerow(trace)
    f.close()


if __name__ == "__main__":
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]

    outputFile = outputFile.replace(".csv", "_tmp.csv")

    outputFile_fail = outputFile.replace(".csv", "_fail.csv")

    f1 = open(outputFile, "w")
    f1.write(
        "unix_start_t,user_ID,mark_1,orig_lat,orig_long,orig_unc,stay_lat,stay_long,stay_unc,stay_dur,stay_ind,human_start_t\n"
    )
    f1.close()

    f1 = open(outputFile, "a")
    writeCSV1 = csv.writer(f1, delimiter=",")

    f2 = open(outputFile_fail, "w")
    f2.write(
        "unix_start_t,user_ID,mark_1,orig_lat,orig_long,orig_unc,stay_lat,stay_long,stay_unc,stay_dur,stay_ind,human_start_t\n"
    )
    f2.close()

    f2 = open(outputFile_fail, "a")
    writeCSV2 = csv.writer(f2, delimiter=",")

    with open(inputFile, "rU") as csvfile:
        readCSV = csv.reader(csvfile, delimiter=",")
        readCSV.next()
        for row in readCSV:
            if not len(row) == 12 or len(row[0]) != 10 or len(row[11]) != 12:
                writeCSV2.writerow(row)
                print(row)
            else:
                writeCSV1.writerow(row)

    f1.close()
    f2.close()

    outputFile_real = outputFile.replace("_tmp.csv", ".csv")
    if os.path.isfile(outputFile_real):
        os.remove(outputFile_real)
    os.rename(outputFile, outputFile_real)
