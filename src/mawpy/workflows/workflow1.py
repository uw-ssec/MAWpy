# importing the modules
import datetime

import multiprocessing
from mawpy.steps.incremental_clustering import incremental_clustering
import os


def workflow1(
    input_file,
    output_file,
    spatial_constraint,
    duration_constraint1,
    duration_constraint2,
):
    incremental_clustering(input_file, output_file, spatial_constraint, duration_constraint1)


if __name__ == "__main__":

    multiprocessing.freeze_support()
    st = datetime.datetime.now()
    workflow1("/Users/anujsinha/MAWpy/src/mawpy/input_file_old.csv", "output_file.csv", 1.0, 0, 300)
    en = datetime.datetime.now()
    print(en - st)
    current_filename = "output_file.csv"
    new_filename = "outputfile_workflow1.csv"
    os.rename(current_filename, new_filename)
