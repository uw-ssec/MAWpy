# importing the modules
from datetime import datetime

import pandas as pd
import multiprocessing
from UpdateStayDuration import USD
import os

from mawpy.IncrementalClustering import IC


def clean_file(file_name):
    df = pd.read_csv(file_name)
    df = df.dropna(how="all")
    df.to_csv(file_name, index=False)


def workflow1(
    input_file,
    output_file,
    spatial_constraint,
    duration_constraint1,
    duration_constraint2,
):
    st = datetime.now()
    IC(input_file, output_file, spatial_constraint, duration_constraint1)
    clean_file(output_file)
    mid = datetime.now()
    USD(output_file, output_file, duration_constraint2)
    clean_file(output_file)
    en = datetime.now()
    print(f"Time Mid: {mid - st}")
    print(f"Time End: {en - mid}")


if __name__ == "__main__":

    multiprocessing.freeze_support()
    st = datetime.now()
    workflow1("/Users/anujsinha/MAWpy/src/mawpy/input_file_old.csv", "output_file.csv", 1.0, 0, 300)
    en = datetime.now()
    print(f"Total Time taken for execution: {en - st}")
    current_filename = "output_file.csv"
    new_filename = "outputfile_workflow1.csv"
    os.rename(current_filename, new_filename)
