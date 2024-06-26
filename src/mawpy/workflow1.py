# importing the modules
import datetime

import pandas as pd
import multiprocessing
from mawpy.steps.incremental_clustering import incremental_clustering
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
    incremental_clustering(input_file, output_file, spatial_constraint, duration_constraint1)
    clean_file(output_file)
    USD(output_file, output_file, duration_constraint2)
    clean_file(output_file)


if __name__ == "__main__":

    multiprocessing.freeze_support()
    workflow1("input_updated.csv", "output_file.csv", 1.0, 0, 300)
    current_filename = "output_file.csv"
    new_filename = "outputfile_workflow1.csv"
    os.rename(current_filename, new_filename)
