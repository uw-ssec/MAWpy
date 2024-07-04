# importing the modules
import datetime
import logging

import multiprocessing
from mawpy.steps.incremental_clustering import incremental_clustering
import os

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("input_file", help="the CSV file to read the input from")
parser.add_argument("output_file", help="the output CSV file.")

logger = logging.getLogger(__name__)

def workflow1(
    input_file,
    output_file,
    spatial_constraint,
    duration_constraint1,
    duration_constraint2,
):
    incremental_clustering(input_file, output_file, spatial_constraint, duration_constraint1)


if __name__ == "__main__":
    args = parser.parse_args()
    multiprocessing.freeze_support()
    st = datetime.datetime.now()
    workflow1(args.input_file, "output_file.csv", 1.0, 0, 300)
    en = datetime.datetime.now()
    logger.info(f"Total Time taken for execution: {en - st}")
    current_filename = "output_file.csv"
    new_filename = args.output_file
    os.rename(current_filename, new_filename)
