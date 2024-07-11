# importing the modules
import datetime
import logging

import multiprocessing
from mawpy.steps.incremental_clustering import incremental_clustering
from mawpy.steps.update_stay_duration import update_stay_duration
import os

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("input_file", help="the CSV file to read the input from")
parser.add_argument("output_file", help="the CSV file to write the output to.")

logger = logging.getLogger(__name__)


def workflow1(
    input_file,
    output_file,
    spatial_constraint,
    duration_constraint1,
    duration_constraint2,
):
    df_output = incremental_clustering(input_file, output_file, spatial_constraint, duration_constraint1)
    update_stay_duration(output_file, duration_constraint2, input_df=df_output)


if __name__ == "__main__":
    args = parser.parse_args()
    multiprocessing.freeze_support() # TODO: do we require this? most probably NOT.
    st = datetime.datetime.now()
    workflow1(args.input_file, "output_file.csv", 1.0, 0, 300)
    en = datetime.datetime.now()
    logger.info(f"Total Time taken for execution: {en - st}")
    current_filename = "output_file.csv"
    new_filename = args.output_file
    os.rename(current_filename, new_filename)
