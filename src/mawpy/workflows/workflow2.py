# importing the modules
import datetime
import logging

import multiprocessing
from mawpy.steps.trace_segmentation_clustering import trace_segmentation_clustering
from mawpy.steps.update_stay_duration import update_stay_duration
import os

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("input_file", help="the CSV file to read the input from")
parser.add_argument("output_file", help="the CSV file to write the output to.")
parser.add_argument('--spatial_constraint', default=1, type=float, required=False)
parser.add_argument('--duration_constraint_1', default=0, type=float, required=False)
parser.add_argument('--duration_constraint_2', default=300, type=float, required=False)

logger = logging.getLogger(__name__)


def workflow2(
    input_file: str,
    output_file: str,
    spatial_constraint: float,
    duration_constraint1: float,
    duration_constraint2: float,
):
    df_output = trace_segmentation_clustering(output_file, spatial_constraint, duration_constraint1,
                                              input_file=input_file)
    update_stay_duration(output_file, duration_constraint2, input_df=df_output)


if __name__ == "__main__":
    args = parser.parse_args()
    multiprocessing.freeze_support() # TODO: do we require this? most probably NOT.
    st = datetime.datetime.now()
    workflow2(args.input_file, "output_file.csv", args.spatial_constraint,
              args.duration_constraint_1, args.duration_constraint_2)
    en = datetime.datetime.now()
    logger.info(f"Total Time taken for execution: {en - st}")
    current_filename = "output_file.csv"
    new_filename = args.output_file
    os.rename(current_filename, new_filename)
