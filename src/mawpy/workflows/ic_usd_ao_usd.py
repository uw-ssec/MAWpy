# importing the modules
import datetime
import logging

import multiprocessing

import pandas as pd

from mawpy.constants import IC_USD_AO_USD_WIP_FILE_NAME
from mawpy.steps import (
    address_oscillation,
    incremental_clustering,
    update_stay_duration
)
import os

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("input_file", help="the CSV file to read the input from")
parser.add_argument("output_file", help="the CSV file to write the output to.")
parser.add_argument('--spatial_constraint', default=1.0, type=float, required=False)
parser.add_argument('--duration_constraint_1', default=0, type=float, required=False)
parser.add_argument('--duration_constraint_2', default=300, type=float, required=False)
parser.add_argument('--duration_constraint_3', default=300, type=float, required=False)
parser.add_argument('--duration_constraint_4', default=300, type=float, required=False)

logger = logging.getLogger(__name__)


def ic_usd_ao_usd(
    input_file: str,
    output_file: str,
    spatial_constraint: float,
    duration_constraint1: float,
    duration_constraint2: float,
    duration_constraint3: float,
    duration_constraint4: float,
) -> pd.DataFrame:
    df_output_ic = incremental_clustering(output_file, spatial_constraint, duration_constraint1, input_file=input_file)
    df_output_usd = update_stay_duration(output_file, duration_constraint2, input_df=df_output_ic)
    df_output_ao = address_oscillation(output_file, duration_constraint3, input_df=df_output_usd)
    df_output_usd_final = update_stay_duration(output_file, duration_constraint4, input_df=df_output_ao)
    return df_output_usd_final


if __name__ == "__main__":
    args = parser.parse_args()
    multiprocessing.freeze_support() # TODO: do we require this? most probably NOT.
    st = datetime.datetime.now()
    ic_usd_ao_usd(args.input_file, IC_USD_AO_USD_WIP_FILE_NAME, args.spatial_constraint, args.duration_constraint_1,
                  args.duration_constraint_2, args.duration_constraint_3, args.duration_constraint_4)
    en = datetime.datetime.now()
    logger.info(f"Total Time taken for execution: {en - st}")
    os.rename(IC_USD_AO_USD_WIP_FILE_NAME, args.output_file)
