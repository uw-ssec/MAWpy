# importing the modules
import datetime
import logging


import pandas as pd

from mawpy.constants import TSC_IC_USD_WIP_FILE_NAME
from mawpy.steps import (
    trace_segmentation_clustering,
    incremental_clustering,
    update_stay_duration
)
import os

import argparse


parser = argparse.ArgumentParser()
parser.add_argument("input_file", help="the CSV file to read the input from")
parser.add_argument("output_file", help="the CSV file to write the output to.")
parser.add_argument('--spatial_constraint_1', default=1.0, type=float, required=False)
parser.add_argument('--spatial_constraint_2', default=1.0, type=float, required=False)
parser.add_argument('--duration_constraint_1', default=300, type=float, required=False)
parser.add_argument('--duration_constraint_2', default=0, type=float, required=False)
parser.add_argument('--duration_constraint_3', default=300, type=float, required=False)

logger = logging.getLogger(__name__)


def tsc_ic_usd(
    input_file: str,
    output_file: str,
    spatial_constraint1: float,
    spatial_constraint2: float,
    duration_constraint1: float,
    duration_constraint2: float,
    duration_constraint3: float
) -> pd.DataFrame:
    """
    Perform trace segmentation clustering, incremental clustering, and update stay duration on user data.

    This script processes trace data using a series of steps:
    1. **Trace Segmentation Clustering**: Segments traces and clusters them based on spatial and duration constraints.
    2. **Incremental Clustering**: Groups locations using a spatial threshold to detect potential stays.
    3. **Update Stay Duration**: Recalculates the duration of the detected stays.

    The processed data is then saved to a specified output file.

    Parameters
    ----------
    input_file : str
        Path to the input CSV file containing the trace data.
    output_file : str
        Path to the output CSV file where the processed data will be saved.
    spatial_constraint_1 : float
        The spatial threshold used for trace segmentation clustering. Default is 1.0.
    spatial_constraint_2 : float
        The spatial threshold used for incremental clustering. Default is 1.0.
    duration_constraint_1 : float
        The duration threshold used for trace segmentation clustering. Default is 300.
    duration_constraint_2 : float
        The minimum duration constraint for incremental clustering. Default is 0.
    duration_constraint_3 : float
        The minimum duration constraint for the final stay duration update. Default is 300.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the processed trace data with updated stay durations.

    Notes
    -----
    The script can be executed from the command line with the required arguments.

    Example
    -------
    To run the script with custom parameters: (Make sure your python executable points to the right working directory)

    ```bash
    python3 tsc_ic_usd.py <input csv file path> <output file path> --spatial_constraint_1=1 --spatial_constraint_2=1 --duration_constraint_1=600 --duration_constraint_2=300 --duration_constraint_3=600
    ```

    """
    df_output_tsc = trace_segmentation_clustering(output_file, spatial_constraint1, duration_constraint1,
                                                  input_file=input_file)
    df_output_ic = incremental_clustering(output_file, spatial_constraint2, duration_constraint2, input_df=df_output_tsc)
    df_output_final = update_stay_duration(output_file, duration_constraint3, input_df=df_output_ic)
    return df_output_final


def main():
    args = parser.parse_args()
    st = datetime.datetime.now()
    tsc_ic_usd(args.input_file, TSC_IC_USD_WIP_FILE_NAME, args.spatial_constraint_1, args.spatial_constraint_2,
               args.duration_constraint_1, args.duration_constraint_2, args.duration_constraint_3)
    en = datetime.datetime.now()
    logger.info(f"Total Time taken for execution: {en - st}")
    os.rename(TSC_IC_USD_WIP_FILE_NAME, args.output_file)


if __name__ == "__main__":
    main()
