# importing the modules
import datetime
import logging


import pandas as pd

from mawpy.constants import TSC_USD_WIP_FILE_NAME
from mawpy.steps import (
    trace_segmentation_clustering,
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

logger = logging.getLogger(__name__)


def tsc_usd(
    input_file: str,
    output_file: str,
    spatial_constraint: float,
    duration_constraint1: float,
    duration_constraint2: float,
) -> pd.DataFrame:
    df_output_tsc = trace_segmentation_clustering(output_file, spatial_constraint, duration_constraint1,
                                              input_file=input_file)
    df_output_final = update_stay_duration(output_file, duration_constraint2, input_df=df_output_tsc)
    return df_output_final


def main():
    """
    Perform trace segmentation clustering and update stay duration on user data.

    This script processes trace data using the following steps:
    1. **Trace Segmentation Clustering**: Segments traces and clusters them based on a spatial constraint and duration threshold.
    2. **Update Stay Duration**: Recalculates the duration of detected stays.

    The processed data is saved to a specified output file.

    Parameters
    ----------
    input_file : str
        Path to the input CSV file containing the trace data.
    output_file : str
        Path to the output CSV file where the processed data will be saved.
    spatial_constraint : float
        The spatial threshold used for trace segmentation clustering. Default is 1.0.
    duration_constraint_1 : float
        The duration threshold used for trace segmentation clustering. Default is 0.
    duration_constraint_2 : float
        The minimum duration constraint for the final stay duration update. Default is 300.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the processed traces data with updated stay durations.

    Notes
    -----
    The script can be executed from the command line with the required arguments.

    Example
    -------
    To run the script with custom parameters: (Make sure your python executable points to the right working directory)

    ```bash
    python3 tsc_usd.py <input csv file path> <output file path> --spatial_constraint=1 --duration_constraint_1=0 --duration_constraint_2=300
    ```

    """
    args = parser.parse_args()
    st = datetime.datetime.now()
    tsc_usd(args.input_file, TSC_USD_WIP_FILE_NAME, args.spatial_constraint,
              args.duration_constraint_1, args.duration_constraint_2)
    en = datetime.datetime.now()
    logger.info(f"Total Time taken for execution: {en - st}")
    os.rename(TSC_USD_WIP_FILE_NAME, args.output_file)


if __name__ == "__main__":
    main()
