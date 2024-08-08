# importing the modules
import datetime
import logging


import pandas as pd

from mawpy.constants import IC_USD_WIP_FILE_NAME
from mawpy.steps import (
    incremental_clustering,
    update_stay_duration
)
import os

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("input_file", help="the CSV file to read the input from")
parser.add_argument("output_file", help="the CSV file to write the output to.")
parser.add_argument('--spatial_constraint', default=1.0, type=float, required=False)
parser.add_argument('--duration_constraint_1',
                    help="This should be set to 0 if using Incremental Clustering as the first step in the workflow",
                    default=0, type=float, required=False)
parser.add_argument('--duration_constraint_2', default=300, type=float, required=False)

logger = logging.getLogger(__name__)


def ic_usd(
    input_file: str,
    output_file: str,
    spatial_constraint: float,
    duration_constraint1: float,
    duration_constraint2: float,
) -> pd.DataFrame:
    df_output_ic = incremental_clustering(output_file, spatial_constraint, duration_constraint1, input_file=input_file)
    df_output_final = update_stay_duration(output_file, duration_constraint2, input_df=df_output_ic)
    return df_output_final


def main():
    """
    Perform incremental clustering and update stay duration on user traces data.

    This script processes trace data by applying two main steps:
    1. **Incremental Clustering**: Groups locations based on a spatial threshold to detect potential stays.
    2. **Update Stay Duration**: Recalculates the duration of the detected stays.

    The processed data is then saved to a specified output file.

    Parameters
    ----------
    input_file : str
        Path to the input CSV file containing the trace data.
    output_file : str
        Path to the output CSV file where the processed data will be saved.
    spatial_constraint : float
        The spatial threshold used for clustering locations to detect stays. Default is 1.0.
    duration_constraint1 : float
        The minimum duration required for the first stay detection step. **This should be passed as 0 only**
        Should be set to 0 if using Incremental Clustering as the first step in the workflow. Default is 0.
    duration_constraint2 : float
        The minimum duration constraint for updating stay duration. Default is 300.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the processed data with updated stay durations.

    Notes
    -----
    The script can be executed from the command line with the required arguments.

    Example
    -------
    To run the script with custom parameters: (Make sure your python executable points to the right working directory)

    ```bash
    python3 ic_usd.py <input csv file path> <output file path> --spatial_constraint=1 --duration_constraint_1=0 --duration_constraint_2=300
    ```

    """
    args = parser.parse_args()
    st = datetime.datetime.now()
    ic_usd(args.input_file, IC_USD_WIP_FILE_NAME, args.spatial_constraint,
           args.duration_constraint_1, args.duration_constraint_2)
    en = datetime.datetime.now()
    logger.info(f"Total Time taken for execution: {en - st}")
    os.rename(IC_USD_WIP_FILE_NAME, args.output_file)


if __name__ == "__main__":
    main()
