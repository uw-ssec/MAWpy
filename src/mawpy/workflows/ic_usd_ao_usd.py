# importing the modules
import datetime
import logging


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


def main():
    """
    Perform a series of geospatial operations on user data, including clustering, stay duration updates,
    and oscillation correction.

    This script processes  trace data to detect and refine potential stays, applying a series of steps:
    1. **Incremental Clustering**: Groups locations based on a spatial threshold.
    2. **Update Stay Duration**: Recalculates the duration of detected stays.
    3. **Address Oscillation**: Corrects oscillating traces that may indicate repeated movement between two points.
    4. **Final Update Stay Duration**: Applies a final duration update after oscillation correction.

    The results are saved to a specified output file.

    Parameters
    ----------
    input_file : str
        Path to the input CSV file containing the trace data.
    output_file : str
        Path to the output CSV file where the processed data will be saved.
    spatial_constraint : float
        The spatial threshold used for clustering locations to detect stays. Default is 1.0.
    duration_constraint1 : float
        The minimum duration required for the first stay detection step. Default is 0.
    duration_constraint2 : float
        The minimum duration constraint for the first stay duration update. Default is 300.
    duration_constraint3 : float
        The minimum duration constraint for addressing oscillations. Default is 300.
    duration_constraint4 : float
        The minimum duration constraint for the final stay duration update. Default is 300.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the processed data, including refined stay durations and corrected oscillations.

    Notes
    -----
    The script can be executed from the command line with the required arguments.

    Example
    -------
    To run the script with custom parameters: (Make sure your python executable points to the right working directory)

    ```bash
    python3 ic_usd_ao_usd.py <input csv file path> <output file path> --spatial_constraint=1 --duration_constraint_1=0
    ```

    """
    args = parser.parse_args()
    st = datetime.datetime.now()
    ic_usd_ao_usd(args.input_file, IC_USD_AO_USD_WIP_FILE_NAME, args.spatial_constraint, args.duration_constraint_1,
                  args.duration_constraint_2, args.duration_constraint_3, args.duration_constraint_4)
    en = datetime.datetime.now()
    logger.info(f"Total Time taken for execution: {en - st}")
    os.rename(IC_USD_AO_USD_WIP_FILE_NAME, args.output_file)


if __name__ == "__main__":
    main()
