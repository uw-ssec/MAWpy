import os
from argparse import Namespace

import pandas as pd

from mawpy.constants import (USER_ID, UNIX_START_T, UNIX_START_DATE, ORIG_LAT, ORIG_LONG, ORIG_UNC, STAY_LAT, STAY_LONG,
                             STAY_UNC, STAY, STAY_DUR)
from mawpy.steps.address_oscillation import address_oscillation
from mawpy.steps.incremental_clustering import incremental_clustering
from mawpy.steps.update_stay_duration import update_stay_duration
from mawpy.workflows.ic_usd_ao_usd import ic_usd_ao_usd


def test_workflow_ic_usd_ao_usd(tmp_path):

    input_file = os.path.dirname(__file__) + '/../resources/test_input.csv'
    output_file = os.path.join(tmp_path, 'test_output_workflow_ic_usd_ao_usd.csv')
    # Prepare arguments similar to command-line arguments
    args = Namespace(
        input_file=input_file,
        output_file=output_file,
        spatial_constraint=1,
        duration_constraint_1=0,
        duration_constraint_2=300,
        duration_constraint_3=300,
        duration_constraint_4=300,
    )

    # Run the workflow to be tested and get the output
    actual_output_df = ic_usd_ao_usd(args.input_file, args.output_file, args.spatial_constraint,
                                     args.duration_constraint_1, args.duration_constraint_2,
                                     args.duration_constraint_3, args.duration_constraint_4)

    # Check if output file was created
    assert os.path.exists(output_file)

    # Destroy test output file after test completed
    os.remove(args.output_file)

    # Check if the workflow steps were called with expected arguments
    df_output_ic = incremental_clustering(output_file, args.spatial_constraint, args.duration_constraint_1,
                                          input_file=input_file)
    df_output_usd = update_stay_duration(output_file, args.duration_constraint_2, input_df=df_output_ic)
    df_output_ao = address_oscillation(output_file, args.duration_constraint_3, input_df=df_output_usd)
    expected_output_df = update_stay_duration(output_file, args.duration_constraint_4, input_df=df_output_ao)

    # Assert if the expected_output_df equal to actual_output_df
    pd.testing.assert_frame_equal(actual_output_df, expected_output_df)

    # List of columns expected in the actual_output_df
    expected_output_columns = [USER_ID, UNIX_START_T, UNIX_START_DATE,
                               ORIG_LAT, ORIG_LONG, ORIG_UNC,
                               STAY_LAT, STAY_LONG, STAY_UNC,
                               STAY_DUR, STAY]

    # Check if all the expected columns are present in the workflow output columns
    assert len(list(set(expected_output_columns) & set(actual_output_df.columns))) == len(expected_output_columns)

    # Check if output file was written successfully
    assert os.path.exists(args.output_file)
