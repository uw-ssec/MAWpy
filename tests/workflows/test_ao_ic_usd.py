import os
from argparse import Namespace

import pandas as pd

from mawpy.constants import (USER_ID, UNIX_START_T, UNIX_START_DATE, ORIG_LAT, ORIG_LONG, ORIG_UNC, STAY_LAT, STAY_LONG,
                             STAY_UNC, STAY, STAY_DUR)
from mawpy.workflows.ao_ic_usd import ao_ic_usd


def test_workflow_ao_ic_usd(tmp_path):

    input_file = os.path.dirname(__file__) + '/../resources/test_input.csv'
    output_file = os.path.join(tmp_path, 'test_output_workflow_ao_ic_usd.csv')
    # Prepare arguments similar to command-line arguments
    args = Namespace(
        input_file=input_file,
        output_file=output_file,
        spatial_constraint=1,
        duration_constraint_1=0,
        duration_constraint_2=300,
        duration_constraint_3=300
    )

    # Run the workflow to be tested and get the output
    actual_output_df = ao_ic_usd(args.input_file, args.output_file, args.spatial_constraint,
                                 args.duration_constraint_1, args.duration_constraint_2, args.duration_constraint_3)
    expected_output_df = pd.read_csv(os.path.dirname(__file__)
                                               + '/../resources/ao_ic_usd_output_for_test_input.csv')
    # Check if output file was created
    assert os.path.exists(output_file)

    pd.testing.assert_frame_equal(actual_output_df, expected_output_df, check_like=True)

    # List of columns expected in the actual_output_df
    expected_output_columns = [USER_ID, UNIX_START_T, UNIX_START_DATE,
                               ORIG_LAT, ORIG_LONG, ORIG_UNC,
                               STAY_LAT, STAY_LONG, STAY_UNC,
                               STAY_DUR, STAY]

    # Check if all the expected columns are present in the workflow output columns
    assert len(list(set(expected_output_columns) & set(actual_output_df.columns))) == len(expected_output_columns)

    # Check if output file was written successfully
    assert os.path.exists(args.output_file)
