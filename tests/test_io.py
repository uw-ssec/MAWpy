import os

import pandas as pd
from pandas.testing import assert_frame_equal
from hypothesis.extra.pandas import column, data_frames, range_indexes
from hypothesis import given, settings
import hypothesis
import hypothesis.strategies as st
import pytest
from pathlib import Path

from mawpy.constants import UNIX_START_T, USER_ID, ORIG_LAT, ORIG_LONG, ORIG_UNC
from mawpy.io import open_file

# Setup hypothesis strategy for the DataFrame
df_strategy = data_frames(
    index=range_indexes(min_size=5, max_size=10),
    columns=[
        column(UNIX_START_T, dtype=int),
        column(USER_ID, dtype=str),
        column(ORIG_LAT, dtype=float),
        column(ORIG_LONG, dtype=float),
        column(ORIG_UNC, dtype=int)
    ],
    rows=st.tuples(
        st.integers(min_value=1549499431, max_value=1719367258),
        st.uuids(),
        st.floats(min_value=46.7326586, max_value=48.2975503),
        st.floats(min_value=-123.0193554, max_value=-121.0816193),
        st.integers(min_value=101, max_value=2147480)
    )
)

@given(df=df_strategy)
@settings(max_examples=1, deadline=None)
@pytest.mark.parametrize("ext", [".csv",  ".xlsx"])
def test_open_file(df, ext, tmp_path_factory):
    # Setup tempdir for file
    d = tmp_path_factory.mktemp("mawpy-io-test")

    ext_dict = {
        ".csv": "to_csv",
        ".xlsx": "to_excel"
    }
    filename = str((d / f".test{ext}").resolve())

    # Convert UUID to simple string
    df.loc[:, USER_ID] = df[USER_ID].astype(str)

    # Save the DataFrame to a temp file
    to_file = getattr(df, ext_dict[ext])
    to_file(filename, index=False)

    # Open the file and read it into a DataFrame
    # using the open_file function to be tested
    file_df = open_file(filename)

    # Check that the file_df is a DataFrame
    assert isinstance(file_df, pd.DataFrame)

    # Check that the file_df is equal to the original DataFrame
    assert_frame_equal(df, file_df, check_dtype=False)
