import pandas as pd
import numpy as np
from hypothesis import given, settings, strategies as st
from hypothesis.extra.pandas import column, data_frames, range_indexes
from mawpy.constants import STAY_LAT, STAY_LONG, STAY
from mawpy.utilities.common import _mean_ignore_minus_ones, _merge_stays, get_combined_stay, get_stay_groups

# Testing _mean_ignore_minus_one()
series_strategy = st.lists(st.one_of(st.integers(min_value=-1, max_value=100), st.just(-1)),
                            min_size=1,
                            max_size=100).map(pd.Series)
@given(series=series_strategy)
def test_mean_ignore_minus_ones(series):
    result = _mean_ignore_minus_ones(series)
    if all(series == -1):
        assert result == -1
    else:
        assert result == series[series != -1].mean()

# Testing _merge_stays()
df_columns = {
    STAY: {"elements": st.integers(min_value=0, max_value=10)},
    STAY_LAT: {"elements": st.floats(min_value=-90, max_value=90)},
    STAY_LONG: {"elements": st.floats(min_value=-180, max_value=180)}
}
df_strategy = data_frames(
    index=range_indexes(min_size=5, max_size=10),
    columns=[column(key, **value) for key, value in df_columns.items()]
)
@given(df_by_user=df_strategy, group_avgs=df_strategy, stay_to_update=st.integers(min_value=0, max_value=10),
       updated_stay=st.integers(min_value=0, max_value=10), group_avgs_index_to_update=st.integers(min_value=0, max_value=10))
@settings(max_examples=100, deadline=None)
def test_merge_stays(df_by_user, group_avgs, stay_to_update, updated_stay, group_avgs_index_to_update):

    result_df = _merge_stays(stay_to_update, updated_stay, df_by_user, group_avgs, group_avgs_index_to_update)

    df_by_user_expected = df_by_user.copy()
    group_avgs_expected = group_avgs.copy()

    # Exclude -1 and compute the mean for latitite and longitude series
    merged_values = df_by_user[df_by_user[STAY] == updated_stay][[STAY_LAT, STAY_LONG]]
    means = {}
    for column in merged_values.columns:
        column_data = merged_values[column]
        if (column_data == -1).all():
            means[column] = -1
        else:
            means[column] = column_data[column_data != -1].mean()
    new_avg = pd.Series(means)

    # Update expected DataFrames with new average location data
    df_by_user_expected.loc[df_by_user_expected[STAY] == updated_stay, [STAY_LAT, STAY_LONG]] = new_avg.values
    group_avgs_expected.loc[group_avgs_expected[STAY] == updated_stay, [STAY_LAT, STAY_LONG]] = new_avg.values
    group_avgs_expected.loc[group_avgs_index_to_update, STAY] = updated_stay
    group_avgs_expected.loc[group_avgs_index_to_update, [STAY_LAT, STAY_LONG]] = new_avg.values

    pd.testing.assert_frame_equal(result_df, df_by_user_expected, check_dtype=False, rtol=1e-05)

# Test3: Test get_combined_stay
@given(df_by_user=df_strategy, threshold=st.floats(min_value=0.0, max_value=4000.0))
@settings(max_examples=100, deadline=None)
def test_get_combined_stay(df_by_user, threshold):
    result = get_combined_stay(df_by_user, threshold)

    # Stays same; no merges
    if threshold == 0.0:
        pd.testing.assert_frame_equal(result, df_by_user)

    assert isinstance(result, pd.DataFrame)

# Test4: Test get_stay_groups
@given(df_with_stay_added=df_strategy)
@settings(max_examples=100, deadline=None)
def test_get_stay_groups(df_with_stay_added):

    # Single entry
    if len(df_with_stay_added) == 1:
        expected = np.array([0])
        result = get_stay_groups(df_with_stay_added)
        np.testing.assert_array_equal(result, expected)

    # All same latitudes and longitudes
    elif (df_with_stay_added[STAY_LAT].nunique() == 1) and (df_with_stay_added[STAY_LONG].nunique() == 1):
        expected = np.zeros(len(df_with_stay_added), dtype=int)
        result = get_stay_groups(df_with_stay_added)
        np.testing.assert_array_equal(result, expected)

    # Different locations
    elif len(df_with_stay_added) > 1:
        expected = np.zeros(len(df_with_stay_added), dtype=int)
        for i in range(1, len(df_with_stay_added)):
            if (df_with_stay_added[STAY_LAT].iloc[i] != df_with_stay_added[STAY_LAT].iloc[i - 1]) or (df_with_stay_added[STAY_LONG].iloc[i] != df_with_stay_added[STAY_LONG].iloc[i - 1]):
                expected[i] = expected[i - 1] + 1
            else:
                expected[i] = expected[i - 1]
        result = get_stay_groups(df_with_stay_added)
        np.testing.assert_array_equal(result, expected)
