import pytest
import pandas as pd
import numpy as np
from mawpy.constants import STAY_LAT, STAY_LONG, STAY_LAT_LONG, STAY
from mawpy.distance import distance
from mawpy.utilities.common import _mean_ignore_minus_ones, _merge_stays, get_combined_stay, get_stay_groups

def test_mean_ignore_minus_ones():

    # series with -1 and positive numbers
    series = pd.Series([1, 2, -1, 3, -1, 4, 5, -1])
    result = _mean_ignore_minus_ones(series)
    assert isinstance(result, float)
    assert result == 3

    # series with only -1
    series = pd.Series([-1, -1, -1])
    assert _mean_ignore_minus_ones(series) == -1

    # series with positive numbers
    series = pd.Series([1, 3, 5, 7, 9])
    assert _mean_ignore_minus_ones(series) == 5

    # list instead of pandas series
    with pytest.raises(TypeError):
        _mean_ignore_minus_ones([1, 2, -1, 3, -1, 4, 5, -1])

def test_merge_stays():

    df_by_user = pd.DataFrame({
        STAY: [1, 1, 2, 2, 3],
        STAY_LAT: [10, 20, -1, 30, 40],
        STAY_LONG: [15, -1, 25, -1, 35]
    })
    group_avgs = pd.DataFrame({
        STAY: [1, 2, 3],
        STAY_LAT: [15, 30, 40],
        STAY_LONG: [10, 25, 35]
    })

    # Test 1: Merges stay ID 1 into stay ID 2
    result_df_by_user = _merge_stays(1, 2, df_by_user.copy(), group_avgs.copy(), 2) # average is recalculated after the merge
    expected_df_by_user = pd.DataFrame({
        STAY: [2, 2, 2, 2, 3],
        STAY_LAT: [20, 20, 20, 20, 40],
        STAY_LONG: [20, 20, 20, 20, 35]
    })
    assert isinstance(result_df_by_user, pd.DataFrame)
    pd.testing.assert_frame_equal(result_df_by_user, expected_df_by_user, rtol=1e-5)

    # Test 2: Merges stay ID 2 into stay ID 3
    result_df_by_user = _merge_stays(2, 3, df_by_user.copy(), group_avgs.copy(), 1) # average is recalculated after the merge
    expected_df_by_user = pd.DataFrame({
        STAY: [1, 1, 3, 3, 3],
        STAY_LAT: [10, 20, 35, 35, 35],
        STAY_LONG: [15, -1, 30, 30, 30]
    })
    pd.testing.assert_frame_equal(result_df_by_user, expected_df_by_user, rtol=1e-5)

    # Test 3: No change - Merge stay ID 3 with itself
    result_df_by_user = _merge_stays(3, 3, df_by_user.copy(), group_avgs.copy(), 2) # no change when merging a stay with itself
    expected_df_by_user = df_by_user.copy()
    pd.testing.assert_frame_equal(result_df_by_user, expected_df_by_user, rtol=1e-5)


def test_get_combined_stay():

    # Test 1: No merges
    df_by_user = pd.DataFrame({
        STAY: [1, 2, 3],
        STAY_LAT: [10, 20, 30],
        STAY_LONG: [15, 25, 35]
    })
    result_df = get_combined_stay(df_by_user, threshold=20)
    expected_df = df_by_user.copy()
    pd.testing.assert_frame_equal(result_df, expected_df, rtol=1e-5)

    # Test 2: Some merges
    df_by_user = pd.DataFrame({
        STAY: [1, 2, 3, 4, 5, 6],
        STAY_LAT: [10, 20, 30, -1, 50, 60],
        STAY_LONG: [11, 21, 31, -1, 51, 61]
    })
    expected_df = pd.DataFrame({
        STAY: [1, 1, 1, 4, 5, 5],
        STAY_LAT: [20, 20, 20, -1, 55, 55],
        STAY_LONG: [21, 21, 21, -1, 56, 56]
    })
    result_df = get_combined_stay(df_by_user, threshold=2500)
    pd.testing.assert_frame_equal(result_df, expected_df, rtol=1e-5)

    # Test 3: Many more merges
    df_by_user = pd.DataFrame({
        STAY: [1, 2, 3, 4, 5],
        STAY_LAT: [10, 20, -1, -1, 50],
        STAY_LONG: [15, 25, -1, -1, 55]
    })
    expected_df = pd.DataFrame({
        STAY: [1, 1, 1, 1, 5],
        STAY_LAT: [15, 15, 15, 15, 50],
        STAY_LONG: [20, 20, 20, 20, 55]
    })
    result_df = get_combined_stay(df_by_user, threshold=4500)
    pd.testing.assert_frame_equal(result_df, expected_df, rtol=1e-5)

    # Test 4: No iterations, total_groups=1
    df_by_user = pd.DataFrame({
        STAY: [1],
        STAY_LAT: [10],
        STAY_LONG: [15]
    })
    expected_df = df_by_user.copy()
    result_df = get_combined_stay(df_by_user, threshold=10)
    pd.testing.assert_frame_equal(result_df, expected_df, rtol=1e-5)

def test_get_stay_groups():

    # Test1: Same consecutive values
    df_with_stay_added = pd.DataFrame({
        STAY_LAT: [1, 1, 2, 2, 3, 3],
        STAY_LONG: [10, 10, 20, 20, 30, 30]
    })
    expected_groups = np.array([0, 0, 1, 1, 2, 2])
    result_groups = get_stay_groups(df_with_stay_added)
    np.testing.assert_array_equal(result_groups, expected_groups)

    # Test2: Same/Different values
    df_with_stay_added = pd.DataFrame({
        STAY_LAT: [1, 1, 2, 2, 2, 3, 3, 3],
        STAY_LONG: [10, 10, 20, 20, 25, 30, 30, 35]
    })
    expected_groups = np.array([0, 0, 1, 1, 2, 3, 3, 4])
    result_groups = get_stay_groups(df_with_stay_added)
    np.testing.assert_array_equal(result_groups, expected_groups)

    # Test3: All different values
    df_with_stay_added = pd.DataFrame({
        STAY_LAT: [1, 2, 3, 4],
        STAY_LONG: [10, 20, 30, 40]
    })
    expected_groups = np.array([0, 1, 2, 3])
    result_groups = get_stay_groups(df_with_stay_added)
    np.testing.assert_array_equal(result_groups, expected_groups)

    # Test4:  Same values
    df_with_stay_added = pd.DataFrame({
        STAY_LAT: [1, 1, 1, 1],
        STAY_LONG: [10, 10, 10, 10]
    })
    expected_groups = np.array([0, 0, 0, 0])
    result_groups = get_stay_groups(df_with_stay_added)
    np.testing.assert_array_equal(result_groups, expected_groups)

    # Test5: Empty Dataframe
    df_with_stay_added = pd.DataFrame(columns=[STAY_LAT, STAY_LONG])
    expected_groups = np.array([])
    result_groups = get_stay_groups(df_with_stay_added)
    np.testing.assert_array_equal(result_groups, expected_groups)
