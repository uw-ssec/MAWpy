import logging

import pytest
from hypothesis import given, strategies as st

from mawpy.utilities.validations import check_non_negative, validate_input_args

logger = logging.getLogger(__name__)


@given(st.floats(min_value=0, allow_infinity=False, allow_nan=False))
def test_valid_non_negative_values(value):
    # Hypothesis generates various non-negative floats, including edge cases like 0.0
    check_non_negative(value)


@given(st.floats(max_value=-1e-10, allow_infinity=False, allow_nan=False))
def test_invalid_negative_value(value):
    # Hypothesis generates various negative floats
    with pytest.raises(ValueError, match="is not a valid non-negative numerical value"):
        check_non_negative(value)


@given(st.floats(min_value=1e-10, allow_infinity=False, allow_nan=False),
       st.floats(min_value=1e-10, allow_infinity=False, allow_nan=False))
def test_validate_input_args(duration_constraint, spatial_constraint):
    # No Exception as Hypothesis generates positive floats, and the arguments are successfully validated.
    validate_input_args(duration_constraint=duration_constraint, spatial_constraint=spatial_constraint)


@given(st.floats(max_value=-1e-10))
def test_validate_input_args_for_negative_arg_not_in_validation_list(abc):
    # Exception as Hypothesis generates negative floats, but the argument passed is not in the validation list.
    with pytest.raises(AssertionError, match="validation called with invalid keyword arguments"):
        validate_input_args(abc=abc)


@given(st.floats(max_value=-1e-10, allow_infinity=False, allow_nan=False))
def test_validate_input_args_negative_value(negative_float_value):
    # Raises Value Error as the passed argument is in the validation list for negative value check.
    with pytest.raises(ValueError, match="is not a valid non-negative numerical value"):
        check_non_negative(negative_float_value)
