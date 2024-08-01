import logging
from hypothesis import given, strategies as st

from mawpy.utilities.validations import check_non_negative

logger = logging.getLogger(__name__)


@given(st.floats(min_value=0, allow_infinity=False, allow_nan=False))
def test_valid_non_negative_values(value):
    # Hypothesis generates various non-negative floats, including edge cases like 0.0
    check_non_negative(value)
    pass


@given(st.floats(max_value=-1e-10, allow_infinity=False, allow_nan=False))
def test_invalid_negative_value(value):
    # Hypothesis generates various negative floats
    try:
        check_non_negative(value)
    except ValueError:
        pass  # Expected exception
    else:
        assert False, f"Expected Value Error for value: {value}"
