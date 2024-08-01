from mawpy.constants import INPUT_ARGS_LIST


def check_non_negative(value):
    if value < 0:
        raise ValueError(f"{value} is not a valid non-negative numerical value")


def validate_input_args(**kwargs):
    for k, val in kwargs.items():
        if k in INPUT_ARGS_LIST:
            check_non_negative(val)
