from mawpy.constants import SPATIAL_CONSTRAINT, DURATION_CONSTRAINT


def check_non_negative(value: float) -> None:
    if value < 0:
        raise ValueError(f"{value} is not a valid non-negative numerical value")


validation_functions_map = {
    SPATIAL_CONSTRAINT: [check_non_negative],
    DURATION_CONSTRAINT: [check_non_negative],
}


def validate_input_args(**kwargs: float) -> None:
    validated = False
    for k, val in kwargs.items():
        validation_function_list = validation_functions_map.get(k, [])
        for func in validation_function_list:
            func(val)
            validated = True
    assert validated, f"validation called with invalid keyword arguments: {kwargs}"
