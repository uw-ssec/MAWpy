import argparse
import logging

logger = logging.getLogger(__name__)


def check_non_negative(value):
    try:
        f_value = float(value)
    except Exception as ex:
        logger.error(ex)
        raise argparse.ArgumentTypeError(f"{value} is not a valid non-negative numerical value")
    if f_value < 0:
        raise argparse.ArgumentTypeError(f"{value} is not a valid non-negative numerical value")
    return f_value
