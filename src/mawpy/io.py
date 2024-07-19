import fsspec
import os
import pandas as pd
import importlib
from typing import Callable

FUNCTION = "func"
EXTENSION = "ext"

supported_files = {
    "csv": {FUNCTION: "pandas.read_csv", EXTENSION: ".csv"},
    "excel": {FUNCTION: "pandas.read_excel", EXTENSION: ".xlsx"},
}


def _import_func(fqp: str):
    """Take a fully-qualified path and return the imported function.

    ``fqp`` is of the form "package.module.func" or
    "package.module:subobject.func".

    Warnings
    --------
    This can import arbitrary modules. Make sure you haven't installed any modules
    that may execute malicious code at import time.
    """
    if ":" in fqp:
        mod, name = fqp.rsplit(":", 1)
    else:
        mod, name = fqp.rsplit(".", 1)

    mod = importlib.import_module(mod)
    for part in name.split("."):
        mod = getattr(mod, part)

    if not isinstance(mod, Callable):
        raise TypeError(f"{fqp} is not a function")

    return mod


def _get_pandas_reader(file_path: str) -> Callable:
    file_name = os.path.basename(file_path)
    _, ext = os.path.splitext(file_name)
    try:
        format_dict = next(
            format_dict
            for format_dict in supported_files.values()
            if format_dict.get(EXTENSION) == ext
        )
        pd_class = format_dict.get(FUNCTION)
        return _import_func(pd_class)
    except StopIteration:
        supported_extensions = [
            format_dict.get(EXTENSION) for format_dict in supported_files.values()
        ]
        raise NotImplementedError(
            f"File extension '{ext}' is not supported. "
            f"Supported extensions are: {','.join(supported_extensions)}"
        )


def open_file(
    input_file: str, pandas_kwargs: dict = {}, storage_options: dict = {}
) -> pd.DataFrame:
    """
    Opens a file and reads it into a pandas DataFrame.

    Parameters
    ----------
    input_file : str
        The path to the file to open and read into a DataFrame.
    pandas_kwargs : dict, optional
        The options to pass to the pandas reader function.
        Depending on the file format, different options may be available.
        See https://pandas.pydata.org/docs/reference/io.html
    storage_options : dict, optional
        The options to pass to the fsspec file opener.
        See https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.open

    Returns
    -------
    pd.DataFrame
        The data read from the file
    """
    # Get the file opener obj and metadata
    file_obj = fsspec.open(input_file, **storage_options)

    # Get the pandas reader function
    read_func = _get_pandas_reader(file_obj.full_name)

    # Actually open the file object and pass it to the reader
    with file_obj as f:
        return read_func(f, **pandas_kwargs)
