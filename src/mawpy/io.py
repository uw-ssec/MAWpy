import fsspec
import os
import pandas as pd
from typing import Callable

supported_files = {
    ".csv": pd.read_csv,
    ".xlsx": pd.read_excel,
    ".xls": pd.read_excel,
}


def _get_pandas_reader(file_path: str) -> Callable:
    """
    Takes an input file path and returns
    the appropriate pandas reader function from
    the ``supported_files`` dictionary

    Parameters
    ----------
    file_path : str
        The path file string

    Returns
    -------
    Callable
        The pandas reader function

    Raises
    ------
    NotImplementedError
        If the file extension is not supported
    """
    # Get the file base name, e.g. ``file.csv``
    # and extract the extension, e.g. ``.csv``
    file_name = os.path.basename(file_path)
    _, ext = os.path.splitext(file_name)

    # The read function from the supported_files dictionary
    read_func = supported_files.get(ext, None)

    if read_func is None:
        # If the extension is not supported, raise an error
        raise NotImplementedError(
            f"File extension '{ext}' is not supported. "
            f"Supported extensions are: {','.join(list(supported_files.keys()))}"
        )

    return read_func


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
