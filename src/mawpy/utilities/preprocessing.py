import logging
import time
from multiprocessing import Pool
from typing import Callable

import pandas as pd
import psutil

from mawpy import io
from mawpy.constants import UNIX_START_DATE, UNIX_START_T, USER_ID

logger = logging.getLogger(__name__)


def _divide_chunks(user_id_list: list[str], n: int) -> list[list[str]]:
    user_id_chunks = []
    for i in range(0, len(user_id_list), n):
        user_id_chunks.append(user_id_list[i: i + n])
    return user_id_chunks


def get_list_of_chunks_by_column(df: pd.DataFrame, column_for_chunking: str, in_memory_rows_count: int = 100) -> list[
    list[str]]:
    user_id_list = df[column_for_chunking].unique()
    logger.info(f"total number of users to be processed:  {len(user_id_list)}")

    user_id_chunks = _divide_chunks(user_id_list, in_memory_rows_count)
    logger.info(f"number of chunks to be processed:  {len(user_id_chunks)}")
    return user_id_chunks


def get_preprocessed_dataframe(input_file_path: str) -> pd.DataFrame:
    input_df = io.open_file(input_file_path)
    input_df.columns = map(str.strip, map(str.lower, input_df.columns))
    input_df[UNIX_START_DATE] = (
        input_df[UNIX_START_T]
            .astype('datetime64[s]')
            .dt.strftime('%Y-%m-%d')
    )
    return input_df


def execute_parallel(user_id_chunks: list[list[str]], input_df: pd.DataFrame, function_to_run: Callable, args: tuple):
    """
        This function is used to parallelize computation using multi-cores available on the compute instance.
        It creates a task for each chunk of ids in the user_id_chunks.
        The task is to run  'function_to_run' passed in as the argument, with parameters as 'input_df' and 'args'
        The tasks are assigned to process pool to run asynchronously.

        Post execution the results (pd.Dataframes) are collected and concatenated together and returned
    """
    pool = Pool()
    chunk_count = 0
    tasks = []
    for each_chunk in user_id_chunks:
        chunk_count += 1
        logger.info(
            f"Start processing bulk: {chunk_count} at "
            f"time: {time.strftime('%m%d-%H:%M')} memory: "
            f"{psutil.virtual_memory().percent}"
        )

        task = pool.apply_async(function_to_run,
                                (input_df[input_df[USER_ID].isin(each_chunk)], args,))
        tasks.append(task)

    pool.close()
    pool.join()

    df_output_list = [t.get().reset_index(drop=True) for t in tasks]
    df_output = pd.concat(df_output_list)
    return df_output
