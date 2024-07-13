import logging
from datetime import datetime

import pandas as pd

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
    input_df = pd.read_csv(input_file_path)
    input_df[UNIX_START_DATE] = (
        input_df[UNIX_START_T]
            .astype('datetime64[s]')
            .dt.strftime('%Y-%m-%d')
    )
    return input_df
