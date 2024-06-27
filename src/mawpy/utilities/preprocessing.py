from datetime import datetime

import pandas as pd

from mawpy.constants import UNIX_START_DATE, UNIX_START_T


def _divide_chunks(user_id_list, n):
    user_id_chunks = []
    for i in range(0, len(user_id_list), n):
        user_id_chunks.append(user_id_list[i: i + n])
    return user_id_chunks


def get_list_of_chunks_by_column(df, column_for_chunking, in_memory_rows_count=1000):
    user_id_list = df[column_for_chunking].unique().tolist()
    print("total number of users to be processed: ", len(user_id_list))

    user_id_chunks = list(_divide_chunks(user_id_list, in_memory_rows_count))
    print("number of chunks to be processed", len(user_id_chunks))
    return user_id_chunks


def get_preprocessed_dataframe(input_file_path):
    input_df = pd.read_csv(input_file_path)
    input_df[UNIX_START_DATE] = input_df[UNIX_START_T].apply(
        lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d'))
    return input_df
