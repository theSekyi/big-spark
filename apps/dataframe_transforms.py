import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path


def convert_dat_to_df(src, columns):
    """Converts .dat file to csv

    Parameters
    ----------
    src: path to .dat file
    columns: column names of .dat file

    Returns
    -------
    df: pandas dataframe
    """
    df = pd.read_csv(
        src,
        sep="|",
        header=None,
        skipinitialspace=False,
    ).reset_index(drop=True)
    df = df.iloc[:, :-1]
    df.columns = columns
    return df


def convert_df_to_pq(df, dest):
    """Converts a pandas dataframe to compressed parquet format
    Parameters
    ----------
    df: pd DataFrame
    dest: destination file  path
    """

    table = pa.Table.from_pandas(df)
    pq.write_table(table, dest, compression="GZIP")


def convert_dat_files(src, columns, dest, filetype):
    """Converts dat files to either csv or parquet format
    Parameters
    ----------
    src: path to source file
    columns: list of column names
    dest: path to destination file
    filetype: expected format of converted file ie. csv or parquet
    """

    df = convert_dat_to_df(src, columns)
    file_name = Path(src).stem
    file_path = f"{dest}/{filetype}/{file_name}.{filetype}"

    if filetype == "csv":
        df.to_csv(file_path, index=False)
    if filetype == "parquet":
        convert_df_to_pq(df, file_path)


def denormalize_csv(df_store, df_items, df_date, df_time):
    """Denormalizes the store,items,date and time dataframes"""
    df = df_store.merge(df_items, how="left", left_on=["ss_item_sk"], right_on=["i_item_sk"])
    df = df.merge(df_date, how="left", left_on=["ss_sold_date_sk"], right_on=["d_date_sk"])
    df = df.merge(df_time, how="left", left_on=["ss_sold_time_sk"], right_on=["t_time_sk"])
    return df
