import os
from datetime import datetime
import dask.dataframe as dd
import logging

try:
    from log import log_message
    from prints import custom_print, print_error, register_time_spent
except ImportError:
    from src.log import log_message
    from src.prints import custom_print, print_error, register_time_spent


def read_parquet_file(file_path: str, logger: logging.Logger) -> dd.DataFrame:
    """
    Reads a Parquet file into a Dask DataFrame.

    Parameters:
    file_path (str): The full path to the Parquet file.
    logger (logging.Logger): The logger instance for logging operations.

    Returns:
    dd.DataFrame: The loaded Dask DataFrame or None if the read fails.
    """
    try:
        custom_print(logger, f"Reading file: {file_path}", space_lines=True)
        start_time = datetime.now()

        dask_df = dd.read_parquet(file_path, split_row_groups=True)

        register_time_spent(logger, start_time, f"Reading {file_path}")

        return dask_df
    except Exception as e:
        print_error(logger, f"Failed to read file: {file_path}. Error: {str(e)}")
        return None


def analyze_dask_dataframe(dask_df: dd.DataFrame, name: str, logger: logging.Logger, n: int = 10) -> None:
    """
    Analyzes a Dask DataFrame and logs various metrics like info and summary statistics.

    Parameters:
    dask_df (dd.DataFrame): The Dask DataFrame to analyze.
    name (str): The name of the DataFrame for logging.
    logger (logging.Logger): The logger instance.
    n (int): The number of rows to print during the analysis.

    Returns:
    None
    """
    start_time = datetime.now()
    custom_print(logger, f"--- Analyzing {name} DataFrame ---")

    try:
        custom_print(logger, f"Displaying data sample {n} rows:")
        df_sample = dask_df.head(n)
        df_markdown = df_sample.to_markdown(index=False)
        custom_print(logger, f"\n{df_markdown}\n", skip_lines=True)
        
        custom_print(logger, f"\nDataFrame info for {name}:")
        custom_print(logger, str(dask_df.dtypes))
        
        custom_print(logger, "\nSummary statistics:")
        custom_print(logger, str(dask_df.describe().compute()))
        
        custom_print(logger, "\nMissing values per column:")
        custom_print(logger, str(dask_df.isnull().sum().compute()))
        
        register_time_spent(logger, start_time, f"Analyzing {name} DataFrame")
        
    except Exception as e:
        print_error(logger, f"Error analyzing {name}: {str(e)}")


def read_and_analyze_files(data_dir: str, file_names: list, logger: logging.Logger) -> dict:
    """
    Reads and analyzes multiple Parquet files located in the specified directory.

    Parameters:
    data_dir (str): The directory where the Parquet files are located.
    file_names (list): A list of Parquet file names to read and analyze.
    logger (logging.Logger): The logger instance.

    Returns:
    dict: A dictionary containing the file names as keys and their corresponding Dask DataFrames as values.
    """
    dict_df = {}
    
    for file_name in file_names:
        file_path = os.path.join(data_dir, file_name)
        dask_df = read_parquet_file(file_path, logger)
        
        if dask_df is not None:
            row_count = dask_df.shape[0].compute()
            if row_count == 0:
                error_message = f"File {file_name} is empty."
                print_error(logger, error_message)
                raise ValueError(error_message)
            else:
                analyze_dask_dataframe(dask_df, file_name, logger)
                dict_df.update({file_name: dask_df})
        else:
            error_message = f"Failed to load {file_name}. No DataFrame returned."
            print_error(logger, error_message)
    
    return dict_df
