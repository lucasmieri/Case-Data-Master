"""read_data.py

This script handles the ingestion and preliminary analysis of Parquet files using Dask, 
which is suitable for handling large datasets in a distributed manner.

Function: read_parquet_file() - Reads Parquet files into a Dask DataFrame. It logs the file reading process, 
providing crucial debugging and process tracking capabilities.

Function: analyze_dask_dataframe() - Performs an exploratory data analysis on the Dask DataFrame. It logs 
various metrics and statistics, offering a deep insight into the dataset's structure and content, which is 
pivotal for guiding further data processing and analysis.

Function: read_and_analyze_files() - Orchestrates the reading and analysis of multiple Parquet files. 
This function is central to the script as it integrates reading, logging, and analytical functionalities.
"""
import os
from datetime import datetime
import numpy as np
import pandas as pd
import dask.dataframe as dd
import logging
from tabulate import tabulate


try:
    from log import log_message
    from prints import custom_print, print_error, register_time_spent
except ImportError:
    from src.log import log_message
    from src.prints import custom_print, print_error, register_time_spent

def dict_types(file_name: str) -> dict:
    """
    return column types
    Parameters:
    file_name (str): name parquet file do arquivo Parquet.

    Returns:
    dict: Mapeamento de tipos de colunas para o arquivo.
    """
    type_mappings = {
        "members.parquet": {
            "msno": "string",  # User ID
            "safra": "string",  # Cohort
            "registration_init_time": "datetime64[ns]",  # Registration time
            "city": "string",  # City ID
            "bd": "int16",  # Age (must be positive, subject to outlier handling)
            "gender": "float16",  # Gender encoding (1: male, 0: female)
            "registered_via": "string",  # Registration method
            "is_ativo": "int16"  # Is active (0 or 1, must be positive)
        },
        "transactions.parquet": {
            "msno": "string",  # User ID
            "payment_method_id": "string",  # Payment method
            "payment_plan_days": "int16",  # Plan duration (positive)
            "plan_list_price": "int16",  # Plan price (positive)
            "actual_amount_paid": "int16",  # Amount paid (positive)
            "is_auto_renew": "int16",  # Auto-renew flag (0 or 1, must be positive)
            "transaction_date": "datetime64[ns]",  # Transaction date
            "membership_expire_date": "datetime64[ns]",  # Membership expiration date
            "is_cancel": "int16",  # Cancellation flag (0 or 1, must be positive)
            "safra": "string"  # Cohort
        },
    "user_logs.parquet": {
        "msno": "string",           # User ID
        "safra": "string",           # Cohort (positive)
        "num_25": "float64",          # Songs played <25% (positive)
        "num_50": "float64",          # Songs played 25%-50% (positive)
        "num_75": "float64",          # Songs played 50%-75% (positive)
        "num_985": "float64",         # Songs played 75%-98.5% (positive)
        "num_100": "float64",         # Songs played >98.5% (positive)
        "num_unq": "float64",         # Unique songs played (positive)
        "total_secs": "float64"     # Total listening time in seconds (positive)
    }

    }

    return type_mappings.get(file_name, {})

def convert_column_types(dask_df: dd.DataFrame, type_mapping: dict, logger: logging.Logger) -> dd.DataFrame:
    """
    Converts columns in a Dask DataFrame to specified types, checks for problematic values,
    validates resulting types, and ensures positive values where applicable.

    Parameters:
    dask_df (dd.DataFrame): The Dask DataFrame to convert.
    type_mapping (dict): A dictionary with column names as keys and target types as values.
    logger (logging.Logger): Logger instance for error and info logging.

    Returns:
    dd.DataFrame: The Dask DataFrame with converted column types.
    """
    for column, target_type in type_mapping.items():
        if column not in dask_df.columns:
            log_message(logger, 'warning', f"Column '{column}' not found in DataFrame. Skipping conversion.")
            continue
        
        if str(dask_df[column].dtype) == target_type:
            custom_print(logger, f"The column '{column}' is already the type '{target_type}'.")
            continue

        try:
            custom_print(logger, f"Converting column '{column}' from type {str(dask_df[column].dtype)} to '{target_type}'.")

            if column == 'gender':
                unique_genders = dask_df[column].unique().compute()
                log_message(logger, 'info', f"Unique genders found in column '{column}': {unique_genders}")
                
                gender_map = {'male': 1, 'female': 0, 'man': 1, 'woman': 0}
                dask_df[column] = dask_df[column].map(
                    gender_map, meta=(column, 'float16')
                ).fillna(np.nan)
                target_type = 'float16'
                invalid_genders = dask_df[~dask_df[column].isin([1, 0])]
                if not invalid_genders.empty:
                    log_message(logger, 'warning', "Found invalid 'gender' values. Replacing with NaN.")
                    dask_df[column] = dask_df[column].where(dask_df[column].isin([1, 0]), np.nan)
                    
            elif target_type in ["datetime32[ns]", "datetime64[ns]"]:
                dask_df[column] = dd.to_datetime(dask_df[column], errors='coerce', format='%Y%m%d')
                
            elif target_type in ["int16","int32"]:
                dask_df[column] = dask_df[column].replace(['', None], np.nan).astype(target_type)
                
            elif target_type in ["float32", "float64"]:
                dask_df[column] = dask_df[column].astype(target_type)
                
            elif target_type in ["str", "string","object"]:
                dask_df[column] = dask_df[column].astype('string[pyarrow]')
            else:
                log_message(logger, 'warning', f"Unsupported target type '{target_type}' for column '{column}'. Skipping conversion.")
                continue

            if target_type in ["int16", "int32", "float32", "float64"]:
                non_positive_values = (dask_df[column] < 0).compute()
                if non_positive_values.any():
                    log_message(logger, 'warning', f"Column '{column}' contains non-positive values. Replacing with NaN.")
                    dask_df[column] = dask_df[column].where(dask_df[column] > 0, np.nan)

            resulting_dtype = str(dask_df[column].dtype)
            if resulting_dtype != target_type:
                log_message(logger, 'error', f"Type mismatch for column '{column}'. Expected: '{target_type}', Got: '{resulting_dtype}'.")
                raise TypeError(f"Column '{column}' conversion failed to match expected type '{target_type}'.")
            
            custom_print(logger, f"Column '{column}' successfully converted to type '{resulting_dtype}'.")

        except Exception as e:
            log_message(logger, 'error', f"Error converting column '{column}' to type '{target_type}': {str(e)}")

    return dask_df




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
    Analyzes a Dask DataFrame and logs various metrics like info, summary statistics, and missing values.

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
        custom_print(logger, f"Displaying first {n} rows of the DataFrame:")
        df_sample = dask_df.head(n)
        custom_print(logger, "\n" + tabulate(df_sample, headers="keys", tablefmt="grid"), skip_lines=True)

        custom_print(logger, f"\nData types of columns in {name}:")
        custom_print(logger, str(dask_df.dtypes))

        numeric_cols = dask_df.select_dtypes(include=["number"]).columns.tolist()
        if len(numeric_cols) > 0:
            try:
                custom_print(logger, f"\nSummary statistics for numeric columns: {numeric_cols}")
                summary_stats = dask_df[numeric_cols].describe().compute()
                custom_print(logger, "\n" + tabulate(summary_stats, headers="keys", tablefmt="grid"))
            except Exception as e:
                log_message(logger, 'error', f"Error calculating summary statistics for {name}: {str(e)}")
                problematic_cols = [col for col in numeric_cols if dask_df[col].isnull().sum().compute() > 0]
                log_message(logger, 'warning', f"Problematic columns during describe: {problematic_cols}")
        else:
            custom_print(logger, "\nNo numeric columns to summarize.")

        # Missing values and proportions
        custom_print(logger, "\nMissing values and their proportions per column:")
        total_rows = len(dask_df)
        missing_values = dask_df.isnull().sum().compute()
        missing_percentages = (missing_values / total_rows) * 100
        missing_info = pd.DataFrame({
            "Missing Count": missing_values,
            "Missing Percentage (%)": missing_percentages
        })
        custom_print(logger, "\n" + tabulate(missing_info, headers="keys", tablefmt="grid"))

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
        custom_print(logger,f"Reading {file_name}")
        file_path = os.path.join(data_dir, file_name)
        type_mapping = dict_types(file_name)
        if not type_mapping:
            log_message(logger, 'warning',f"No type mapping found for {file_name}. Skipping type conversion.")
        
        dask_df = read_parquet_file(file_path, logger)
        dask_df = convert_column_types(dask_df, type_mapping, logger)

        
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
