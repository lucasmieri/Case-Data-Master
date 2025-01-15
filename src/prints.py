"""prints.py
This script enhances the standard print functionality with logging integration, making it suitable for 
both console output and file logging, thus ensuring that all outputs are captured comprehensively.

Function: custom_print() - A versatile print function that outputs messages to both the console and the log file. 
It includes options to add spacing and line skips for improved readability of the console output.

Function: print_error(), print_warning(), print_success(), print_info() - These functions are specialized 
versions of custom_print(), each tailored for specific types of messages (error, warning, success, and info). 
They provide clarity and context in output messaging, which is crucial for user feedback and debugging.

Function: register_time_spent() - Logs the duration taken by specific processes, an essential feature for performance
monitoring and optimization analysis.
"""
from datetime import datetime
import logging

def custom_print(logger: logging.Logger, message: str, skip_lines: bool = True, space_lines:bool=False) -> None:
    """
    Custom print function to print to both the console and the log file.

    Parameters:
    logger (logging.Logger): The logger instance.
    message (str): The message to print.
    skip_lines (bool): If True, adds an extra blank line for readability.

    Returns:
    None
    """
    if space_lines:
        space='_'*150+"\n"
    else:
        space=''
    if skip_lines:
        message = "\n" +space+ message + "\n"

    print(message)
    logger.info(message.strip())
    
def print_error(logger: logging.Logger, message: str) -> None:
    """
    Custom print function to handle error messages.

    Parameters:
    logger (logging.Logger): The logger instance.
    message (str): The error message to print.

    Returns:
    None
    """
    error_message = f"ERROR: {message}"
    print(error_message)
    logger.error(error_message)

def print_warning(logger: logging.Logger, message: str) -> None:
    """
    Custom print function to handle warning messages.

    Parameters:
    logger (logging.Logger): The logger instance.
    message (str): The warning message to print.

    Returns:
    None
    """
    warning_message = f"WARNING: {message}"
    print(warning_message)
    logger.warning(warning_message)

def print_success(logger: logging.Logger, message: str) -> None:
    """
    Custom print function to handle success messages.

    Parameters:
    logger (logging.Logger): The logger instance.
    message (str): The success message to print.

    Returns:
    None
    """
    success_message = f"SUCCESS: {message}"
    print(success_message)
    logger.info(success_message)


def print_info(logger: logging.Logger, message: str, additional_info: str = None) -> None:
    """
    Custom print function to handle informational messages.

    Parameters:
    logger (logging.Logger): The logger instance.
    message (str): The informational message to print.
    additional_info (str): Additional info to print, if provided.

    Returns:
    None
    """
    info_message = f"INFO: {message}"
    if additional_info:
        info_message += f" | Additional Info: {additional_info}"

    print(info_message)
    logger.info(info_message)


def register_time_spent(logger: logging.Logger, start_time: datetime, process_name: str) -> None:
    """
    Logs the time spent on a process.

    Parameters:
    logger (logging.Logger): The logger instance.
    start_time (datetime): The start time of the process.
    process_name (str): A string indicating the name of the process.

    Returns:
    None
    """
    end_time = datetime.now()
    time_spent = (end_time - start_time).total_seconds()  
    message = f"Time spent on {process_name}: {time_spent:.2f} seconds"  
    logger.info(message)