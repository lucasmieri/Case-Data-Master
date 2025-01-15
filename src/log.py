"""log.py

This script contains functions for configuring and utilizing logging within the Python environment. 
The use of logging is crucial for recording the execution flow and debugging.

Function: configure_logging() - Initializes and configures the logging system, allowing for logging 
across different severity levels (INFO, DEBUG, WARNING, ERROR). It supports custom log formats and automatically 
creates log directories and files based on the timestamp. This setup is essential for tracking the application's 
operational history and troubleshooting issues.

Function: log_message() - Facilitates logging messages at specified levels, ensuring that all significant events within 
the application are recorded systematically. This function is versatile, catering to different logging needs such as 
informational messages or errors.
"""
import os
import logging
from datetime import datetime

def configure_logging(log_dir: str = 'log', log_level: str = 'INFO', log_format: str = None) -> logging.Logger:
    """
    Configures logging settings for the application and returns a logger instance.

    Parameters:
    log_dir (str): The directory where the log file will be saved.
    log_level (str): The logging level (INFO, DEBUG, WARNING, ERROR).
    log_format (str): The logging format string.

    Returns:
    logging.Logger: A logger instance for logging messages.
    """
    if log_format is None:
        log_format = '%(asctime)s - %(levelname)s - %(message)s'+'\n'
    
    log_level = log_level.upper()
    log_levels = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR}
    level = log_levels.get(log_level, logging.INFO)

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file_name = f'log_src_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'
    log_file_path = os.path.join(log_dir, log_file_name)

    logging.basicConfig(filename=log_file_path, level=level, format=log_format, filemode='w')
    
    logger = logging.getLogger()
    return logger

def log_message(logger: logging.Logger, level: str, message: str) -> None:
    """
    Logs a message at the specified level using the given logger.

    Parameters:
    logger (logging.Logger): The logger instance.
    level (str): The level of the log ('INFO', 'ERROR', 'WARNING', etc.).
    message (str): The log message.

    Returns:
    None
    """
    if level.upper() == 'INFO':
        logger.info(message)
    elif level.upper() == 'WARNING':
        logger.warning(message)
    elif level.upper() == 'ERROR':
        logger.error(message)
    elif level.upper() == 'DEBUG':
        logger.debug(message)
    else:
        logger.info(message)