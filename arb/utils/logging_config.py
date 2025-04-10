"""
Provides a function to configure application-wide logging.
"""

import logging
import sys

def setup_logging(level=logging.INFO, log_to_file: bool = False, log_filename: str = "deribit_arb_bot.log"):
    """
    Configures the root logger for the application.

    Args:
        level: The minimum logging level (e.g., logging.INFO, logging.DEBUG).
        log_to_file: If True, logs will also be written to a file.
        log_filename: The name of the file to log to if log_to_file is True.
    """
    log_format = "%(asctime)s.%(msecs)03d [%(levelname)-8s] [%(name)-20s] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Create formatter
    formatter = logging.Formatter(log_format, datefmt=date_format)

    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers to avoid duplicates if called multiple times
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Configure console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Configure file handler if requested
    if log_to_file:
        try:
            file_handler = logging.FileHandler(log_filename, mode='a') # Append mode
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            logging.info(f"Logging configured. Level: {logging.getLevelName(level)}. Output to console and file '{log_filename}'.")
        except Exception as e:
             logging.error(f"Failed to configure file logging to '{log_filename}': {e}", exc_info=True)
             logging.info(f"Logging configured. Level: {logging.getLevelName(level)}. Output to console only.")
    else:
         logging.info(f"Logging configured. Level: {logging.getLevelName(level)}. Output to console.")