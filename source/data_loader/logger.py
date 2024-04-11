# This logger is imported in source/data_loader/utils.py:9
import asyncio
import logging
import sys
import time

# Color escape codes
COLORS = {
    "RESET": "\033[0m",
    "BOLD": "\033[1m",
    "RED": "\033[91m",
    "GREEN": "\033[92m",
    "YELLOW": "\033[93m",
    "BLUE": "\033[94m",
}


class MyLogger:
    def __init__(self, name: str, log_file: str = "logfile.log") -> None:
        """
        Initialize the logger.

        Args:
            name: The name of the logger.
            log_file: The path to the log file. Defaults to 'logfile.log'.
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # Create a file handler
        file_handler = logging.FileHandler(log_file)

        # Create a stream handler for terminal output
        stream_handler = logging.StreamHandler(stream=sys.stdout)

        # Create a formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # Set formatter for file handler and stream handler
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(ColoredFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

        # Add the file handler and stream handler to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_handler)

        # Initialize timestamp for time measurement
        self.prev_time = time.time()

    def get_logger(self) -> logging.Logger:
        """
        Get the logger instance.

        Returns:
            The logger instance.
        """
        return self.logger

    async def log_with_time_elapsed(self, message: str) -> None:
        """
        Log a message with the time elapsed since the previous log.

        Args:
            message: The message to log.
        """
        current_time = time.time()
        time_elapsed = current_time - self.prev_time
        self.prev_time = current_time
        self.logger.info(f"{message} - Time elapsed since previous log: {time_elapsed:.2f} seconds")


# Custom formatter for colored log messages
class ColoredFormatter(logging.Formatter):
    def format(self, record):
        level = record.levelname
        msg = super().format(record)
        if level == "DEBUG":
            return f'{COLORS["BLUE"]}{msg}{COLORS["RESET"]}'
        elif level == "INFO":
            return f'{COLORS["GREEN"]}{msg}{COLORS["RESET"]}'
        elif level == "WARNING":
            return f'{COLORS["YELLOW"]}{msg}{COLORS["RESET"]}'
        elif level == "ERROR" or level == "CRITICAL":
            return f'{COLORS["RED"]}{msg}{COLORS["RESET"]}'
        else:
            return msg
