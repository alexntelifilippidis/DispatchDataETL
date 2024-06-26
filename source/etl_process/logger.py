import logging
import sys
import time

# Define COLORS dictionary if not defined already
COLORS = {
    "BLUE": "\033[94m",
    "GREEN": "\033[92m",
    "YELLOW": "\033[93m",
    "RED": "\033[91m",
    "ORANGE": "\033[38;5;208m",  # ANSI escape code for orange color
    "RESET": "\033[0m",
}


class MyLogger:
    def __init__(self, name: str, log_file: str = "logfile.log") -> None:
        """
        Initialize the logger.

        :param name: The name of the logger.
        :type name: str
        :param log_file: The path to the log file. Defaults to 'logfile.log'.
        :type log_file: str, optional
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

        # Set exception hook to log uncaught exceptions
        sys.excepthook = self.log_uncaught_exception

    def get_logger(self) -> logging.Logger:
        """
        Get the logger instance.

        :return: The logger instance.
        :rtype: logging.Logger
        """
        return self.logger

    async def log_with_time_elapsed(self, message: str) -> None:
        """
        Log a message with the time elapsed since the previous log.

        :param message: The message to log.
        :type message: str
        """
        current_time = time.time()
        time_elapsed = current_time - self.prev_time
        self.prev_time = current_time
        self.logger.info(f"{message} - Time elapsed since previous log: {time_elapsed:.2f} seconds")

    def log_uncaught_exception(self, exc_type, exc_value, exc_traceback):
        """
        Log uncaught exceptions.

        :param exc_type: The exception type.
        :type exc_type: Exception
        :param exc_value: The exception value.
        :type exc_value: Exception
        :param exc_traceback: The exception traceback.
        :type exc_traceback: traceback
        """
        self.logger.error("Uncaught exception occurred:", exc_info=(exc_type, exc_value, exc_traceback))


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
            return f'{COLORS["ORANGE"]}{msg}{COLORS["RESET"]}'
        elif level == "ERROR" or level == "CRITICAL":
            return f'{COLORS["RED"]}{msg}{COLORS["RESET"]}'
        else:
            return msg
