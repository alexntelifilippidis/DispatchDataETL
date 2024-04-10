# This logger is imported in source/data_loader/utils.py:9
import logging


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

        # Create a formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        # Add the file handler to the logger
        self.logger.addHandler(file_handler)

    def get_logger(self) -> logging.Logger:
        """
        Get the logger instance.

        Returns:
            The logger instance.
        """
        return self.logger
