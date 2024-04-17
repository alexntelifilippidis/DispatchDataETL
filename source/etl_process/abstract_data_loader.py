from abc import ABC, abstractmethod
from typing import Any


class AbstractDataReader(ABC):
    """Abstract class for reading data asynchronously."""

    @abstractmethod
    async def read_data(self, **kwargs) -> Any:
        """Read data asynchronously.

        :return: Data read from a source
        :rtype: Any
        """
        pass

    @abstractmethod
    async def transform_data(self, **kwargs) -> Any:
        """Transform data asynchronously.

        :return: Transformed data
        :rtype: Any
        """
        pass

    @abstractmethod
    async def check_data(self, **kwargs) -> Any:
        """Check if data is in the correct format asynchronously.

        :return: Checked data
        :rtype: Any
        """
        pass


class AbstractDataLoader(ABC):
    """Abstract class for loading data into a database."""

    @abstractmethod
    async def load_data_to_db(self, **kwargs) -> None:
        """Load data into a database asynchronously.

        :param kwargs: Additional keyword arguments for loading data
        :type kwargs: dict
        :raises DatabaseError: If there is an error while loading data into the database
        """
        pass
