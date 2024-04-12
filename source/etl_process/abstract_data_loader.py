from abc import ABC, abstractmethod
from typing import Any


class AbstractDataReader(ABC):
    """Abstract class for reading data asynchronously."""

    @abstractmethod
    async def read_data(self, **kwargs) -> Any:
        """Read data asynchronously."""
        pass

    @abstractmethod
    async def transform_data(self, **kwargs) -> Any:
        """Transform data asynchronously."""
        pass

    @abstractmethod
    async def check_data(self, **kwargs) -> Any:
        """Check if data is in the correct format asynchronously."""
        pass


class AbstractDataLoader(ABC):
    """Abstract class for loading data into a database."""

    @abstractmethod
    async def load_data_to_db(self, **kwargs) -> None:
        """Load data into a database asynchronously."""
        pass
