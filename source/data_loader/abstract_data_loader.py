from abc import ABC, abstractmethod
from typing import Any


class AbstractDataReader(ABC):
    """Abstract class for reading files asynchronously."""

    @abstractmethod
    async def read_data(self, **kwargs) -> Any:
        """Read data from a file asynchronously."""
        pass


class AbstractDataLoader(ABC):
    """Abstract class for loading data into a database."""

    @abstractmethod
    async def load_data_to_db(self, **kwargs) -> None:
        """Load data into a database asynchronously."""
        pass
