from abc import ABC, abstractmethod
from typing import Any, Tuple


class DB(ABC):

    def __init__(self, db_config):
        pass

    @abstractmethod
    def generate_schema(self):
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def execute(self, query: str) -> Tuple[Any, float]:
        """Execute a query string and return the execution result and the time spent.
        Args:
            query (str): The SQL query to execute.

        Returns:
            Tuple[Any, float]: The result of the query execution and the time spent.
        """
        raise NotImplementedError("Subclasses must implement this method")
