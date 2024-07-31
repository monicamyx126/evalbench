"""Multiprocessing runner."""

import concurrent.futures
from typing import Any

from work import work


def do_work(work_obj: work.Work, item_config: Any = None) -> Any:
    """Do the work.

    Args:
      work_obj: The work object.
      item_config: The config for the work item.

    Returns:
      The result of the work.
    """
    return work_obj.run(item_config)


class MPRunner:
    """Multi-processing class that implements the threadpool based execution of work.

    Attributes:
      executor:
      futures:
    """

    def __init__(self, concurrent_tests: int = 10) -> None:
        """Initialize the class.

        Args:
          concurrent_tests:
        """
        self.executor = concurrent.futures.ThreadPoolExecutor(concurrent_tests)
        self.futures = []

    def execute_work(self, work_obj: work.Work) -> None:
        """Schedule to requested work.

        Args:
          work_obj: The work object.
        """
        self.futures.append(self.executor.submit(do_work, work_obj))
