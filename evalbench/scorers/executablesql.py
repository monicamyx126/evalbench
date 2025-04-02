"""
ExecutableGenerationScore

This scorer tests whether the generated query is executable. It scores 100 if the generated query
runs without error (i.e. generated_error is empty) and 0 if an error is present.
"""

from typing import Tuple

from scorers import comparator


class ExecutableGenerationScore(comparator.Comparator):
    """
    ExecutableGenerationScore implements the Comparator base class to verify the executability of the generated query.

    Attributes:
      - name: Name of the comparator. Set to "executable match"
      - config: The scorer configuration defined in the run config yaml file
    """

    def __init__(self, config: dict):
        self.name = "executable_sql"
        self.config = config

    def compare(
        self,
        nl_prompt: str,
        golden_query: str,
        query_type: str,
        golden_execution_result: list,
        golden_eval_result: str,
        golden_error: str,
        generated_query: str,
        generated_execution_result: list,
        generated_eval_result: str,
        generated_error: str,
    ) -> Tuple[float, str]:
        """
        Compares whether the generated query is executable based on the generated_error.

        Returns:
            A tuple containing:
            - A score (100 if executable, 0 if not)
            - A message describing the result.
        """
        if generated_error:
            return 0.0, f"Generated query was not executable: {generated_error}"
        else:
            return 100.0, "Generated query was executable."
