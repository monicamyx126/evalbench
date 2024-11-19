"""Simple comparison strategy that checks if the two execution results are exactly the same."""

from typing import Tuple

from scorers import comparator


class ExactMatcher(comparator.Comparator):
    """ExactMatcher.

    Attributes:
      name:
    """

    def __init__(self, config: dict):
        self.name = "exact_match"
        self.config = config

    def compare(
        self,
        nl_prompt: str,
        golden_query: str,
        query_type: str,
        golden_execution_result: str,
        golden_eval_result: str,
        golden_error: str,
        generated_query: str,
        generated_execution_result: str,
        generated_eval_result: str,
        generated_error: str,
    ) -> Tuple[float, str]:
        """Simple comparison strategy that checks if the two execution results are exactly the same."""
        if golden_error or generated_error:
            return 0, None
        if self.config and "use_eval_sql" in self.config:
            score = 100 if golden_eval_result == generated_eval_result else 0
            return score, None
        else:
            score = 100 if golden_execution_result == generated_execution_result else 0
            return score, None
