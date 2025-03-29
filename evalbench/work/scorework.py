"""ScorerWork is the class for all scoring work."""

from typing import Any
from scorers import score
from work import Work


class ScorerWork(Work):
    """ScorerWork is the class for all scoring work."""

    def __init__(
        self,
        experiment_config: dict,
        eval_result: dict,
        scoring_results: list,
        global_models,
    ):
        self.experiment_config = experiment_config
        self.eval_result = eval_result
        self.scoring_results = scoring_results
        self.global_models = global_models

    def run(self, work_config: Any = None) -> dict:
        """Score the work item.

        Args:
          work_config:

        Returns:

        """
        score.compare(
            self.eval_result,
            self.experiment_config,
            self.scoring_results,
            self.global_models,
        )
        return self.eval_result
