"""EvalBench is a framework to measure the quality of a generative AI (GenAI) workflow."""

from collections.abc import Sequence
from absl import app
from absl import flags
from reporting import get_reporters
from util.config import load_yaml_config, config_to_df
from dataset.dataset import load_json, load_dataset_from_json, flatten_dataset
from evaluator.orchestrator import Orchestrator
import reporting.report as report
import reporting.analyzer as analyzer
import logging
from util.config import set_session_configs
from util.service import load_session_configs
import os
import sys

try:
    import google.colab  # type: ignore

    _IN_COLAB = True
except ImportError:
    _IN_COLAB = False

logging.getLogger().setLevel(logging.INFO)

_EXPERIMENT_CONFIG = flags.DEFINE_string(
    "experiment_config",
    "configs/experiment_config.yaml",
    "Path to the eval execution configuration file.",
)


def main(argv: Sequence[str]):
    try:
        logging.info("EvalBench v1.0.0")
        session: dict = {}

        parsed_config = load_yaml_config(_EXPERIMENT_CONFIG.value)
        if parsed_config == "":
            logging.error("No Eval Config Found.")
            return

        set_session_configs(session, parsed_config)
        # Load the configs
        config, db_configs, model_config, setup_config = load_session_configs(session)
        logging.info("Loaded Configurations in %s", _EXPERIMENT_CONFIG.value)

        # Load the dataset
        dataset = load_dataset_from_json(session["dataset_config"], config)

        # Load the evaluator
        evaluator = Orchestrator(config, db_configs, setup_config, report_progress=True)

        # Run evaluations
        evaluator.evaluate(flatten_dataset(dataset))
        job_id, run_time, results_tf, scores_tf = evaluator.process()

        # Create Dataframes for reporting
        reporters = get_reporters(parsed_config.get("reporting"), job_id, run_time)
        config_df = config_to_df(job_id, run_time, config, model_config, db_configs)
        results = load_json(results_tf)
        results_df = report.get_dataframe(results)
        report.quick_summary(results_df)
        scores = load_json(scores_tf)
        scores_df, summary_scores_df = analyzer.analyze_result(scores, config)
        summary_scores_df["job_id"] = job_id
        summary_scores_df["run_time"] = run_time

        # Store the reports in specified outputs
        for reporter in reporters:
            reporter.store(config_df, report.STORETYPE.CONFIGS)
            reporter.store(results_df, report.STORETYPE.EVALS)
            reporter.store(scores_df, report.STORETYPE.SCORES)
            reporter.store(summary_scores_df, report.STORETYPE.SUMMARY)

        print(f"Finished Job ID {job_id}")
        if _IN_COLAB:
            # Exit gracefully for colab environment
            return sys.exit(0)
        return os._exit(0)
    except Exception as e:
        logging.error(e)
        return os._exit(1)


if __name__ == "__main__":
    app.run(main)
