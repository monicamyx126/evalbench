"""EvalBench is a framework to measure the quality of a generative AI (GenAI) workflow."""

from collections.abc import Sequence
from absl import app
from absl import flags
from util.config import load_yaml_config, config_to_df
from dataset.dataset import load_json, load_dataset_from_json, flatten_dataset
from evaluator.evaluator import Evaluator
import reporting.report as report
import reporting.bqstore as bqstore
import reporting.analyzer as analyzer
import logging
from util.config import set_session_configs
from util.service import load_session_configs, create_eval_instances
import os

logging.getLogger().setLevel(logging.INFO)

CSV_OUTPUT = "csv"
BIG_QUERY_OUTPUT = "big_query"

_EXPERIMENT_CONFIG = flags.DEFINE_string(
    "experiment_config",
    "configs/experiment_config.yaml",
    "Path to the eval execution configuration file.",
)

_OUTPUT_TYPE = flags.DEFINE_string(
    "output_type",
    BIG_QUERY_OUTPUT,
    "Specifies the output type: 'csv' for a CSV file, 'big_query' to store results in BigQuery",
)


def store_data(output_type, data_df, csv_file_name, bigquery_store_type, job_id):
    if output_type == CSV_OUTPUT:
        logging.info(f"Storing {csv_file_name}")
        data_df.to_csv(f"{csv_file_name}_{job_id}.csv", index=False)
    else:
        report.store(data_df, bigquery_store_type)


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
        config, db_config, model_config, setup_config = load_session_configs(session)
        logging.info("Loaded Configurations in %s", _EXPERIMENT_CONFIG.value)

        # Load the dataset
        dataset = load_dataset_from_json(session["dataset_config"], config)

        # Load the instances for eval
        core_db, model_generator, prompt_generator = create_eval_instances(
            config, db_config, model_config
        )

        # Load the evaluator
        evaluator = Evaluator(
            config, prompt_generator, model_generator, db_config, core_db, setup_config
        )

        # Run evaluations
        evaluator.evaluate(flatten_dataset(dataset))
        job_id, run_time = evaluator.process()
        core_db.clean_tmp_creations()
        core_db.close_connections()

        config_df = config_to_df(job_id, run_time, config, model_config, db_config)

        output_type = _OUTPUT_TYPE.value

        store_data(output_type, config_df, "configs", bqstore.STORETYPE.CONFIGS, job_id)

        results = load_json(f"/tmp/eval_output_{job_id}.json")
        results_df = report.get_dataframe(results)
        report.quick_summary(results_df)
        store_data(output_type, results_df, "results", bqstore.STORETYPE.EVALS, job_id)

        scores = load_json(f"/tmp/score_result_{job_id}.json")
        scores_df, summary_scores_df = analyzer.analyze_result(scores, config)
        summary_scores_df["job_id"] = job_id
        summary_scores_df["run_time"] = run_time

        store_data(output_type, scores_df, "scores", bqstore.STORETYPE.SCORES, job_id)
        store_data(
            output_type, summary_scores_df, "summary", bqstore.STORETYPE.SUMMARY, job_id
        )
        print(f"Finished Job ID {job_id}")
        return os._exit(0)
    except Exception as e:
        logging.error(e)
        return os._exit(1)


if __name__ == "__main__":
    app.run(main)
