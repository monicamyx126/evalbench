import uuid
import json
import datetime
import logging
import tempfile
from evaluator.evaluator import Evaluator
from evaluator.db_manager import build_db_queue
from dataset.evalinput import EvalInputRequest
from dataset.dataset import breakdown_datasets
import databases
import generators.models as models
import generators.prompts as prompts
import concurrent.futures


class Orchestrator:
    def __init__(
        self,
        config,
        db_configs,
        setup_config,
    ):
        self.config = config
        self.db_configs = db_configs
        self.setup_config = setup_config
        self.job_id = f"{uuid.uuid4()}"
        self.run_time = datetime.datetime.now()
        self.total_eval_outputs = []
        self.total_scoring_results = []

        runner_config = self.config.get("runners", {})
        self.eval_runners = runner_config.get("eval_runners", 4)
        self.sqlexec_runners = runner_config.get("sqlexec_runners", 10)

    def evaluate(self, dataset: list[EvalInputRequest]):
        """This wrapper breaks down evaluations by category of evaluations. (dql, dml, ddl).
        This allows the module to prepare the correct database connections as DDL queries
        require setting up and tearing down the databsae and DML queries require prevention
        of unintended consequences. Additionally, DQLs are run under a read-only user.
        """
        sub_datasets, total_dataset_len = breakdown_datasets(dataset)

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=self.eval_runners
        ) as executor:
            futures = []
            for dialect in sub_datasets:
                db_config = self.db_configs.get(dialect)
                if not db_config:
                    logging.info(
                        f"Skipping queries for {dialect} as no applicable db_config"
                        + " was found."
                    )
                    continue
                for database in sub_datasets[dialect]:
                    future = executor.submit(
                        self.evaluate_sub_dataset,
                        sub_datasets,
                        db_config,
                        dialect,
                        database,
                        total_dataset_len,
                    )
                    futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                eval_outputs, scoring_results = future.result()
                self.total_eval_outputs.extend(eval_outputs)
                self.total_scoring_results.extend(scoring_results)

    def evaluate_sub_dataset(
        self, sub_datasets, db_config, dialect, database, total_dataset_len
    ):
        eval_outputs = []
        scoring_results = []

        try:
            # Setup the core connection just once (for all query types in database)
            core_db = databases.get_database(db_config)
        except Exception as e:
            raise RuntimeError(
                f"Could not connect to database {database} on {dialect}; due to {e}"
            )

        for query_type in sub_datasets[dialect][database]:
            sub_dataset = sub_datasets[dialect][database][query_type]
            sub_dataset_len = len(sub_dataset)
            db_queue = None
            try:
                db_queue = build_db_queue(
                    core_db,
                    db_config,
                    self.setup_config,
                    query_type,
                    min(self.sqlexec_runners, sub_dataset_len),
                )
            except Exception as e:
                logging.info(
                    f"Skipping {query_type} queries as DB {database} "
                    + f"could not be setup properly in {dialect} due to {e}."
                )
                return

            evaluator = Evaluator(self.config)
            try:
                eval_outputs, scoring_results = evaluator.evaluate(
                    sub_dataset,
                    db_queue,
                    prompts.get_generator(core_db, self.config),
                    models.get_generator(self.config["model_config"]),
                    total_dataset_len,
                    self.job_id,
                    self.run_time,
                )
            except Exception as e:
                logging.info(
                    f"Failed to evaluate {sub_dataset_len} {query_type} queries "
                    + f"on DB {database} on {dialect}. Due to {e}"
                )

        # Cleanup all the tmp creations that were built from the core connection
        if core_db:
            core_db.clean_tmp_creations()
            core_db.close_connections()

        return eval_outputs, scoring_results

    def process(self):
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            json.dump(self.total_eval_outputs, f, sort_keys=True, indent=4, default=str)
            results_tf = f.name
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            json.dump(
                self.total_scoring_results, f, sort_keys=True, indent=4, default=str
            )
            scores_tf = f.name
        return (
            self.job_id,
            self.run_time,
            results_tf,
            scores_tf,
        )
