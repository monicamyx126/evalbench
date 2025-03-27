from typing import Any, List
import datetime
from util import printProgressBar, truncateExecutionOutputs
from work import promptgenwork
from work import sqlgenwork
from work import sqlexecwork
from work import scorework
from mp import mprunner
import concurrent.futures
from dataset.evalinput import EvalInputRequest
from dataset.evaloutput import EvalOutput
from queue import Queue
from databases import DB


class Evaluator:
    def __init__(
        self,
        config,
    ):
        self.config = config
        runner_config = self.config.get("runners", {})
        self.promptgen_runners = runner_config.get("promptgen_runners", 10)
        self.sqlgen_runners = runner_config.get("sqlgen_runners", 10)
        self.sqlexec_runners = runner_config.get("sqlexec_runners", 10)
        self.scoring_runners = runner_config.get("scoring_runners", 10)

    def evaluate(
        self,
        dataset: List[EvalInputRequest],
        db_queue: Queue[DB],
        prompt_generator,
        model_generator,
        total_dataset_len: int,
        job_id: str,
        run_time: datetime.datetime,
    ):
        eval_outputs: List[Any] = []
        scoring_results: List[Any] = []

        self.promptrunner = mprunner.MPRunner(self.promptgen_runners)
        self.genrunner = mprunner.MPRunner(self.sqlgen_runners)
        self.sqlrunner = mprunner.MPRunner(self.sqlexec_runners)
        self.scoringrunner = mprunner.MPRunner(self.scoring_runners)
        prompt_generator.setup()

        prompt_i = 0
        gen_i = 0
        exec_i = 0
        score_i = 0

        self.promptrunner.futures.clear()
        self.genrunner.futures.clear()
        self.sqlrunner.futures.clear()
        self.scoringrunner.futures.clear()

        for eval_input in dataset:
            eval_output = EvalOutput(eval_input)
            eval_output["job_id"] = job_id
            eval_output["run_time"] = run_time
            work = promptgenwork.SQLPromptGenWork(prompt_generator, eval_output)
            self.promptrunner.execute_work(work)

        for future in concurrent.futures.as_completed(self.promptrunner.futures):
            eval_output = future.result()
            prompt_i = prompt_i + 1
            printProgressBar(
                prompt_i,
                total_dataset_len,
                prefix="Prompts:",
                suffix="Complete",
                length=50,
            )
            work = sqlgenwork.SQLGenWork(model_generator, eval_output)
            self.genrunner.execute_work(work)

        for future in concurrent.futures.as_completed(self.genrunner.futures):
            eval_output = future.result()
            gen_i = gen_i + 1
            printProgressBar(
                gen_i, total_dataset_len, prefix="SQLGen:", suffix="Complete", length=50
            )
            work = sqlexecwork.SQLExecWork(
                db_queue.get(), self.config, eval_output, db_queue
            )
            self.sqlrunner.execute_work(work)

        for future in concurrent.futures.as_completed(self.sqlrunner.futures):
            eval_output = future.result()
            exec_i = exec_i + 1
            work = scorework.ScorerWork(self.config, eval_output, scoring_results)
            self.scoringrunner.execute_work(work)
            printProgressBar(
                exec_i,
                total_dataset_len,
                prefix="SQLExec:",
                suffix="Complete",
                length=50,
            )

        for future in concurrent.futures.as_completed(self.scoringrunner.futures):
            eval_output = future.result()
            score_i = score_i + 1
            if "truncate_execution_outputs" in self.config:
                truncateExecutionOutputs(
                    eval_output,
                    self.config["truncate_execution_outputs"],
                )
            printProgressBar(
                score_i,
                total_dataset_len,
                prefix="Scoring:",
                suffix="Complete",
                length=50,
            )
            eval_outputs.append(eval_output)

        if db_queue:
            while not db_queue.empty():
                db = db_queue.get()
                db.close_connections()

        return eval_outputs, scoring_results
