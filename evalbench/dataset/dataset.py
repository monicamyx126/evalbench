"""Process datasets."""

from absl import app
from absl import flags
import json
import logging
from collections.abc import Sequence
from dataset.evalinput import EvalInput
from dataset.evaloutput import EvalOutput


_SOURCE_DATASET_PATH = flags.DEFINE_string(
    "source_dataset_path",
    "datasets/bird_pg_dev/financial.json",
    "Path to the source dataset configuration file.",
)


def load_json(json_file_path):
    all_items = []
    json_file_path = f"{json_file_path}"
    with open(json_file_path, "r") as json_file:
        all_items.extend(json.load(json_file))
    return all_items


def load_dataset_from_json(json_file_path):
    input_items = []
    all_items = load_json(json_file_path)
    if "db_id" in all_items[0].keys():
        logging.info("dataset in BIRD Format.")
        input_items = load_dataset_from_bird(all_items)
        logging.info("Converted %d entries to EvalInput.", len(input_items))
    else:
        logging.info("dataset in regular Format.")
        input_items = load_dataset_from_regular(all_items)
        logging.info("Converted %d entries to EvalInput.", len(input_items))

    return input_items, input_items[0].database


def load_dataset_from_regular(dataset: Sequence[dict]):
    input_items = []
    gen_id = 1
    for item in dataset:
        eval_input = EvalInput(
            id=gen_id,
            query_type=item["query_type"],
            database=item["database"],
            nl_prompt=item["prompt"],
            dialects=item["dialects"],
            golden_sql=item["examples"][0],
            eval_query=item["eval_query"],
            setup_sql=item["setup_sql"],
            cleanup_sql=item["cleanup_sql"],
            tags=item["tags"],
        )
        gen_id = gen_id + 1
        input_items.append(eval_input)
    return input_items


def load_dataset_from_bird(dataset: Sequence[dict]):
    input_items = []
    for item in dataset:
        if item["result_matched"]:
            eval_input = EvalInput(
                id=item["question_id"],
                query_type="DQL",
                database=item["db_id"],
                nl_prompt=" ".join([item["question"], item["evidence"]]).replace("`", '"'),
                dialects=["postgres"],
                golden_sql=item["Postgres_query"],
                eval_query="",
                setup_sql="",
                cleanup_sql="",
                tags=[item["difficulty"]],
            )
            input_items.append(eval_input)
    return input_items


def main(argv: Sequence[str]) -> None:
    logging.info("Dataset converter v1.0.0")
    logging.info("Loading dataset from %s", _SOURCE_DATASET_PATH.value)
    dataset = load_dataset_from_json(_SOURCE_DATASET_PATH.value)
    logging.info("Loaded %d entries.", len(dataset))
    evaloutput = EvalOutput(dataset[0])
    logging.info("Done.")


if __name__ == "__main__":
    app.run(main)
