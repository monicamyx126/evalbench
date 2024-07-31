import logging

from pyaml_env import parse_config
import pandas as pd
import json

logging.getLogger().setLevel(logging.INFO)


def load_yaml_config(yaml_file):
    config = parse_config(yaml_file)
    return config


def config_to_df(
    job_id: str,
    run_time: str,
    experiment_config: dict,
    model_config: dict,
    db_config: dict,
):
    configs = []
    config = {
        "experiment_config": experiment_config,
        "model_config": model_config,
        "db_config": db_config,
    }
    df = pd.json_normalize(config, sep=".")
    d_flat = df.to_dict(orient="records")[0]
    for key in d_flat:
        configs.append(
            {
                "job_id": job_id,
                "run_time": run_time,
                "config": key,
                "value": d_flat[key],
            }
        )
    df = pd.DataFrame.from_dict(configs)
    df[["job_id", "config", "value"]] = df[["job_id", "config", "value"]].astype("string")
    return df
