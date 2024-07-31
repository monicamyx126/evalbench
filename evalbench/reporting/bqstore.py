from google.cloud import bigquery

import logging
from enum import Enum

STORETYPE = Enum('StoreType', ['CONFIGS', 'EVALS', 'SCORES', "SUMMARY"])

project_id = "cloud-db-nl2sql"
dataset_id = "{}.evalbench".format(project_id)
configs_table = "{}.configs".format(dataset_id)
results_table = "{}.results".format(dataset_id)
scores_table = "{}.scores".format(dataset_id)
summary_table = "{}.summary".format(dataset_id)


def store(df, STORETYPE):
    # Construct a BigQuery client object.
    client = bigquery.Client()
    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, exists_ok=True, timeout=30)
    logging.info("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    job_config = bigquery.LoadJobConfig()
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
    ]
    if STORETYPE == STORETYPE.CONFIGS:
        table = configs_table
    elif STORETYPE == STORETYPE.EVALS:
        table = results_table
    elif STORETYPE == STORETYPE.SCORES:
        table = scores_table
    elif STORETYPE == STORETYPE.SUMMARY:
        table = summary_table

    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()  # Wait for the job to complete.
