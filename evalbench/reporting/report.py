import pandas as pd
import reporting.bqstore as bq
import logging


def get_dataframe(results):
    results_df = pd.DataFrame.from_dict(results, dtype="string")
    logging.info("Total Prompts: %d.", len(results_df))
    return results_df


def quick_summary(results_df):
    prompt_generator_error_df = results_df["prompt_generator_error"].notnull()
    generated_error_df = results_df["generated_error"].notnull()
    sql_generator_error_df = results_df["sql_generator_error"].notnull()
    golden_error_df = results_df["golden_error"].notnull()

    logging.info("Prompt Errors: %d.", len(results_df[prompt_generator_error_df]))
    logging.info("SQLGen Errors: %d.", len(results_df[sql_generator_error_df]))
    logging.info("SQLExec Gen Errors: %d.", len(results_df[generated_error_df]))
    logging.info("Golden Errors: %d.", len(results_df[golden_error_df]))


def store(results, storetype):
    bq.store(results, storetype)
    return
