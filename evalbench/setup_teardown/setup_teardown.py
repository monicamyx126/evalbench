import os
import sys
import importlib
import logging
from google.protobuf import text_format
from .schema_details.schema_detail_pb2 import SchemaDetails

sys.path.append('..')
sys.path.append('setup_teardown')

util_config = importlib.import_module("util.config")
databaseHandler = importlib.import_module("databaseHandler")

logging.getLogger().setLevel(logging.INFO)

def is_bat_dataset(database_name):
    bat_datasets = {"db_hr", "db_blog", "db_chat", "db_ecommerce", "db_finance"}
    return database_name in bat_datasets


def parse_textproto_file(textproto_path):
    schema_details = SchemaDetails()
    with open(textproto_path, 'r') as file:
        text_format.Merge(file.read(), schema_details)
    return schema_details


def setupDatabase(db_config: dict, database: str, no_data: bool = False):
    if not is_bat_dataset(database):
        return

    logging.info("Running setup-teardown...")
    db_engine = db_config['db']
    
    setup_file = os.path.join(os.path.dirname(__file__), f"schema_details/bat/{database}/setup.yaml")
    setup = util_config.load_yaml_config(setup_file)
    if not setup:
        logging.error("setup.yaml file not found.")
        return

    db_handler = databaseHandler.get_db_handler(db_config)
    result, error = db_handler.drop_all_tables()
    if error:
        logging.error(f"Error while dropping tables: {error}")
        return

    setup_commands = {
        "pre_setup": [],
        "schema_creation": [],
        "post_schema_creation": [],
        "data_insertion": [],
        "post_setup": [],
        "post_data_insertion_checks": []
    }

    for section in ["pre_setup", "post_schema_creation", "post_setup"]:
        commands = setup['setup_commands'][section][db_engine]
        setup_commands[section].extend(commands)

    schema_file = os.path.join(os.path.dirname(__file__), f"schema_details/bat/{database}/{db_engine}.textproto")
    schema = parse_textproto_file(schema_file)
    setup_commands['schema_creation'] = db_handler.create_schema_statements(
        schema, setup['setup_commands']['excluded_columns'][db_engine]
    )

    if not no_data:
        data_directory = os.path.join(os.path.dirname(__file__), f"datasets/bat/{database}/")
        setup_commands['data_insertion'] = db_handler.create_insert_statements(data_directory)
        setup_commands['post_data_insertion_checks'] = setup['setup_commands']['post_data_insertion_checks'][db_engine]

    for section in setup_commands:
        if no_data and section in ["data_insertion", "post_data_insertion_checks"]:
            continue
        logging.info(f"Executing setup commands for section: {section}")
        result, error = db_handler.execute(setup_commands[section])
        if error is not None:
            logging.error(f"Error in section {section}: {error}")
            return

    logging.info("Setup completed successfully.")
