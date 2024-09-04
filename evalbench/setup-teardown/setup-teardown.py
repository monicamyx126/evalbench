import sys
import os
import csv
import logging
import argparse
sys.path.append('..')

from util.config import load_yaml_config
from databases import get_database
from google.protobuf import text_format
from schema_detail_pb2 import SchemaDetails

logging.getLogger().setLevel(logging.INFO)


def connect_and_execute(setup_config, query_list: list[str]):
    result = None
    error = None
    db_instance = get_database(setup_config)
    if setup_config["db"] == "mysql":
        for query in query_list:
            result, error = db_instance.execute(query)
            if error:
                logging.error("An error occurred: %s", error)
    elif setup_config["db"] == "postgres":
        combined_query = "\n".join(query_list)
        result, error = db_instance.execute(combined_query)
        if error:
            logging.error("An error occurred: %s", error)
    return result, error

def drop_all_tables(setup_config):
    if setup_config["db"] == "postgres":
        query = """
        DO $$
        DECLARE
            r RECORD;
        BEGIN
            FOR r IN (SELECT table_name FROM information_schema.tables WHERE table_schema = 'public')
            LOOP
                EXECUTE 'DROP TABLE IF EXISTS ' || r.table_name || ' CASCADE';
            END LOOP;
        END $$;
        """
        drop_all_query = [query]
    elif setup_config["db"] == "mysql":
        drop_all_query = [
        "SET GROUP_CONCAT_MAX_LEN = 32768;",
        "SET @tables = NULL;",
        "SELECT GROUP_CONCAT('`', table_name, '`') INTO @tables FROM information_schema.tables \
        WHERE table_schema = (SELECT DATABASE());",
        "SET @tables = CONCAT('DROP TABLE IF EXISTS ', @tables);",
        "PREPARE stmt FROM @tables;",
        "EXECUTE stmt;",
        "DEALLOCATE PREPARE stmt;"
        ]

    result, error = connect_and_execute(setup_config, drop_all_query)
    return result, error


def parse_textproto_file(textproto_path):
    schema_details = SchemaDetails()
    with open(textproto_path, 'r') as file:
        text_format.Merge(file.read(), schema_details)
    return schema_details


def generate_insert_commands(directory, engine):
    insertion_strings = []

    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            table_name = filename[:-4]

            with open(os.path.join(directory, filename), 'r') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    values = ", ".join([f"{value}" for value in row])

                    if engine == 'postgres':
                        insertion_strings.append(f"INSERT INTO public.{table_name} VALUES ({values});")
                    elif engine == 'mysql':
                        insertion_strings.append(f"INSERT INTO `{table_name}` VALUES ({values});")

    return insertion_strings


def generate_table_creation_commands(schema, excluded_columns=None):
    create_statements = []

    for table in schema.tables:
        table_name = table.table
        columns = []

        for column in table.columns:
            column_name = column.column
            data_type = column.data_type

            if excluded_columns and column_name in excluded_columns:
                continue

            columns.append(f"{column_name} {data_type}")

        columns_str = ",\n    ".join(columns)
        create_statement = f"CREATE TABLE {table_name} (\n    {columns_str}\n);"
        create_statements.append(create_statement)

    return create_statements


def main():
    logging.info("Running setup-teardown...")

    parser = argparse.ArgumentParser(description="Setup Teardown Script")
    parser.add_argument("--setup_config_file", type=str, required=True, help="Path to the setup configuration file")
    args = parser.parse_args()

    setup_config = load_yaml_config(args.setup_config_file)
    if not setup_config:
        logging.error("Setup configuration file not found.")
        return

    db_engine = setup_config['db']
    database_name = setup_config['database_name']

    setup_file = f"schema_details/bat/{database_name}/setup.yaml"
    setup = load_yaml_config(setup_file)
    if not setup:
        logging.error("setup.yaml file not found.")
        return

    drop_all_tables(setup_config)

    setup_commands = {
      "pre_setup": [],
      "schema_creation": [],
      "post_schema_creation": [],
      "data_insertion": [],
      "post_setup": [],
      "post_data_insertion_checks": []
    }

    for section in ["pre_setup", "post_schema_creation", "post_setup", "post_data_insertion_checks"]:
        commands = setup['setup_commands'][section][db_engine]
        setup_commands[section].extend(commands)

    # Create schema creation commands
    schema = parse_textproto_file(f"schema_details/bat/{database_name}/{db_engine}.textproto")
    setup_commands['schema_creation'] = generate_table_creation_commands(schema, setup['setup_commands']['excluded_columns'][db_engine])

    # Create data insertion commands
    data_directory = f"datasets/bat/{database_name}/"
    setup_commands['data_insertion'] = generate_insert_commands(data_directory, db_engine)

    for section in setup_commands:
        print(f"Executing setup commands for section: {section}")
        connect_and_execute(setup_config, setup_commands[section])

    print("Setup completed successfully.")


if __name__ == "__main__":
    main()
