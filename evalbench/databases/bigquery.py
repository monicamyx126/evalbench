from google.cloud import bigquery
import logging
from .db import DB
from .util import with_cache_execute, DatabaseSchema
from util.rate_limit import rate_limit, ResourceExhaustedError
from typing import List, Optional, Tuple, Any
import json
import sqlparse
from google.cloud.bigquery import QueryJobConfig, ConnectionProperty
from util.gcp import get_gcp_project
from google.api_core.exceptions import GoogleAPICallError


class BQDB(DB):

    #####################################################
    #####################################################
    # Database Connection Setup Logic
    #####################################################
    #####################################################

    def __init__(self, db_config):
        super().__init__(db_config)
        self.project_id = get_gcp_project("")
        self.location = db_config.get("location", "US")
        self.client = bigquery.Client(project=self.project_id)
        self.tmp_users = []

    #####################################################
    #####################################################
    # Database Specific Execution Logic and Handling
    #####################################################
    #####################################################

    def _execute_queries(self, query: str, job_config: Optional[bigquery.QueryJobConfig] = None) -> List:
        result: List = []
        for sub_query in sqlparse.split(query):
            if sub_query:
                resultset = self.client.query(sub_query, job_config)
                rows = resultset.result()
                if rows:
                    for row in rows:
                        result.append(dict(row))
        return result

    def batch_execute(self, commands: list[str]):
        for command in commands:
            self.execute(command)

    def execute(
        self, query: str, eval_query: Optional[str] = None, use_cache=False, rollback=False
    ) -> Tuple[Any, Any, Any]:
        if query.strip() == "":
            return None, None, None
        if not use_cache or not self.cache_client or eval_query:
            return self._execute(query, eval_query, rollback)
        return with_cache_execute(
            query, f"{self.project_id}.{self.db_name}", self._execute, self.cache_client
        )

    def _execute(
        self, query: str, eval_query: Optional[str] = None, rollback=False
    ) -> Tuple[Any, Any, Any]:
        def _run_execute(query: str, eval_query: Optional[str] = None, rollback=False):
            result: List = []
            eval_result: List = []
            error = None
            try:
                if rollback:
                    try:
                        initial_query = "SELECT 1;"
                        job_config = QueryJobConfig(create_session=True)
                        init_job = self.client.query(initial_query, job_config=job_config)
                        init_job.result()
                        session_id = init_job.session_info.session_id
                        conn_props = [ConnectionProperty(key="session_id", value=session_id)]

                        self.client.query(
                            "BEGIN TRANSACTION;",
                            job_config=QueryJobConfig(connection_properties=conn_props)
                        ).result()

                        result = self._execute_queries(query, job_config=QueryJobConfig(connection_properties=conn_props))

                        if eval_query:
                            eval_result = self._execute_queries(eval_query, job_config=QueryJobConfig(connection_properties=conn_props))

                        self.client.query(
                            "ROLLBACK TRANSACTION;",
                            job_config=QueryJobConfig(connection_properties=conn_props)
                        ).result()

                    except Exception as e:
                        print(f"Error: {str(e)}")

                    finally:
                        if 'session_id' in locals():
                            self.client.query(
                                "CALL BQ.ABORT_SESSION();",
                                job_config=QueryJobConfig(connection_properties=conn_props)
                            ).result()
                if not rollback:
                    result = self._execute_queries(query)

                if eval_query and not rollback:
                    eval_result = self._execute_queries(eval_query)

            except (GoogleAPICallError, Exception) as e:
                error_message = str(e)
                if "resources exceeded" in error_message:
                    raise ResourceExhaustedError(f"BigQuery resources exhausted: {e}") from e
                elif "quota exceeded" in error_message:
                    raise ResourceExhaustedError(f"BigQuery quota exceeded: {e}") from e
                else:
                    print(error_message)

            return result, eval_result, error

        try:
            return rate_limit(
                (query, eval_query, rollback),
                _run_execute,
                self.execs_per_minute,
                self.semaphore,
                self.max_attempts,
            )
        except ResourceExhaustedError as e:
            logging.info(
                "Resource Exhausted on Postgres DB. Giving up execution. Try reducing execs_per_minute."
            )
            return None, None, None

    def get_metadata(self) -> dict:
        metadata = {}
        try:
            for table in self.client.list_tables(self.db_name):
                schema = self.client.get_table(table.reference).schema
                metadata[table.table_id] = [{"name": f.name, "type": f.field_type} for f in schema]
        except Exception as e:
            print(f"Error while fetching metadata for dataset '{self.db_name}': {e}")
        return metadata

    #####################################################
    #####################################################
    # Setup / Teardown of temporary databases
    #####################################################
    #####################################################

    def generate_ddl(self, schema: DatabaseSchema) -> List[str]:
        ddl_statements = []
        try:
            for table in schema.tables:
                columns = ", ".join([f"{col.name} {col.type}" for col in table.columns])
                ddl_statements.append(
                    f"CREATE TABLE `{self.project_id}.{self.db_name}.{table.name}` ({columns})"
                )
        except Exception as e:
            print(f"Error generating DDL statements: {e}")
        return ddl_statements

    def create_tmp_database(self, database_name: str):
        dataset_ref = bigquery.Dataset(f"{self.project_id}.{database_name}")
        dataset_ref.location = self.location
        self.client.create_dataset(dataset_ref, exists_ok=True)
        self.tmp_dbs.append(database_name)

    def drop_tmp_database(self, database_name: str):
        try:
            self.client.delete_dataset(
                dataset=database_name,
                delete_contents=True,
                not_found_ok=True,
            )
            if database_name in self.tmp_dbs:
                self.tmp_dbs.remove(database_name)
        except Exception as e:
            logging.warning(f"Failed to drop dataset {database_name}: {e}")

    def drop_all_tables(self):
        try:
            tables = list(self.client.list_tables(self.db_name))

            if tables:
                for table in tables:
                    full_table_id = f"{self.project_id}.{self.db_name}.{table.table_id}"
                    self.client.delete_table(full_table_id)

        except Exception as e:
            raise RuntimeError(f"Failed to drop tables in dataset {self.db_name}: {e}")

    def is_json(self, value) -> bool:
        try:
            json.loads(value)
        except (ValueError, TypeError, json.JSONDecodeError):
            return False
        return True

    def is_float(self, value) -> bool:
        try:
            float(value)
        except ValueError:
            return False
        return True

    def insert_data(self, data: dict[str, List[str]]):
        if not data:
            return
        insertion_statements = []
        for table_name in data:
            for row in data[table_name]:
                formatted_values = []
                for value in row:
                    unquoted = value.strip("'")
                    if value == "'1'":
                        formatted_values.append("TRUE")
                    elif value == "'0'":
                        formatted_values.append("FALSE")
                    elif self.is_float(unquoted):
                        formatted_values.append(f"{value}")
                    elif value in ('True', 'true', 'False', 'false'):
                        formatted_values.append(f"{value}")
                    elif self.is_json(unquoted):
                        formatted_values.append(f"PARSE_JSON({value})")
                    else:
                        formatted_values.append(f"{value.replace("''", "\\'")}")

                inline_columns = ", ".join(formatted_values)
                insertion_statements.append(
                    f"INSERT INTO `{self.project_id}.{self.db_name}.{table_name}` VALUES ({inline_columns});"
                )
        try:
            self.batch_execute(insertion_statements)
        except RuntimeError as error:
            raise RuntimeError(f"Could not insert data into database: {error}")

    #####################################################
    #####################################################
    # Database User Management
    #####################################################
    #####################################################

    def close_connections(self):
        pass

    def create_tmp_users(self, dql_user: str, dml_user: str, tmp_password: str):
        pass

    def delete_tmp_user(self, username: str):
        pass
