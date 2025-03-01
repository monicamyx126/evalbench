from sqlalchemy.pool import NullPool
import sqlalchemy
from sqlalchemy import text, MetaData
import logging
from .db import DB
from google.cloud.sql.connector import Connector
from .util import (
    get_db_secret,
    rate_limited_execute,
    with_cache_execute,
    DBResourceExhaustedError,
    DatabaseSchema,
)
from typing import Any, List, Optional, Tuple


class SQLServerDB(DB):

    #####################################################
    #####################################################
    # Database Connection Setup Logic
    #####################################################
    #####################################################

    def __init__(self, db_config):
        super().__init__(db_config)
        logging.getLogger("pytds").setLevel(logging.ERROR)
        self.connector = Connector()

        def get_conn():
            conn = self.connector.connect(
                f"{db_config['project_id']}:{db_config['region']}:{db_config['instance_name']}",
                "pytds",
                user=db_config["user_name"],
                password=get_db_secret(db_config["password"]),
                db=self.db_name,
            )
            return conn

        def get_engine_args():
            common_args = {
                "creator": get_conn,
                "connect_args": {"command_timeout": 60, "multi_statements": True},
                "echo": False,
                "logging_name": None,
            }
            if "is_tmp_db" in db_config:
                common_args["poolclass"] = NullPool
            else:
                common_args["pool_size"] = 50
                common_args["pool_recycle"] = 300
            return common_args

        self.engine = sqlalchemy.create_engine("mssql+pytds://", **get_engine_args())

    def close_connections(self):
        try:
            self.engine.dispose()
            self.connector.close()
        except Exception:
            logging.warning(
                f"Failed to close connections. This may result in idle unused connections."
            )

    #####################################################
    #####################################################
    # Database Specific Execution Logic and Handling
    #####################################################
    #####################################################

    def batch_execute(self, commands: list[str]):
        _, _, error = self.execute(";\n".join(commands))
        if error:
            raise RuntimeError(f"{error}")

    def execute(
        self,
        query: str,
        eval_query: Optional[str] = None,
        use_cache=False,
        rollback=False,
    ) -> Tuple[Any, Any, Any]:
        if query.strip() == "":
            return None, None, None
        if not use_cache or not self.cache_client or eval_query:
            return self._execute(query, eval_query, rollback)
        return with_cache_execute(
            query,
            self.engine.url,
            self._execute,
            self.cache_client,
        )

    def _execute(
        self,
        query: str,
        eval_query: Optional[str] = None,
        rollback=False,
    ) -> Tuple[Any, Any, Any]:
        def _run_execute(query: str, eval_query: Optional[str] = None, rollback=False):
            result: List = []
            eval_result: List = []
            error = None
            try:
                with self.engine.connect() as connection:
                    with connection.begin() as transaction:
                        resultset = connection.execute(text(query))
                        if resultset.returns_rows:
                            rows = resultset.fetchall()
                            result.extend(r._asdict() for r in rows)

                        if eval_query:
                            eval_resultset = connection.execute(text(eval_query))
                            if eval_resultset.returns_rows:
                                eval_rows = eval_resultset.fetchall()
                                eval_result.extend(r._asdict() for r in eval_rows)

                        if rollback:
                            transaction.rollback()
            except Exception as e:
                error = str(e)
                if "57P03" in error:
                    raise DBResourceExhaustedError("DB Exhausted") from e
            return result, eval_result, error

        return rate_limited_execute(
            (query, eval_query, rollback),
            _run_execute,
            self.execs_per_minute,
            self.semaphore,
            self.max_attempts,
        )

    def get_metadata(self) -> dict:
        db_metadata = {}

        try:
            with self.engine.connect() as connection:
                metadata = MetaData()
                metadata.reflect(bind=connection, schema=self.db_name)
                for table in metadata.tables.values():
                    columns = []
                    for column in table.columns:
                        columns.append({"name": column.name, "type": str(column.type)})
                    db_metadata[table.name] = columns
        except Exception:
            pass

        return db_metadata

    #####################################################
    #####################################################
    # Setup / Teardown of temporary databases
    #####################################################
    #####################################################

    def generate_ddl(
        self,
        schema: DatabaseSchema,
    ) -> list[str]:
        raise RuntimeError("Not yet supported.")

    def create_tmp_database(self, database_name: str):
        raise RuntimeError("Not yet supported.")

    def drop_tmp_database(self, database_name: str):
        raise RuntimeError("Not yet supported.")

    def drop_all_tables(self):
        raise RuntimeError("Not yet supported.")

    def insert_data(self, data: dict[str, List[str]]):
        raise RuntimeError("Not yet supported.")

    #####################################################
    #####################################################
    # Database User Management
    #####################################################
    #####################################################

    def create_tmp_users(self, dql_user: str, dml_user: str, tmp_password: str):
        raise RuntimeError("Not yet supported.")

    def delete_tmp_user(self, username: str):
        raise RuntimeError("Not yet supported.")
