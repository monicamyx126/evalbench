
from .db import DB
from .postgres import PGDB
import sqlalchemy
import pg8000
from sqlalchemy.pool import NullPool
from google.cloud.alloydb.connector import Connector as AlloyDBConnector
from google.cloud.alloydb.connector import IPTypes as AlloyDBIPTypes


class AlloyDB(PGDB):
    def __init__(self, db_config):
        """
        Initializes the AlloyDB connection, overriding the PGDB's
        default Google Cloud SQL connection mechanism.
        """
        super().__init__(db_config)
        self.nl_config = db_config['nl_config']
        self.omni = db_config.get("omni", False)
        self.host = db_config.get("host", "localhost")
        self.port = db_config.get("port", 5432)

        self.connector = AlloyDBConnector()

        def get_conn_alloydb():
            if not self.omni:
                return self.connector.connect(
                    self.db_path,
                    "pg8000",
                    user=self.username,
                    password=self.password,
                    db=self.db_name,
                    enable_iam_auth=False,
                    ip_type=AlloyDBIPTypes.PUBLIC,
                )
            else:
                conn_str = f"user={self.username} password={self.password} host={self.host} port={self.port} dbname={self.db_name}"
                return pg8000.connect(
                    user=self.username,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    database=self.db_name
                )

        def get_engine_args_alloydb():
            common_args = {
                "creator": get_conn_alloydb,
                "connect_args": {"command_timeout": 60},
            }
            if "is_tmp_db" in db_config:
                common_args["poolclass"] = NullPool
            else:
                common_args["pool_size"] = 50
                common_args["pool_recycle"] = 300
            return common_args

        self.engine = sqlalchemy.create_engine(
            "postgresql+pg8000://", **get_engine_args_alloydb()
        )
