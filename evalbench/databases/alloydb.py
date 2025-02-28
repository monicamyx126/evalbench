from sqlalchemy.pool import NullPool
import sqlalchemy
from .postgres import PGDB
from google.cloud.sql.connector import Connector
from .util import get_db_secret


class AlloyDB(PGDB):

    #####################################################
    #####################################################
    # Database Connection Setup Logic
    #####################################################
    #####################################################

    def __init__(self, db_config):
        super().__init__(db_config)
        self.connector = Connector()

        def get_conn():
            instance_connection_name = (
                f"projects/{db_config['project_id']}"
                f"/locations/{db_config['region']}/clusters/my-alloydb-cluster"
                f"/instances/{db_config['instance_name']}"
            )
            conn = self.connector.connect(
                instance_connection_name,
                "pg8000",
                user=db_config["user_name"],
                password=get_db_secret(db_config["password"]),
                db=self.db_name,
                ip_type="public",
            )
            return conn

        def get_engine_args():
            common_args = {
                "creator": get_conn,
                "connect_args": {"command_timeout": 60},
            }
            if "is_tmp_db" in db_config:
                common_args["poolclass"] = NullPool
            else:
                common_args["pool_size"] = 50
                common_args["pool_recycle"] = 300
            return common_args

        self.engine = sqlalchemy.create_engine(
            "postgresql+pg8000://", **get_engine_args()
        )
