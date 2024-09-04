from .postgres import PGDB
from .alloydb import AlloyDB
from .mysql import MySQLDB


def get_database(db_config):
    if db_config["db"] == "postgres":
        return PGDB(db_config)
    if db_config["db"] == "alloydb":
        return AlloyDB(db_config)
    if db_config["db"] == "mysql":
        return MySQLDB(db_config)
