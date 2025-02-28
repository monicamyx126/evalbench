from .pg_dbschema import PGDBSchemaGenerator
from .mysql_dbschema import MySQLDBSchemaGenerator
from .passthrough import NOOPGenerator


def get_generator(db, promptgenerator_config):
    if promptgenerator_config["prompt_generator"] == "PG_DBSchemaGenerator":
        return PGDBSchemaGenerator(db, promptgenerator_config)
    if promptgenerator_config["prompt_generator"] == "MySQL_DBSchemaGenerator":
        return MySQLDBSchemaGenerator(db, promptgenerator_config)
    if promptgenerator_config["prompt_generator"] == "NOOPGenerator":
        return NOOPGenerator(None, promptgenerator_config)
