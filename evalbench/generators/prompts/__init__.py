from .dbschema import DBSchemaGenerator
from .passthrough import NOOPGenerator


def get_generator(db, promptgenerator_config):
    if promptgenerator_config["prompt_generator"] == "DBSchemaGenerator":
        return DBSchemaGenerator(db, promptgenerator_config)
    if promptgenerator_config["prompt_generator"] == "NOOPGenerator":
        return NOOPGenerator(None, promptgenerator_config)
