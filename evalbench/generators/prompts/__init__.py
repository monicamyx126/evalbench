from .sqlgenbase import SQLGenBasePromptGenerator
from .passthrough import NOOPGenerator


def get_generator(db, promptgenerator_config):
    if promptgenerator_config["prompt_generator"] == "SQLGenBasePromptGenerator":
        return SQLGenBasePromptGenerator(db, promptgenerator_config)
    if promptgenerator_config["prompt_generator"] == "NOOPGenerator":
        return NOOPGenerator(None, promptgenerator_config)
