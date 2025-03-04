from .gemini import GeminiGenerator
from .passthrough import NOOPGenerator
from .claude import ClaudeGenerator


def get_generator(core_db, model_config):
    if model_config["generator"] == "gemini":
        return GeminiGenerator(core_db, model_config)
    if model_config["generator"] == "noop":
        return NOOPGenerator(core_db, model_config)
    if model_config["generator"] == "claude":
        return ClaudeGenerator(core_db, model_config)
