from .gemini import GeminiGenerator
from .magick import MagicSQLGenerator


def get_generator(model_config):
    if model_config["generator"] == "gemini":
        return GeminiGenerator(model_config)
    if model_config["generator"] == "magick":
        return MagicSQLGenerator(model_config)
