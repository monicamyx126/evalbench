from generators.models.generator import QueryGenerator
from .gemini import GeminiGenerator
from .passthrough import NOOPGenerator
from .claude import ClaudeGenerator
from util.config import load_yaml_config

_global_models_checked_out: dict[str, QueryGenerator] = {}

def get_generator(model_config_path: str):
    global _global_models_checked_out
    if model_config_path in _global_models_checked_out:
        return _global_models_checked_out[model_config_path]
    
    model_config = load_yaml_config(model_config_path)

    # Create a new model_config
    model: QueryGenerator | None = None
    if model_config["generator"] == "gcp_vertex_gemini":
        model = GeminiGenerator(model_config)
    if model_config["generator"] == "gcp_vertex_claude":
        model = ClaudeGenerator(model_config)
    if model_config["generator"] == "noop":
        model = NOOPGenerator(model_config)
    if not model:
        raise ValueError(f"Unknown Generator {model_config['generator']}")

    _global_models_checked_out[model_config_path] = model
    return GeminiGenerator(model_config)
