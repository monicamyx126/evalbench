import vertexai
from util.rate_limit import ResourceExhaustedError
from util.gcp import get_gcp_project, get_gcp_region
from vertexai.generative_models._generative_models import (
    GenerativeModel,
    GenerationResponse,
)
from google.api_core.exceptions import ResourceExhausted
from .generator import QueryGenerator
import logging


class GeminiGenerator(QueryGenerator):
    """Generator queries using Vertex model."""

    def __init__(self, querygenerator_config):
        super().__init__(querygenerator_config)
        self.name = "gcp_vertex_gemini"
        self.project_id = get_gcp_project(querygenerator_config.get("gcp_project_id"))
        self.region = get_gcp_region(querygenerator_config.get("gcp_region"))
        self.vertex_model = querygenerator_config["vertex_model"]
        self.base_prompt = querygenerator_config.get("base_prompt") or ""
        self.generation_config = None

        vertexai.init(project=self.project_id, location=self.region)
        self.model = GenerativeModel(self.vertex_model)
        self.base_prompt = self.base_prompt

    def generate_internal(self, prompt):
        logger = logging.getLogger(__name__)
        try:
            response = self.model.generate_content(
                self.base_prompt + prompt,
                generation_config=self.generation_config,
            )
            if isinstance(response, GenerationResponse):
                r = response.text
                r = r.replace(
                    "```sql", ""
                )  # required for gemini_1.0_pro, gemini_2.0_flash, gemini_2.5_pro
                r = r.replace(
                    "```", ""
                )  # required for gemini_1.0_pro, gemini_2.0_flash, gemini_2.5_pro
            return r
        except ResourceExhausted as e:
            raise ResourceExhaustedError(e)
        except Exception as e:
            logger.exception("Unhandled exception during generate_content")
            raise
