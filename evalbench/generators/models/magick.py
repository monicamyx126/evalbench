from sqlalchemy import text
from .generator import QueryGenerator
import databases
from util.config import load_yaml_config


class MagicSQLGenerator(QueryGenerator):
    """Generator queries using Vertex model."""

    def __init__(self, querygenerator_config):
        super().__init__(querygenerator_config)
        self.name = "magic_alloydb"
        self.model = querygenerator_config["vertex_model"]
        self.base_prompt = querygenerator_config["base_prompt"]
        self.project_id = querygenerator_config["project_id"]
        self.location = querygenerator_config["location"]
        db_config = querygenerator_config["database_config"]
        # # Load the DB config
        self.db = databases.get_database(db_config)

        self.magic_get_sql = """
          SELECT alloydb_ai_nl.google_get_sql_with_context('{}', '', '{}')->>'sql';
        """
        self.model_request_url = (
            f"https://{self.location}-aiplatform.googleapis.com/v1/projects/"
            f"{self.project_id}/locations/{self.location}/publishers/google/models/"
            "gemini-1.5-flash-preview-0514:streamGenerateContent"
        )
        self.magic_gemini_model_sql = f"""CALL google_ml.create_model(
      model_id => '{self.model}',
      model_request_url => '{self.model_request_url}',
      model_provider => 'google',
      model_auth_type => 'alloydb_service_agent_iam')"""

    def generate(self, human_language_question):
        question = human_language_question.replace("'", "''")
        magic_get_sql = self.magic_get_sql.format(self.model, question)

        with self.db.engine.connect() as connection:
            with connection.begin():
                result = connection.execute(text(magic_get_sql))
                result = result.scalar().replace("\\", "")
        return result
