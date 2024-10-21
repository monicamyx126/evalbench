"""
Currently the LLM Rater compares the golden execution results with the
generated sql execution results. It returns a score of 100 for concrete
positive cases, where either there is a Mismatch of Columns names or Extra Relevant
Columns in Generated SQL exists.
"""
from typing import Tuple
import logging
import backoff
import vertexai
from vertexai.preview.generative_models import GenerationConfig, GenerativeModel

from ratelimit import limits
from scorers import comparator


class LLMRater(comparator.Comparator):
    """LLMRater.

    Attributes:
      name:
    """

    def __init__(self, config: dict):
        """Constructor.

        Args:
            config: Configuration dictionary.
        """
        self.name = "llmrater"
        self.config = config
        vertexai.init(
            project=self.config["gcp_project_id"],
            location=self.config["gcp_project_location"],
        )
        self.generation_config = GenerationConfig(temperature=0)
        self.model = GenerativeModel(self.config["model"])

    @staticmethod
    def remove_duplicates(output_list: list) -> list:
        """Remove duplicates from the output list.

        Args:
          output_list: The execution output result set.

        Returns:
          The execution output result set without duplicates.
        """
        final_output = []
        for item in output_list:
            if item not in final_output:
                final_output.append(item)
        return final_output

    @backoff.on_exception(backoff.constant, exception=Exception,
                          max_tries=8, interval=80, jitter=backoff.full_jitter)
    @limits(calls=30, period=60)
    def compare(
        self,
        nl_prompt: str,
        golden_query: str,
        golden_execution_result: str,
        generated_query: str,
        generated_execution_result: str,
    ) -> Tuple[float, str]:
        only_first_n = 50
        golden_execution_result = self.remove_duplicates(golden_execution_result)
        generated_execution_result = self.remove_duplicates(generated_execution_result)

        if len(golden_execution_result) > only_first_n:
            golden_execution_result = golden_execution_result[:only_first_n]
        if len(generated_execution_result) > only_first_n:
            generated_execution_result = generated_execution_result[:only_first_n]

        prompt = f"""
        We are trying to answer this question by querying a database:

        QUESTION: {nl_prompt}

        The correct answer to this question is:

        OUTPUT #1:

        {golden_execution_result}


        We get the following answer from a second query.

        OUTPUT #2

        {generated_execution_result}


        Thinking step by step, compare the two outputs and look for differences in data presentation.
        Here are steps to follow:

        1. Ensure that every column in OUTPUT #1 has a corresponding column in OUTPUT #2 that represents
           the same information, even if the column names are different.
        2. All columns present in OUTPUT #1 must also be present in OUTPUT #2. OUTPUT #2 is allowed to
           have additional columns relevant to the query.
        3. Compare the data within each mapped column pair between OUTPUT #1 and OUTPUT #2.
           Ensure that OUTPUT #2 contains all the data from OUTPUT #1, with no missing or extra rows.
        4. Minor string transformations are allowed (e.g., concatenating first and last name), but the
           underlying information must be preserved.

        RULES - These MUST be strictly followed, to answer the FINAL QUESTION:

        1. Assume OUTPUT #1 is the gold standard and is ALWAYS correct.
        2. The order of columns in OUTPUT #2 does not matter.
        3. Allow slight variations due to differences in rounding or precision, for calculated values.
           Otherwise ensure exact matches for numbers, dates, timestamps, measurements, and metrics
           between the two outputs.
        4. The mapped column names might differ, do not make any assumptions based on them.

        FINAL QUESTION: Does OUTPUT #2 provide the same information as OUTPUT #1?
        FINAL ANSWER: Choose ONLY ONE
        - INFORMATION_MATCHES -- OUTPUT #1 and OUTPUT #2 provide the same information.
        - MISSING_INFORMATION -- Something important is missing from OUTPUT #2.
        - EXTRA_INFORMATION -- Some non-harmful extra relevant columns were added to OUTPUT #2.
        - INCORRECT_INFORMATION -- Some incorrect information was added to OUTPUT #2, likely due to
          an incorrect filter or incorrect aggregation.
        """

        logging.debug("\n --------- prompt:   --------- \n %s ", prompt)
        response = self.model.generate_content(
            prompt, generation_config=self.generation_config
        ).text

        logging.debug("\n --------- llm_rater_output:   --------- \n %s ", response)
        score = (
            100
            if ("INFORMATION_MATCHES" in response or "EXTRA_INFORMATION" in response)
            else 0
        )
        return score, response
