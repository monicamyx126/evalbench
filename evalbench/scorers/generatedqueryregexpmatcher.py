"""
GeneratedQueryRegexpMatcher

This comparison strategy matches generated queries with regular expressions.
Golden query is not taken into account here.

This strategy is useful in cases where you want to verify whether generated queries satisfy/contain some specified patterns.

Run configuration options:
    1. regexp_string_list: Required
        - A list of regular expressions to be matched against the generated query
        - Accepted values: a list of strings containing regular expressions

    2. invert_results: Optional
        - Accpted values: True/False
        - Default value: False
        - When this is set to true, non-matched queries are scored as 100, and matched queries are scored as 0.

    3. match_all_patterns: Optional
        - Accepted values: True/False
        - Default value: False
        - When this is set to true, the query is given a score of 100 when it matches all the patterns in the list. When set to false, the query is scored to 100 when it matches atleast 1 pattern.

    4. match_whole_query: Optional
        - Accepted values: True/False
        - Default value: False
        - When this is set to true, you force the pattern to match the whole query, when set to false, a substring matching the given pattern is considered a match.
"""

import re
from typing import Tuple
from scorers import comparator


def clean_sql_query(query: str):
    # Remove any backticks from front and back
    query = query.strip("`")

    # Replace any whitespaces with a single space
    query = re.sub(r'\s+', ' ', query)

    # Remove extra whitespaces from front and back
    query = query.strip()
    return query


class GeneratedQueryRegexpMatcher(comparator.Comparator):
    """
    GeneratedQueryRegexpMatcher class implements the Comparator base class with regex matching logic.

    Attributes:
        1. name: Name of the comparator. Set to "regexp_matcher"
        2. config: Scorer config defined in the run config yaml file
        3. regexp_string_list: list of regular expressions to match
        4. invert_results: A boolean value which determines whether to invert the value of score
        5. match_all_patterns: A boolean value which determines whether to force matching of all regular expressions
        6. match_whole_query: A boolean valule which determines whether to force matching of the whole query
    """

    def __init__(self, config: dict):
        self.name = "regexp_matcher"
        self.config = config
        self.regexp_string_list = config["regexp_string_list"]
        self.invert_results = False
        self.match_all_patterns = False
        self.match_whole_query = False

        if "invert_results" in config:
            self.invert_results = config["invert_results"]
        if "match_all_patterns" in config:
            self.match_all_patterns = config["match_all_patterns"]
        if "match_whole_query" in config:
            self.match_whole_query = config["match_whole_query"]

    def compare(
        self,
        nl_prompt: str,
        golden_query: str,
        query_type: str,
        golden_execution_result: str,
        golden_eval_result: str,
        golden_error: str,
        generated_query: str,
        generated_execution_result: str,
        generated_eval_result: str,
        generated_error: str,
    ) -> Tuple[float, str]:

        """compare function implements the comparison logic for GeneratedQueryRegexpMatcher comparator."""

        cleaned_query = clean_sql_query(generated_query)
        score = 0
        matching_regexps = []
        match_count = 0
        for regexp_string in self.regexp_string_list:
            result = re.search(regexp_string, cleaned_query)
            if result:
                matching_regexps.append(result.group(0))
                if self.match_whole_query:
                    if result.group(0) == cleaned_query:
                        match_count = match_count + 1
                else:
                    match_count = match_count + 1

        if self.match_all_patterns:
            if match_count == len(self.regexp_string_list):
                score = 100
        else:
            if match_count > 0:
                score = 100

        if self.invert_results:
            score = 100 - score
        return score, str(matching_regexps)
