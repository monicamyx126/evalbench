from google.protobuf.json_format import MessageToDict
import copy

try:
    from evalproto import eval_request_pb2

    PROTO_IMPORTED = True
except ImportError:
    PROTO_IMPORTED = False


class EvalInputRequest:

    def __init__(
        self,
        id: str,
        query_type: str,
        database: str,
        nl_prompt: str,
        dialects: list,
        golden_sql: dict | list,
        eval_query: dict | list,
        setup_sql: dict | list,
        cleanup_sql: dict | list,
        tags: list,
        other: dict,
        sql_generator_error: str = "",
        sql_generator_time: float = 0.0,
        generated_sql: str = "",
        job_id: str = "",
        trace_id: str = "",
    ):
        """Initializes an EvalInputRequest object with all required fields.

        See eval_request_pb2 for types
        """
        self.id = id
        self.database = database
        self.query_type = query_type
        self.nl_prompt = nl_prompt
        self.dialects = dialects
        self.golden_sql = golden_sql
        self.eval_query = eval_query
        self.setup_sql = setup_sql
        self.cleanup_sql = cleanup_sql
        self.tags = tags
        self.other = other
        self.sql_generator_error = sql_generator_error
        self.sql_generator_time = sql_generator_time
        self.generated_sql = generated_sql
        self.job_id = job_id
        self.trace_id = trace_id

    if PROTO_IMPORTED:

        @classmethod
        def init_from_proto(self, proto: eval_request_pb2.EvalInputRequest):
            """Initializes an EvalInputRequest from eval_request_pb2 proto."""

            request = MessageToDict(proto)
            return self(
                id=request.get("id"),
                query_type=request.get("queryType"),
                database=request.get("database"),
                nl_prompt=request.get("nlPrompt"),
                dialects=request.get("dialects"),
                golden_sql=request.get("goldenSql"),
                eval_query=request.get("evalQuery"),
                setup_sql=request.get("setupSql"),
                cleanup_sql=request.get("cleanupSql"),
                tags=request.get("tags"),
                other=request.get("other"),
                sql_generator_error=request.get("sqlGeneratorError"),
                sql_generator_time=request.get("sqlGeneratorTime"),
                generated_sql=request.get("generatedSql"),
                job_id=request.get("jobId"),
                trace_id=request.get("traceId"),
            )

    else:

        @classmethod
        def init_from_proto(cls, proto):
            raise ImportError("protobuf module not available")

    def copy(self):
        return copy.deepcopy(self)

    def copy_for_dialect(self, dialect: str):
        copy = self.copy()
        copy.dialects = [dialect]
        if isinstance(self.golden_sql, dict):
            copy.golden_sql = self.golden_sql.get(dialect, [])
        if isinstance(self.eval_query, dict):
            copy.eval_query = self.eval_query.get(dialect, [])
        if isinstance(self.setup_sql, dict):
            copy.setup_sql = self.setup_sql.get(dialect, [])
        if isinstance(self.cleanup_sql, dict):
            copy.cleanup_sql = self.cleanup_sql.get(dialect, [])
        return copy
