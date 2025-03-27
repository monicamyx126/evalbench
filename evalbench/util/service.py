from dataset import evalinput


def load_session_configs(session):
    return (
        session["config"],
        session["db_configs"],
        session["model_config"],
        session["setup_config"],
    )


async def get_dataset_from_request(request_iterator):
    return [
        evalinput.EvalInputRequest.init_from_proto(request)
        async for request in request_iterator
    ]
