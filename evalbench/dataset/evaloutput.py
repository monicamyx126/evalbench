from dataset.evalinput import EvalInput


class EvalOutput(dict):
    def __init__(
        self,
        evalinput: EvalInput,
    ):
        self.update(evalinput.__dict__)
