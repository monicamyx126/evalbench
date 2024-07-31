from abc import ABC, abstractmethod


class PromptGenerator(ABC):

    def __init__(self, db_config, promptgenerator_config):
        pass

    @abstractmethod
    def generate(self, prompt):
        raise NotImplementedError("Subclasses must implement this method")
