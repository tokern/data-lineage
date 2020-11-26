from abc import ABC, abstractmethod


class Source(ABC):
    @abstractmethod
    def read(self):
        pass


class FileSource(Source):
    def __init__(self, path):
        super().__init__()
        self.path = path
        self.queries = None

    @property
    def name(self):
        return self.path

    def read(self):
        import json

        with open(self.path, "r") as file:
            return json.load(file)
