from abc import ABC, abstractmethod

from data_lineage.catalog.query import Query


class Source(ABC):
    @abstractmethod
    def get_queries(self):
        pass


class FileSource(Source):
    def __init__(self, path):
        super().__init__()
        self.path = path
        self.queries = None

    def _read(self):
        import json

        with open(self.path, "r") as file:
            return json.load(file)

    def get_queries(self):
        if self.queries is None:
            self.queries = [Query(q) for q in self._read()]

        return self.queries
