class Query:
    def __init__(self, sql=None):
        self._sql = sql

    @property
    def sql(self):
        return self._sql

    @staticmethod
    def get_queries(source):
        return [Query(q) for q in source.read()]
