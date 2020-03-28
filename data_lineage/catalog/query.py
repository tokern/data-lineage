class Query:
    def __init__(self, sql=None):
        self._sql = sql

    @property
    def sql(self):
        return self._sql
