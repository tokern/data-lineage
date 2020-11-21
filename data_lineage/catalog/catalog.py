import uuid
from abc import ABC

from data_lineage.log_mixin import LogMixin


class NamedObject(ABC, LogMixin):
    def __init__(self, name):
        self._oid = uuid.uuid1().int
        self._name = name

    @property
    def oid(self):
        return self._oid

    @property
    def name(self):
        return self._name


class Namespace(NamedObject):
    def __init__(self, name):
        super(Namespace, self).__init__(name)
        self._children = []


class Database(Namespace):
    def __init__(self, name, schemata):
        super(Database, self).__init__(name)
        self._oid_object_map = {}

        for schema in schemata:
            self._children.append(Schema(**schema))

        for s in self.schemata:
            for t in s.tables:
                for c in t.columns:
                    self._oid_object_map[c.oid] = c
                self._oid_object_map[t.oid] = t
            self._oid_object_map[s.oid] = s

    @property
    def schemata(self):
        return self._children

    def get_object(self, oid):
        return self._oid_object_map[oid]

    def get_table_oid(self, schema, table):
        if schema is None:
            schema = "default"

        for s in self.schemata:
            if s.name == schema:
                for t in s.tables:
                    if t.name == table:
                        return t.oid

        return None

    def get_column_oid(self, schema, table, column):
        if schema is None:
            schema = "default"

        for s in self.schemata:
            if s.name == schema:
                for t in s.tables:
                    if t.name == table:
                        for c in t.columns:
                            if c.name == column:
                                return c.oid

        return None

    @staticmethod
    def get_database(source):
        return Database(source.name, source.read())


class Schema(Namespace):
    def __init__(self, name, tables):
        super(Schema, self).__init__(name)

        for table in tables:
            self._children.append(Table(**table))

    @property
    def tables(self):
        return self._children


class Table(Namespace):
    def __init__(self, name, columns):
        super(Table, self).__init__(name)

        for column in columns:
            self._children.append(Column(**column))

    @property
    def columns(self):
        return self._children


class Column(NamedObject):
    def __init__(self, name, type):
        super(Column, self).__init__(name)
        self._type = type

    @property
    def type(self):
        return self._type
