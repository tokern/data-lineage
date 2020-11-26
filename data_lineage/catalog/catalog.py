from abc import ABC

from data_lineage.log_mixin import LogMixin


class NamedObject(ABC, LogMixin):
    def __init__(self, name, parent=None):
        self._name = name
        self._parent = parent

    @property
    def name(self):
        return self._name

    @property
    def fqdn(self):
        if self._parent is None:
            return (self._name,)
        else:
            fq = list(self._parent.fqdn)
            fq.append(self._name)
            return tuple(fq)


class Namespace(NamedObject):
    def __init__(self, name, parent):
        super(Namespace, self).__init__(name, parent)
        self._children = []


class Database(Namespace):
    def __init__(self, name, schemata):
        super(Database, self).__init__(name, None)
        self._oid_object_map = {}

        for schema in schemata:
            self._children.append(Schema(**schema))

    @property
    def schemata(self):
        return self._children

    def get_object(self, oid):
        return self._oid_object_map[oid]

    def get_table(self, fqdn):
        schema, table = fqdn
        if schema is None:
            schema = "default"

        for s in self.schemata:
            if s.name == schema:
                for t in s.tables:
                    if t.name == table:
                        return t

        return None

    def get_column(self, fqdn):
        schema, table, column = fqdn
        if schema is None:
            schema = "default"

        for s in self.schemata:
            if s.name == schema:
                for t in s.tables:
                    if t.name == table:
                        for c in t.columns:
                            if c.name == column:
                                return c

        return None

    @staticmethod
    def get_database(source):
        return Database(source.name, source.read())


class Schema(Namespace):
    def __init__(self, name, tables):
        super(Schema, self).__init__(name, None)

        for table in tables:
            self._children.append(Table(parent=self, **table))

    @property
    def tables(self):
        return self._children


class Table(Namespace):
    def __init__(self, name, columns, parent):
        super(Table, self).__init__(name, parent)

        for column in columns:
            self._children.append(Column(parent=self, **column))

    @property
    def columns(self):
        return self._children


class Column(NamedObject):
    def __init__(self, parent, name, type):
        super(Column, self).__init__(name, parent)
        self._type = type

    @property
    def type(self):
        return self._type
