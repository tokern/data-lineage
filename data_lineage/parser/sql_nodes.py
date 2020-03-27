class SqlNode:
    def __init__(self, parent):
        self._parent = parent

    def accept(self, visitor):
        visitor.visit(self)


class AArrayExpr(SqlNode):
    pass


class AConst(SqlNode):
    pass


class AExpr(SqlNode):
    pass


class AIndices(SqlNode):
    pass


class AIndirection(SqlNode):
    pass


class AStar(SqlNode):
    pass


class Alias(SqlNode):
    pass


class BitString(SqlNode):
    pass


class BoolExpr(SqlNode):
    pass


class BooleanTest(SqlNode):
    pass


class CaseExpr(SqlNode):
    pass


class CaseWhen(SqlNode):
    pass


class CoalesceExpr(SqlNode):
    pass


class CollateClause(SqlNode):
    pass


class ColumnRef(SqlNode):
    pass


class CommonTableExpr(SqlNode):
    pass


class ConstraintsSetStmt(SqlNode):
    pass


class DeleteStmt(SqlNode):
    pass


class Float(SqlNode):
    pass


class FuncCall(SqlNode):
    pass


class GroupingSet(SqlNode):
    pass


class IndexElem(SqlNode):
    pass


class InferClause(SqlNode):
    pass


class Integer(SqlNode):
    pass


class InsertStmt(SqlNode):
    pass


class JoinExpr(SqlNode):
    pass


class LockingClause(SqlNode):
    pass


class MinMaxExpr(SqlNode):
    pass


class MultiAssignRef(SqlNode):
    pass


class NamedArgExpr(SqlNode):
    pass


class Null(SqlNode):
    pass


class NullTest(SqlNode):
    pass


class ParamRef(SqlNode):
    pass


class OnConflictClause(SqlNode):
    pass


class RangeFunction(SqlNode):
    pass


class RangeSubselect(SqlNode):
    pass


class RangeVar(SqlNode):
    pass


class RawStmt(SqlNode):
    pass


class ResTarget(SqlNode):
    pass


class RowExpr(SqlNode):
    pass


class SelectStmt(SqlNode):
    pass


class SetToDefault(SqlNode):
    pass


class SortBy(SqlNode):
    pass


class SQLValueFunction(SqlNode):
    pass


class String(SqlNode):
    pass


class SubLink(SqlNode):
    pass


class TransactionStmt(SqlNode):
    pass


class TruncateStmt(SqlNode):
    pass


class TypeCast(SqlNode):
    pass


class TypeName(SqlNode):
    pass


class UpdateStmt(SqlNode):
    pass


class Values(SqlNode):
    def __init__(self, values_list, **options):
        super(Values).__init__(**options)
        self._valuesList = values_list

    @property
    def values_list(self):
        return self._values_list


class VariableSetStmt(SqlNode):
    pass


class WindowDef(SqlNode):
    pass


class WithClause(SqlNode):
    def __init__(self, recursive, **options):
        super(WithClause).__init__(**options)
        self._recursive = recursive

    @property
    def recursive(self):
        return self._recursive
