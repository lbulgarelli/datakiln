"""Alternative query forms used only by extraction diagnostics."""
from sqlalchemy import case, not_, union_all
from sqlalchemy.sql import Select, operators, visitors
from sqlalchemy.sql.elements import BooleanClauseList, ClauseElement, Grouping
from sqlalchemy.sql.selectable import CompoundSelect, Exists


def _two_valued(predicate: ClauseElement) -> bool:
    """Recognize EXISTS and combinations that cannot evaluate to UNKNOWN."""
    if isinstance(predicate, Grouping):
        return _two_valued(predicate.element)
    if isinstance(predicate, Exists):
        return True
    if isinstance(predicate, BooleanClauseList):
        return all(_two_valued(clause) for clause in predicate.clauses)
    return False


def disjoint_union_all(statement: Select) -> CompoundSelect | None:
    """Split a top-level OR into mutually exclusive UNION ALL branches.

    :param statement: Unbounded diagnostic extraction SELECT.
    :type statement: sqlalchemy.sql.Select
    :return: Equivalent branches, or None when there is no top-level OR.
    :rtype: sqlalchemy.sql.selectable.CompoundSelect, optional
    """
    condition = statement.whereclause
    if not (isinstance(condition, BooleanClauseList)
            and condition.operator is operators.or_):
        return None

    # Assign each physical source row to its first matching OR branch.
    # UNION ALL retains distinct source rows whose projected values coincide.
    branches = []
    previous = []
    for predicate in condition.clauses:
        branch = visitors.replacement_traverse(
            statement, {},
            lambda element, replacement=predicate:
            replacement if element is condition else None)

        # NOT alone would discard UNKNOWN as well as TRUE. CASE excludes
        # only previous matches, retaining rows where a predicate was NULL.
        for earlier in previous:
            # Negating Grouping(EXISTS) renders a boolean comparison on
            # Oracle; unwrap it so SQLAlchemy emits a real NOT EXISTS.
            while isinstance(earlier, Grouping):
                earlier = earlier.element
            exclusion = (not_(earlier) if _two_valued(earlier)
                         else case((earlier, 1), else_=0) == 0)
            branch = branch.where(exclusion)
        branches.append(branch)
        previous.append(predicate)
    return union_all(*branches)
