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


def not_matched(predicate: ClauseElement) -> ClauseElement:
    """Exclude TRUE matches while retaining FALSE and UNKNOWN.

    :param predicate: Predicate from an earlier query branch.
    :type predicate: sqlalchemy.sql.elements.ClauseElement
    :return: NULL-safe overlap exclusion.
    :rtype: sqlalchemy.sql.elements.ClauseElement
    """
    # Unwrap EXISTS so Oracle receives NOT EXISTS rather than '= 0'.
    while isinstance(predicate, Grouping):
        predicate = predicate.element
    if _two_valued(predicate):
        return not_(predicate)
    return case((predicate, 1), else_=0) == 0


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
            branch = branch.where(not_matched(earlier))
        branches.append(branch)
        previous.append(predicate)
    return union_all(*branches)
