"""
SQL rendering strategies for the extraction pipeline.
"""
from threading import Lock
from typing import List, TYPE_CHECKING
import itertools

from sqlalchemy import (
    select, Table, MetaData, literal_column, and_
)
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ClauseElement

from data_utils.metadata.database import Session
from data_utils.extraction.models import ExtractMeta

if TYPE_CHECKING:
    from data_utils.extraction.strategies import ExecutionStrategy


class SQLGenerator:
    # pylint: disable=too-few-public-methods
    """Base SQL generation utility providing column resolution and joins."""
    _lock = Lock()

    @staticmethod
    def base_select(schema: str, table: str) -> Select:
        """Return a base SELECT * for a given schema-qualified table.

        :param schema: Database schema name.
        :type schema: str

        :param table: Table name.
        :type table: str

        :return: SA Select statement.
        :rtype: Select
        """
        src = Table(table, MetaData(), schema=schema).alias("src")
        return select(literal_column("*")).select_from(src)


class QueryDirector:
    # pylint: disable=too-few-public-methods
    """Orchestrates query construction across strategies and filters."""

    @staticmethod
    # pylint: disable=too-many-locals
    def build_queries(
        session: Session,
        task: ExtractMeta,
        strategy: 'ExecutionStrategy'
    ) -> List[Select]:
        """Build the complete list of runnable SA Select statements for a task.

        :param session: Active SQLAlchemy session.
        :type session: Session

        :param task: Extraction task metadata.
        :type task: ExtractMeta

        :param strategy: Execution strategy instance.
        :type strategy: ExecutionStrategy

        :return: List of SA Select statements ready for execution.
        :rtype: list[Select]
        """
        filters = []
        for filter_group in task.filters:
            group_filters = []

            for filter_config in filter_group:
                group_filters.append(strategy.process_filter(
                    filter_config, session, task
                ))
            filters.append(group_filters)

        # Build base statement
        stmt = SQLGenerator.base_select(task.schema, task.table)

        # If no filters, return the base statement
        if not filters:
            return [stmt]

        # Build chunked queries
        return QueryDirector._chunked_queries(
            stmt,
            groups=filters
        )

    @staticmethod
    def _chunked_queries(
        base_stmt: Select,
        groups: List[List[List[ClauseElement]]]
    ) -> List[Select]:
        """Decompose primary cohort sets into chunked SA Select statements.

        :param base_stmt: Base SA Select to extend with WHERE conditions.
        :type base_stmt: Select

        :param groups: A 3D list where the outermost list represents OR groups,
                       the middle list represents AND conditions within an OR
                       group, and the innermost list contains the chunked SA
                       ClauseElements for a specific condition.
        :type groups: List[List[List[ClauseElement]]]

        :return: List of SA Select statements, one per chunk combo.
        :rtype: list[Select]
        """
        queries = []

        for or_group in groups:
            # Iterate over the cartesian product of all filter chunks
            # in this OR group.
            for chunk_combo in itertools.product(*or_group):
                clauses = list(chunk_combo)

                if not clauses:
                    continue

                # Combine clauses since they are part of the same group
                stmt = base_stmt.where(
                    and_(*clauses) if len(clauses) > 1 else clauses[0]
                )
                queries.append(stmt)

        return queries or [base_stmt]
