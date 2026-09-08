"""
SQL rendering strategies for the extraction pipeline.
"""
import logging
from hashlib import md5
from threading import Lock
from typing import List, Optional, TYPE_CHECKING

from sqlalchemy import (
    select, inspect, Index, Table, MetaData, literal_column, or_
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import Select

from data_utils.metadata.database import Session
from data_utils.extraction.models import ExtractMeta

if TYPE_CHECKING:
    from data_utils.extraction.strategies import ExecutionStrategy


class SQLGenerator:
    """Base SQL generation utility providing column resolution and joins."""
    _lock = Lock()

    @staticmethod
    def has_index(
        session: Session,
        schema: Optional[str],
        table: str,
        columns: List[str]
    ) -> bool:
        """Evaluate the existence of an index with the specified columns."""
        insp = inspect(session.bind)
        for idx in insp.get_indexes(table, schema=schema):
            if idx["column_names"] == columns:
                return True
        return False

    @staticmethod
    def ensure_index(
        session: Session,
        strategy: 'ExecutionStrategy',
        table_parts: List[str],
        columns: List[str]
    ):
        """Create a covering index on the reference table if non-existent."""
        if strategy.is_read_only or not columns:
            return

        columns_hash = md5("_".join(columns).encode()).hexdigest()  # noqa: DUO130, E501
        schema = table_parts[0] if len(table_parts) == 2 else None
        tbl_name = table_parts[-1]

        with SQLGenerator._lock:
            if not SQLGenerator.has_index(session, schema, tbl_name, columns):
                try:
                    name = f"{tbl_name}_idx_{columns_hash}"
                    sa_table = Table(
                        tbl_name, MetaData(), schema=schema,
                        autoload_with=session.bind
                    )
                    sa_cols = [sa_table.c[c] for c in columns]
                    idx = Index(name, *sa_cols)
                    idx.create(bind=session.bind)
                except SQLAlchemyError as err:
                    logging.warning(
                        "FAILED TO CREATE INDEX ON '%s': %s",
                        ".".join(table_parts), err
                    )

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
        exists, filters = [], []
        for filter_config in task.filters:
            if strategy.is_read_only and not filter_config.is_sql_cohort:
                # Fetch seeds locally, push as chunked IN filter
                cohort_data = strategy.get_cohort_set(filter_config, task=task)
                filters.append((filter_config, cohort_data))
            else:
                # Ensure index
                actual_table = strategy.cohorts.get(
                    filter_config.resolved_cohort,
                    filter_config.resolved_cohort
                ).split(".")
                SQLGenerator.ensure_index(
                    session,
                    strategy,
                    actual_table,
                    filter_config.cohort_columns
                )

                # Build EXISTS subquery
                exists.append(filter_config.to_exists(strategy))

        # Build base statement with EXISTS clauses
        stmt = SQLGenerator.base_select(task.schema, task.table)
        if exists:
            stmt = stmt.where(or_(*exists))

        # If no filters, return the base statement
        if not filters:
            return [stmt]

        # Build chunked queries if there are filters
        return QueryDirector._chunked_queries(
            stmt,
            primary=filters[0],
            secondaries=filters[1:]
        )

    @staticmethod
    # pylint: disable=too-many-locals
    def _chunked_queries(
        base_stmt: Select,
        primary: tuple,
        secondaries: List[tuple],
        chunk_size: int = 1000
    ) -> List[Select]:
        """Decompose primary cohort sets into chunked SA Select statements.

        :param base_stmt: Base SA Select to extend with WHERE conditions.
        :type base_stmt: Select

        :param primary: (FilterConfig, values) tuple for the primary cohort.
        :type primary: tuple

        :param secondaries: Additional (FilterConfig, values) tuples.
        :type secondaries: list[tuple]

        :param chunk_size: Maximum values per chunk.
        :type chunk_size: int

        :return: List of SA Select statements, one per chunk.
        :rtype: list[Select]
        """
        queries = []
        primary_config, primary_values = primary
        for i in range(0, len(primary_values), chunk_size):
            chunk_values = primary_values[i:i + chunk_size]
            clauses = [primary_config.to_where(chunk_values)]

            for sec_config, sec_values in secondaries:
                clauses.append(sec_config.to_where(sec_values))

            stmt = base_stmt.where(or_(*clauses))
            queries.append(stmt)

        return queries or [base_stmt]
