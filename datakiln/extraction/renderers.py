"""
SQL rendering strategies for the extraction pipeline.
"""
from threading import Lock
from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    MetaData,
    Table,
    inspect,
    select,
)
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select
from sqlalchemy.sql.selectable import SelectBase

from datakiln.extraction.filters import CohortFilters, SeedContext
from datakiln.extraction.models import ExtractMeta

if TYPE_CHECKING:
    from datakiln.extraction.strategies import ExecutionStrategy


class SQLGenerator:
    # pylint: disable=too-few-public-methods
    """Base SQL generation utility providing column resolution and joins."""
    _lock = Lock()

    @staticmethod
    def base_select(
        schema: str | None,
        table: str,
        engine: Engine
    ) -> Select:
        """Select the source columns in their database order.

        :param schema: Database schema name.
        :type schema: str, optional

        :param table: Table name.
        :type table: str

        :param engine: Source engine used to read the column metadata.
        :type engine: sqlalchemy.engine.Engine

        :return: SA Select statement.
        :rtype: Select
        """
        # Read column names and types so window helpers can be excluded
        # from the output without grouping or deduplicating source values.
        columns = []
        for info in inspect(engine).get_columns(table, schema=schema):
            columns.append(Column(info["name"], info["type"]))
        src = Table(table, MetaData(), *columns, schema=schema).alias("src")
        return select(*src.c)


class QueryDirector:
    # pylint: disable=too-few-public-methods
    """Orchestrates query construction across strategies and filters."""

    @staticmethod
    def build_queries(
        task: ExtractMeta,
        strategy: 'ExecutionStrategy'
    ) -> SelectBase:
        """Build the runnable SA Select statement for a task.

        :param task: Extraction task metadata.
        :type task: ExtractMeta

        :param strategy: Execution strategy instance.
        :type strategy: ExecutionStrategy

        :return: SA Select statement ready for execution.
        :rtype: sqlalchemy.sql.selectable.SelectBase
        """
        # Attach all filter joins through one statement builder.
        stmt = SQLGenerator.base_select(
            task.schema, task.table, strategy.engine
        )
        queries = CohortFilters(stmt)

        # Each query has one seed context so filters can reuse seed CTEs.
        filters = []
        seeds = SeedContext(strategy.engine.dialect, task.chunk_size)
        for filter_group in task.filters:
            group_filters = []

            for filter_config in filter_group:
                group_filters.append(strategy.process_filter(
                    filter_config, task, queries, seeds
                ))
            filters.append(group_filters)

        # If no filters, return the base statement
        if not filters:
            return stmt

        # The joins are already attached; combine their match conditions.
        return queries.build(filters)
