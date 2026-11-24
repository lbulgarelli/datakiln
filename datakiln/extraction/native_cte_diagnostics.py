"""Prepare staging-table keys for the shared CTE comparison runner."""
import re
from typing import Any

from sqlalchemy import (
    Column,
    MetaData,
    Table,
    and_,
    inspect,
    literal,
    literal_column,
    select,
)

from datakiln.extraction.cte_diagnostics import CteComparison, _distinct_safe
from datakiln.extraction.models import FilterConfig


class NativeCteComparison(CteComparison):
    """Use staged keys directly, without fetching or JSON encoding them."""

    def _prepare(self) -> None:
        """Reflect key types once and prepare supported staged predicates."""
        task = self.comparison.task
        inspector = inspect(self.comparison.strategy.engine)
        source_types = {}
        for column in inspector.get_columns(task.table, schema=task.schema):
            source_types[column['name']] = column['type']

        # Cache reflected relations so filters on one cohort share metadata.
        tables = {}
        for group in task.filters:
            for config in group:
                name = self.comparison.strategy.cohorts.get(
                    config.resolved_cohort)
                if not name or config.join or name.strip().startswith('('):
                    self.joinable = False
                    continue
                if name not in tables:
                    parts = name.rsplit('.', 1)
                    schema = parts[0] if len(parts) == 2 else None
                    columns = []
                    for column in inspector.get_columns(
                            parts[-1], schema=schema):
                        columns.append(Column(column['name'], column['type']))
                    tables[name] = Table(parts[-1], MetaData(), *columns,
                                         schema=schema)
                self._prepare_filter(config, tables[name], source_types)

    def _prepare_filter(self, config: FilterConfig, table: Any,
                        source_types: dict[str, Any]) -> None:
        """Share distinct numeric/date keys for direct equality predicates.

        :param config: Native filter configuration.
        :type config: datakiln.extraction.models.FilterConfig
        :param table: Reflected staging table.
        :type table: sqlalchemy.sql.schema.Table
        :param source_types: Target column types by name.
        :type source_types: dict[str, typing.Any]
        """
        names = []
        types = []
        for source, operator, reference in zip(
                config.columns, config.reference.operators,
                config.reference.columns):
            pattern = rf'{re.escape(config.reference.alias)}\.([\w$#]+)'
            match = re.fullmatch(pattern, reference)
            if (operator != '=' or not re.fullmatch(r'src\.[\w$#]+', source)
                    or match is None):
                self.joinable = False
                return
            names.append(match[1])
            types.extend((source_types[source[4:]], table.c[match[1]].type))

        # Numeric/date equality permits DISTINCT without collation changes.
        # NULL keys cannot match native equality, so need no wildcard logic.
        if not names or not _distinct_safe(types):
            self.joinable = False
            return
        key = (table.fullname, tuple(names))
        if key not in self.shared:
            self.shared[key] = select(
                *[table.c[name] for name in names]).distinct().cte(
                    f'cohort_seed_{len(self.shared) + 1}')
            self.distinct_count += 1
        relation = self.shared[key]

        # Structured columns allow the common runner to alias each join.
        conditions = []
        for source, name in zip(config.columns, names):
            conditions.append(literal_column(
                source, type_=source_types[source[4:]]) == relation.c[name])
        self.relations[id(config)] = relation
        self.predicates[id(config)] = select(literal(1)).select_from(
            relation).where(and_(*conditions)).exists()
