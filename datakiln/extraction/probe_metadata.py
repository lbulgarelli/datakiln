"""Collect source indexes and estimated plans without session views."""
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    Integer,
    String,
    bindparam,
    column,
    inspect,
    select,
    text,
)
from sqlalchemy import table as sql_table
from sqlalchemy.sql import Select
from sqlalchemy.sql.functions import count as sql_count
from sqlalchemy.sql.selectable import CompoundSelect

from datakiln.extraction.probes import StatementProbe
from datakiln.extraction.rewrites import disjoint_union_all

PLAN_NUMERIC = ('id', 'parent_id', 'cardinality', 'cost', 'bytes')
PLAN_TEXT = ('operation', 'options', 'object_owner', 'object_name')


def _plan_query(identifier: str) -> Select:
    """Select structural columns for one uniquely named estimated plan.

    :param identifier: Generated statement identifier in PLAN_TABLE.
    :type identifier: str
    :return: Statement that excludes SQL, predicates and bind values.
    :rtype: sqlalchemy.sql.Select
    """
    columns = []
    for name in PLAN_NUMERIC:
        columns.append(column(name, Integer))
    for name in PLAN_TEXT:
        columns.append(column(name, String))
    plan = sql_table('plan_table', column('statement_id', String), *columns)
    return select(*columns).where(
        plan.c.statement_id == identifier).order_by(plan.c.id)


class MetadataProbe(StatementProbe):  # pylint: disable=too-few-public-methods
    """Record column types and ordinary index metadata of the source table."""

    def _execute(self, connection: Any, statement: Select | CompoundSelect,
                 record: dict[str, Any]) -> None:
        """Reflect only metadata, excluding defaults, comments and values."""
        del statement
        record['phase'] = 'metadata'
        record['dialect'] = connection.dialect.name
        record['driver'] = connection.dialect.driver
        record['server_version'] = connection.dialect.server_version_info
        driver = connection.connection.driver_connection
        record['driver_thin_mode'] = getattr(driver, 'thin', None)
        inspector = inspect(connection)
        schema, name = record['table'].split('.', 1)
        schema = record.get('object_schema', schema)
        name = record.get('object_name', name)

        # Types help identify conversions; indexes retain their column order.
        record['columns'] = []
        for col in inspector.get_columns(name, schema=schema):
            record['columns'].append({
                'name': col['name'], 'type': str(col['type']),
                'nullable': col.get('nullable')})
        record['indexes'] = []
        for index in inspector.get_indexes(name, schema=schema):
            record['indexes'].append({
                'name': index['name'],
                'columns': index.get('column_names', []),
                'unique': index.get('unique', False)})
        if record['kind'] == 'cohort_metadata':
            # Count only the bounded staging input, never the source table.
            query = select(sql_count()).select_from(
                sql_table(name, schema=schema))
            with connection.execute(query) as result:
                record['cohort_rows'] = result.scalar_one()
        record['status'] = 'complete'


class PlanProbe(StatementProbe):  # pylint: disable=too-few-public-methods
    """Read an estimated plan through PLAN_TABLE without querying V$ views."""

    def _execute(self, connection: Any, statement: Select | CompoundSelect,
                 record: dict[str, Any]) -> None:
        """Explain a bound statement and retain structural plan columns."""
        record['phase'] = 'explain'
        compiled = statement.compile(dialect=connection.dialect)
        identifier = f'diag_{uuid4().hex[:24]}'
        explained = text(
            f"EXPLAIN PLAN SET STATEMENT_ID = '{identifier}' FOR {compiled}")

        # Preserve the original bind types, including JSON CLOB parameters.
        for original, name in compiled.bind_names.items():
            explained = explained.bindparams(bindparam(
                name, value=compiled.params[name], type_=original.type))
        with connection.execute(explained.execution_options(
                skip_formatting=True)):
            pass

        # Exclude predicate text, SQL text and peeked binds from the report.
        with connection.execute(_plan_query(identifier)) as result:
            rows = result.mappings().fetchmany(1001)
        record['plan_steps'] = []
        for row in rows[:1000]:
            step = {}
            for name in PLAN_NUMERIC:
                step[name] = int(row[name]) if row[name] is not None else None
            for name in PLAN_TEXT:
                step[name] = row[name]
            record['plan_steps'].append(step)
        record['status'] = 'row_limit' if len(rows) > 1000 else 'complete'
        # No commit: closing the probe rolls back its PLAN_TABLE inserts.


def run_extraction(probe: StatementProbe, statement: Select,
                   labels: dict[str, Any]) -> None:
    """Compare original and disjoint queries, optionally explaining each.

    :param probe: Execution probe and its settings.
    :type probe: datakiln.extraction.probes.StatementProbe
    :param statement: Extraction or isolated predicate query.
    :type statement: sqlalchemy.sql.Select
    :param labels: Strategy and comparison labels.
    :type labels: dict[str, typing.Any]
    """
    comparisons = [(statement, labels)]
    if labels['kind'] == 'extract':
        rewritten = disjoint_union_all(statement)
        if rewritten is not None:
            comparisons.append((rewritten, dict(
                labels, kind='extract_union_all',
                rewrite='disjoint_union_all')))

    # Keep the baseline first, and use identical budgets and input seeds.
    for query, query_labels in comparisons:
        if probe.options.explain:
            plan = PlanProbe(probe.engine, probe.options)
            plan.run(query, dict(query_labels, kind='plan',
                                 query_kind=query_labels['kind']))
        probe.run(query, query_labels)
