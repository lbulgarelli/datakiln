"""Build cohort-filtered queries that preserve duplicate source rows."""
import json
import re
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import (
    MetaData,
    String,
    Table,
    and_,
    column,
    false,
    literal_column,
    or_,
    select,
    text,
    union_all,
    values,
)
from sqlalchemy import join as sa_join
from sqlalchemy.engine import Dialect
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.selectable import CTE, FromClause
from sqlalchemy.types import TypeEngine

from datakiln.extraction.cohorts import SQLCohort
from datakiln.extraction.models import ColumnPair, FilterConfig, get_table


class CohortFilters:
    """Apply AND/OR cohort filters without multiplying source rows.

    Each filter is an EXISTS over its reference, so a source row is kept or
    dropped once, however many reference rows match it, and identical
    source rows keep their own result. The source table is scanned
    without numbering or sorting its rows. Each OR group is a statement of
    its own, a plain EXISTS that the database can plan as a semi-join.
    A row that matches several groups is returned by each of them, and the
    export writes it once.
    """

    def __init__(self, statement: Select):
        """Keep the unfiltered statement that each group narrows.

        :param statement: Unfiltered SELECT with explicit source columns.
        :type statement: sqlalchemy.sql.Select
        """
        self.statement = statement

        # Cache database reference projections within this statement.
        # Seed references keep their existing cache in SeedContext.
        self.references = {}

    def match(
        self,
        reference: FromClause | None,
        conditions: list[ClauseElement]
    ) -> ClauseElement:
        """Build the condition that a source row has a matching reference.

        :param reference: Reference rows to look for; None for constant
                          filters without a reference table.
        :type reference: sqlalchemy.sql.selectable.FromClause, optional

        :param conditions: Complete conditions for matching a reference row.
        :type conditions: list[sqlalchemy.sql.elements.ClauseElement]

        :return: Condition that holds when the filter matches.
        :rtype: sqlalchemy.sql.elements.ClauseElement
        """
        # Constant filters have no reference rows to look for.
        if reference is None:
            return and_(*conditions)

        # The conditions name the source alias, so the subquery reads the
        # source columns of the row being tested.
        subquery = select(literal_column('1')).select_from(reference)
        return subquery.where(and_(*conditions)).exists()

    def build(self, groups: list[list[ClauseElement]]) -> list[Select]:
        """Build one statement for each OR group of filters.

        :param groups: Match conditions combined with AND within each group
                       and OR between groups.
        :type groups: list[list[sqlalchemy.sql.elements.ClauseElement]]

        :return: SELECT of the rows of each group, including separate rows
                 whose values are identical. A row that matches several
                 groups is in the result of each of them.
        :rtype: list[sqlalchemy.sql.Select]
        """
        # Without filters every source row is returned.
        if not groups:
            return [self.statement]

        # The OR of the groups is not one statement: the database cannot
        # plan an OR of EXISTS as semi-joins, nor the exclusion of the rows
        # of the earlier groups from a later one. Each group is a statement
        # of its own.
        statements = []
        for group in groups:
            statements.append(self.statement.where(and_(*group)))
        return statements


class FilterBuilder:
    """Construct constant comparisons shared by both execution strategies."""

    def __init__(self, config: FilterConfig):
        """Keep the metadata for one filter.

        :param config: Configured reference, comparisons and join chain.
        :type config: datakiln.extraction.models.FilterConfig
        """
        self.config = config

    def conditions(self) -> list[ClauseElement]:
        """Build comparisons using the configured SQL expressions.

        :return: Conditions combined with AND for this filter.
        :rtype: list[sqlalchemy.sql.elements.ClauseElement]
        """
        conditions = []
        for col, operator, reference in zip(
            self.config.columns,
            self.config.reference.operators,
            self.config.reference.columns
        ):
            conditions.append(text(f'{col} {operator} {reference}'))
        return conditions

    def reference_expression(
        self,
        expression: str,
        relation: FromClause,
        dialect: Dialect
    ) -> str:
        """Point reference fields in an expression at their CTE columns.

        :param expression: Configured SQL column or expression.
        :type expression: str

        :param relation: CTE exposing qualified reference-column names.
        :type relation: sqlalchemy.sql.selectable.FromClause

        :param dialect: Database dialect used to quote those column names.
        :type dialect: sqlalchemy.engine.Dialect

        :return: Expression using the named reference CTE.
        :rtype: str
        """
        # Replace complete qualified names while retaining expressions and
        # letting SQLAlchemy quote each CTE column for the source database.
        for name in self.config.reference_columns:
            pattern = rf'(?<![\w.\:]){re.escape(name)}(?![\w.])'
            reference = str(relation.c[name].compile(dialect=dialect))
            expression = re.sub(pattern, reference, expression)
        return expression

    def build(self, queries: CohortFilters) -> ClauseElement:
        """Attach a constant filter without a reference table.

        :param queries: Builder that attaches the common filter join.
        :type queries: datakiln.extraction.filters.CohortFilters

        :return: Condition indicating whether the filter matched.
        :rtype: sqlalchemy.sql.elements.ClauseElement
        """
        return queries.match(None, self.conditions())


class DatabaseFilter(FilterBuilder):
    """Construct comparisons against a database table or SQL cohort query."""

    def __init__(
        self,
        config: FilterConfig,
        cohorts: dict[str, str],
        dialect: Dialect
    ):
        """Keep the filter metadata and resolved cohort references.

        :param config: Configured reference, comparisons and join chain.
        :type config: datakiln.extraction.models.FilterConfig

        :param cohorts: Cohort names mapped to tables or SQL subqueries.
        :type cohorts: dict[str, str]

        :param dialect: Database dialect used to quote reference columns.
        :type dialect: sqlalchemy.engine.Dialect
        """
        super().__init__(config)
        self.cohorts = cohorts
        self.dialect = dialect

    def sql_reference(
        self,
        resolved: str,
        queries: CohortFilters
    ) -> FromClause:
        """Reuse a SQL cohort with its CTEs registered at the statement level.

        :param resolved: Parenthesized cohort SELECT and its trailing alias.
        :type resolved: str

        :param queries: Statement builder holding shared reference CTEs.
        :type queries: datakiln.extraction.filters.CohortFilters

        :return: Cohort CTE under this filter's configured reference alias.
        :rtype: sqlalchemy.sql.selectable.FromClause
        """
        # Different projections of one cohort must share its SQL result,
        # including any randomized sample defined by the cohort itself.
        query = resolved.strip()[1:].rsplit(')', 1)[0]
        key = ('sql_cohort', self.config.resolved_cohort, query)
        if key not in queries.references:
            name = f'cohort_sql_{len(queries.references) + 1}'
            queries.references[key] = SQLCohort(
                query, name, self.dialect
            ).build()
        return FromClause.alias(
            queries.references[key], name=self.config.reference.alias
        )

    def reference_table(self, queries: CohortFilters) -> CTE:
        """Build or reuse a projection of the complete reference join.

        :param queries: Statement builder holding database reference CTEs.
        :type queries: datakiln.extraction.filters.CohortFilters

        :return: CTE with the qualified reference fields.
        :rtype: sqlalchemy.sql.selectable.CTE
        """
        # Find where the reference rows come from: a staged table, a SQL
        # cohort, or the database's own table when nothing maps it.
        resolved = self.cohorts.get(
            self.config.resolved_cohort, self.config.cohort
        )
        source = resolved or self.config.reference.full_name

        # Share projections only when the source, alias and required
        # reference fields are identical.
        key = (
            source,
            self.config.reference.alias,
            tuple(self.config.reference_columns)
        )

        # The join chain is part of the projection, so each join is part of
        # the key too.
        for join in self.config.join:
            # The conditions of this join, by kind and columns.
            pairs = []
            for pair in join.pairs:
                pairs.append((type(pair).__name__, pair.left, pair.right))

            # Two joins match only with the same table, aliases and pairs.
            join_key = (
                join.to_join(self.cohorts),
                join.reference.alias,
                join.reference_alias,
                tuple(pairs)
            )
            key += (join_key,)

        # Reuse the projection already built for this key.
        if key in queries.references:
            return queries.references[key]

        # Resolve the root cohort to its staged table or SQL subquery.
        if resolved:
            if resolved.strip().startswith('('):
                reference = self.sql_reference(resolved, queries)
            else:
                reference = get_table(resolved, self.config.reference.alias)
        else:
            reference = Table(
                self.config.reference.table,
                MetaData(),
                schema=self.config.reference.schema
            ).alias(self.config.reference.alias)

        # Keep join-chain conditions inside the reference projection so
        # comparisons retain fields from the same complete reference row.
        for join in self.config.join:
            join_name = join.to_join(self.cohorts)
            join_table = get_table(join_name, join.reference.alias)
            reference = sa_join(reference, join_table, join.to_onclause())

        # Qualified field names distinguish columns from different joined
        # tables.
        columns = []
        for name in self.config.reference_columns:
            columns.append(literal_column(name).label(name))

        # Keep the projection under its key, numbered within this statement,
        # for the filters that share it.
        queries.references[key] = select(*columns).select_from(reference).cte(
            f'cohort_reference_{len(queries.references) + 1}'
        )

        # Return the projection now stored for this key.
        return queries.references[key]

    def build(self, queries: CohortFilters) -> ClauseElement:
        """Look for the reference rows that satisfy the filter conditions.

        :param queries: Builder that attaches the common filter join.
        :type queries: datakiln.extraction.filters.CohortFilters

        :return: Condition that holds when a reference row matches.
        :rtype: sqlalchemy.sql.elements.ClauseElement
        """
        # Use the same reference CTE for filters with identical projections.
        reference = self.reference_table(queries)
        conditions = []
        for col, operator, ref_col in zip(
            self.config.columns,
            self.config.reference.operators,
            self.config.reference.columns
        ):
            col = self.reference_expression(col, reference, self.dialect)
            ref_col = self.reference_expression(
                ref_col, reference, self.dialect
            )
            conditions.append(text(f'{col} {operator} {ref_col}'))
        return queries.match(reference, conditions)


@dataclass
class SeedContext:
    """Share seed settings and cached CTEs within one extraction query.

    Each context starts with an empty cache for its query's seed CTEs.

    :param dialect: Database dialect for seed types and column quoting.
    :type dialect: sqlalchemy.engine.Dialect

    :param chunk_size: Maximum seeds per JSON parameter; None or 0 uses all.
    :type chunk_size: int, optional
    """

    dialect: Dialect
    chunk_size: int | None = None
    ctes: dict[tuple, CTE] = field(default_factory=dict, init=False)


class SeedFilter(FilterBuilder):
    """Construct typed seed comparisons, including NULL wildcards."""

    def __init__(
        self,
        config: FilterConfig,
        rows: list[tuple],
        types: dict[str, TypeEngine],
        context: SeedContext
    ):
        """Keep the loaded cohort values and their selected SQL types.

        :param config: Configured reference, comparisons and join chain.
        :type config: datakiln.extraction.models.FilterConfig

        :param rows: Seed tuples in reference-column order.
        :type rows: list[tuple]

        :param types: Qualified reference names and SQL types, in row order.
        :type types: dict[str, sqlalchemy.types.TypeEngine]

        :param context: Seed settings and CTE cache for this query.
        :type context: datakiln.extraction.filters.SeedContext
        """
        super().__init__(config)
        self.rows = rows
        self.types = types
        self.context = context

    def seed_table(self) -> CTE:
        """Build a typed seed CTE or reuse it from this query's seed cache."""
        # Share a CTE only when both the JSON values and SQL column types
        # match. Casting the same values to different types can change
        # which source rows satisfy a filter.
        key = (json.dumps(self.rows, default=str),)
        for name, kind in self.types.items():
            key += (name, kind.compile(dialect=self.context.dialect))

        # Reuse the cached seed CTE when these values and types match.
        if key in self.context.ctes:
            return self.context.ctes[key]

        # Preserve qualified reference names, such as ref.order_proc_id.
        columns = []
        for name, kind in self.types.items():
            columns.append(column(name, kind))

        # Split the seeds into separate JSON parameters to respect
        # the configured chunk size.
        chunks = []
        size = self.context.chunk_size or len(self.rows)
        for offset in range(0, len(self.rows), size):
            chunk_data = self.rows[offset:offset + size]
            chunk = values(*columns, name='seed').data(chunk_data)
            chunks.append(select(chunk))

        # Combine all chunks in a CTE and cache it for other filters.
        cohort_seed = f'cohort_seed_{len(self.context.ctes) + 1}'
        self.context.ctes[key] = union_all(*chunks).cte(cohort_seed)

        return self.context.ctes[key]

    @staticmethod
    def candidate_types(
        config: FilterConfig,
        table_types: dict[str, Any],
        cohort_types: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Resolve candidate SQL types by seed-column name.

        :param config: Filter metadata describing the seed comparisons.
        :type config: datakiln.extraction.models.FilterConfig

        :param table_types: Source column types by lower-case name.
        :type table_types: dict[str, sqlalchemy.types.TypeEngine]

        :param cohort_types: Reference types by qualified lower-case name.
        :type cohort_types: dict[str, sqlalchemy.types.TypeEngine]

        :return: Source-preferred and reference-preferred seed types.
        :rtype: tuple[dict[str, TypeEngine], dict[str, TypeEngine]]
        """
        source_types = {}
        reference_types = {}
        for reference in config.reference_columns:
            # Default to the reference column's metadata, or a string when
            # its type is unknown. Both candidates start with this type.
            reference_type = cohort_types.get(reference.lower(), String())
            source_types[reference] = reference_type
            reference_types[reference] = reference_type

            # Find the first comparison whose right side is this column.
            # Also consider columns inside expressions when their metadata
            # is missing; otherwise they keep their own reference type.
            comparison = None
            for source_expression, reference_expression in zip(
                config.columns, config.reference.columns
            ):
                # Use this comparison when its right side is exactly the
                # seed column, such as src.id = ref.id for ref.id.
                if reference_expression == reference:
                    comparison = (source_expression, reference_expression)
                    break

                # For a column inside an expression, keep its known type
                # and continue looking for a direct comparison.
                if reference.lower() in cohort_types:
                    continue

                # When its type is unknown, also allow a comparison that
                # uses the seed column inside an expression on either side.
                expression_columns = ColumnPair.extract_columns(
                    f'{source_expression} {reference_expression}'
                )
                if reference in expression_columns:
                    comparison = (source_expression, reference_expression)
                    break

            # Without a comparison to use, retain the reference type.
            if comparison is None:
                continue
            source_expression, reference_expression = comparison

            # Use the first known source column as the source candidate.
            # An expression's result may differ from that column's type,
            # so retain the reference candidate for the database probe.
            for source in ColumnPair.extract_columns(
                source_expression.lower()
            ):
                alias, name = source.split('.', 1)
                if alias != 'src' or name not in table_types:
                    continue
                source_types[reference] = table_types[name]

                # For a plain column-to-column comparison, use the source
                # type in both candidates to avoid converting source rows.
                if (source_expression.lower() == source
                        and reference_expression == reference):
                    reference_types[reference] = table_types[name]
                break

        return source_types, reference_types

    def build(self, queries: CohortFilters) -> ClauseElement:
        """Prepare the typed seeds and comparisons, then attach their join.

        :param queries: Statement builder that attaches the filter join.
        :type queries: datakiln.extraction.filters.CohortFilters

        :return: Condition indicating whether the seed rows match.
        :rtype: sqlalchemy.sql.elements.ClauseElement
        """
        # Without any seed, nothing matches
        if not self.rows:
            return queries.match(None, [false()])

        # The seeds are the rows of one table
        seeds = self.seed_table()

        # NULL seed values keep their wildcard behavior for the comparisons
        # that use those columns.
        nullable = set()
        for row in self.rows:
            for name, value in zip(self.types, row):
                if value is None:
                    nullable.add(name)

        # Build each comparison using the named seed columns.
        conditions = []
        for comparison in zip(
            self.config.columns,
            self.config.reference.operators,
            self.config.reference.columns
        ):
            condition = self._seed_condition(
                comparison, seeds, nullable
            )
            conditions.append(condition)

        return queries.match(seeds, conditions)

    def _seed_condition(
        self,
        comparison: tuple[str, str, str],
        seeds: FromClause,
        nullable: set[str]
    ) -> ClauseElement:
        """Build one comparison, retaining NULL seed wildcards.

        :param comparison: Source expression, operator and reference
                           expression.
        :type comparison: tuple[str, str, str]

        :param seeds: CTE containing named reference columns.
        :type seeds: sqlalchemy.sql.selectable.FromClause

        :param nullable: Seed columns containing NULL values.
        :type nullable: set[str]

        :return: Comparison including its NULL seed wildcards.
        :rtype: sqlalchemy.sql.elements.ClauseElement
        """
        # Resolve both operands against the reference CTE before applying
        # their configured comparison operator.
        col, operator, reference = comparison
        left_expression = self.reference_expression(
            col, seeds, self.context.dialect
        )
        right_expression = self.reference_expression(
            reference, seeds, self.context.dialect
        )
        left = literal_column(left_expression)
        right = literal_column(right_expression)
        condition = left.op(operator)(right)

        # A NULL acts as a wildcard only when that seed column is used by
        # this comparison, rather than another filter in the same group.
        references = ColumnPair.extract_columns(f'{col} {reference}')
        for reference in self.config.reference_columns:
            if reference in nullable and reference in references:
                condition = or_(seeds.c[reference].is_(None), condition)
        return condition
