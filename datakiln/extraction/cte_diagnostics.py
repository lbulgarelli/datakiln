"""Compare shared typed seed CTEs with EXISTS and explicit inner joins."""
import logging
import re
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    Date,
    DateTime,
    Integer,
    Numeric,
    and_,
    literal_column,
    select,
    union_all,
)
from sqlalchemy.sql import visitors

from datakiln.extraction.models import ExtractMeta, FilterConfig
from datakiln.extraction.probe_metadata import run_extraction
from datakiln.extraction.probes import StatementProbe, write_record
from datakiln.extraction.renderers import QueryDirector, SQLGenerator
from datakiln.extraction.rewrites import disjoint_union_all, not_matched
from datakiln.metadata.database import Session

if TYPE_CHECKING:
    from datakiln.extraction.diagnostics import FilterComparison

logger = logging.getLogger(__name__)


def _distinct_safe(types: list[Any]) -> bool:
    """Recognize keys whose equality does not depend on string collation."""
    return all(isinstance(kind, (Numeric, Integer, Date, DateTime))
               for kind in types)


def replace_relation(expression: Any, original: Any, replacement: Any) -> Any:
    """Replace a seed relation and its columns using SQLAlchemy traversal.

    :param expression: Existing seed predicate or join condition.
    :type expression: sqlalchemy.sql.elements.ClauseElement
    :param original: Seed row source referenced by the expression.
    :type original: sqlalchemy.sql.selectable.FromClause
    :param replacement: Shared CTE or one of its join aliases.
    :type replacement: sqlalchemy.sql.selectable.FromClause
    :return: Rebuilt expression without changing source conditions.
    :rtype: sqlalchemy.sql.elements.ClauseElement
    """
    substitutions = {original: replacement}
    for name, column in original.c.items():
        substitutions[column] = replacement.c[name]
    return visitors.replacement_traverse(expression, {}, substitutions.get)


def join_eligible(prepared: Any, seeds: list[tuple]) -> bool:
    """Check that a distinct typed seed tuple can match a source row once.

    :param prepared: Filter and resolved source types.
    :type prepared: datakiln.extraction.diagnostics.PreparedFilter
    :param seeds: Current sample of raw seed tuples.
    :type seeds: list[tuple]
    :return: Whether a plain inner join preserves source-row multiplicity.
    :rtype: bool
    """
    config = prepared.config
    if (not seeds or len(config.seed_positions) != len(config.columns)
            or set(config.seed_positions.values()) != set(
                range(len(prepared.types)))):
        return False

    # Numeric and date keys avoid collation differences between a source
    # string column and the JSON row source's DISTINCT operation.
    if not _distinct_safe(prepared.types):
        return False
    for row in seeds:
        if any(value is None for value in row):
            return False
    for column, operator in zip(config.columns, config.reference.operators):
        if operator != '=' or not re.fullmatch(r'src\.[\w$#]+', column):
            return False
    reference_pattern = rf'{re.escape(config.reference.alias)}\.[\w$#]+'
    return all(re.fullmatch(reference_pattern, reference)
               for reference in config.reference.columns)


class CteComparison:
    """Share identical typed seeds across predicates within one comparison."""

    def __init__(self, comparison: 'FilterComparison'):
        """Prepare shared CTE predicates for the current cohort sample.

        :param comparison: Existing read-only comparison and its sample.
        :type comparison: datakiln.extraction.diagnostics.FilterComparison
        """
        self.comparison = comparison
        self.relations = {}
        self.predicates = {}
        self.joinable = True
        self.shared = {}
        self.distinct_count = 0
        for identity, prepared in comparison.filters.items():
            seeds = prepared.sample(comparison.seed_count)
            self.joinable &= join_eligible(prepared, seeds)
            predicate = prepared.config.to_where(
                seeds, prepared.types, comparison.chunk_size)
            if not seeds:
                self.predicates[identity] = predicate
                continue

            # Equal reference names alone are insufficient: row values and
            # their resolved SQL types must both agree before sharing.
            key = (tuple(seeds), tuple(kind.compile(
                dialect=comparison.strategy.engine.dialect)
                for kind in prepared.types))
            original = predicate.element.element.get_final_froms()[0]
            if key not in self.shared:
                query = select(*original.c)
                # Keep string rows untouched: their DISTINCT collation may
                # differ from the source column's equality collation.
                if _distinct_safe(prepared.types):
                    query = query.distinct()
                    self.distinct_count += 1
                self.shared[key] = query.cte(
                    f'cohort_seed_{len(self.shared) + 1}')
            relation = self.shared[key]
            self.relations[identity] = relation
            self.predicates[identity] = replace_relation(
                predicate, original, relation)

    def process_filter(self, filter_config: FilterConfig, session: Session,
                       task: ExtractMeta) -> list[Any]:
        """Supply shared seed predicates to the existing query director.

        :param filter_config: Filter being rendered.
        :type filter_config: datakiln.extraction.models.FilterConfig
        :param session: Active query-building session.
        :type session: datakiln.metadata.database.Session
        :param task: Extraction target.
        :type task: datakiln.extraction.models.ExtractMeta
        :return: Shared predicate or the unchanged non-seed filter.
        :rtype: list[typing.Any]
        """
        if id(filter_config) in self.predicates:
            return [self.predicates[id(filter_config)]]
        return self.comparison.process_filter(filter_config, session, task)

    def run(self, probe: StatementProbe, labels: dict[str, Any]) -> None:
        """Compare CTE-backed EXISTS with duplicate-safe explicit joins.

        :param probe: Runner that provides identical limits for each query.
        :type probe: datakiln.extraction.probes.StatementProbe
        :param labels: Existing strategy, sample and repetition metadata.
        :type labels: dict[str, typing.Any]
        """
        labels = dict(labels, shared_seed_ctes=len(self.shared),
                      distinct_seed_ctes=self.distinct_count,
                      seed_relation='shared_typed_cte',
                      cte_materialization='optimizer')
        with self.comparison.strategy.session() as session:
            exists_query = QueryDirector.build_queries(
                session, self.comparison.task, strategy=self)
            # Splitting OR groups keeps this comparable with the fast
            # existing UNION ALL probe rather than reintroducing slow ORs.
            rewritten = disjoint_union_all(exists_query)
            run_extraction(probe, exists_query if rewritten is None
                           else rewritten,
                           dict(labels, kind='extract_cte_exists'))
            if self.joinable:
                run_extraction(probe, self._join_query(session),
                               dict(labels, kind='extract_cte_join'))
            else:
                reason = ('Requires non-null direct numeric/date equality '
                          'keys covering the complete seed tuple.')
                write_record(probe.options.output, dict(
                    labels, event='skip', kind='extract_cte_join',
                    status='unsupported', reason=reason))
                logger.info("Probe '%s': Skipping explicit CTE join (%s).",
                            labels['table'], reason)

    def _join_query(self, session: Session) -> Any:
        """Join unique keys per group and exclude earlier group matches."""
        task = self.comparison.task
        source = SQLGenerator.base_select(
            task.schema, task.table).get_final_froms()[0]
        branches = []
        earlier = []
        for group_index, group in enumerate(task.filters):
            if not group:
                continue
            joined, predicates, conditions = self._join_group(
                source, group, group_index, session)
            for predicate in earlier:
                conditions.append(not_matched(predicate))
            branches.append(select(literal_column('src.*')).select_from(
                joined).where(*conditions))
            earlier.append(and_(*predicates))
        return branches[0] if len(branches) == 1 else union_all(*branches)

    def _join_group(self, source: Any, group: list[FilterConfig],
                    group_index: int, session: Session) -> tuple:
        """Alias shared CTEs independently for each predicate in a group."""
        joined, predicates, conditions = source, [], []
        for filter_index, config in enumerate(group):
            predicate = self.process_filter(
                config, session, self.comparison.task)[0]
            predicates.append(predicate)
            relation = self.relations.get(id(config))
            if relation is None:
                conditions.append(predicate)
                continue
            alias = relation.alias(f'match_{group_index}_{filter_index}')
            condition = predicate.element.element.whereclause
            joined = joined.join(alias, replace_relation(
                condition, relation, alias))
        return joined, predicates, conditions
