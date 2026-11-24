"""Compare native staging-table predicates without introducing JSON filters."""
from typing import Any

from sqlalchemy import literal, select

from datakiln.extraction.models import ExtractMeta
from datakiln.extraction.probe_metadata import MetadataProbe, run_extraction
from datakiln.extraction.probes import ProbeOptions, StatementProbe
from datakiln.extraction.renderers import SQLGenerator
from datakiln.extraction.strategies import TempTableStrategy


class NativeComparison:  # pylint: disable=too-few-public-methods
    """Measure each predicate and the original combination on staged data."""

    def __init__(self, strategy: TempTableStrategy, task: ExtractMeta):
        """Keep the native strategy and target whose cohorts already exist.

        :param strategy: Strategy that prepared source staging tables.
        :type strategy: datakiln.extraction.strategies.TempTableStrategy
        :param task: Extraction task to diagnose.
        :type task: datakiln.extraction.models.ExtractMeta
        """
        self.strategy = strategy
        self.task = task

    def run(self, options: ProbeOptions) -> None:
        """Probe the combined query and each predicate with the full cohort.

        :param options: Probe budgets and report destination.
        :type options: datakiln.extraction.probes.ProbeOptions
        """
        # Build through the native strategy, including its cohort indexes.
        statement = self.strategy.build_queries(self.task)
        probe = StatementProbe(self.strategy.engine, options)
        self._cohort_metadata(options)
        for repeat in range(1, options.repeats + 1):
            labels: dict[str, Any] = {
                'table': self.task.full_name, 'strategy': 'temptable',
                'variant': 'native', 'repeat': repeat}
            run_extraction(probe, statement, dict(labels, kind='extract'))

            # Isolation deliberately removes all other predicates, while
            # the combined query above retains the original AND/OR logic.
            for group_index, group in enumerate(self.task.filters):
                for filter_index, config in enumerate(group):
                    isolated = SQLGenerator.base_select(
                        self.task.schema, self.task.table).where(
                            config.to_exists(self.strategy))
                    run_extraction(probe, isolated, dict(
                        labels, kind='extract_filter',
                        filter_group=group_index, filter_index=filter_index))

    def _cohort_metadata(self, options: ProbeOptions) -> None:
        """Record indexes and types of the actual staged cohort tables."""
        seen = set()
        for group in self.task.filters:
            for config in group:
                resolved = self.strategy.cohorts.get(config.resolved_cohort)
                if not resolved or resolved in seen:
                    continue
                seen.add(resolved)
                parts = resolved.rsplit('.', 1)
                labels = {
                    'table': self.task.full_name, 'strategy': 'temptable',
                    'kind': 'cohort_metadata', 'object_name': parts[-1],
                    'object_schema': parts[0] if len(parts) == 2 else None}
                MetadataProbe(self.strategy.engine, options).run(
                    select(literal(1)), labels)
