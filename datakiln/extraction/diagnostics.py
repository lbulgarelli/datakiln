"""Repeatable comparisons of cohort size and parameter chunking."""
import logging
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import literal, select
from sqlalchemy.sql.functions import count as sql_count

from datakiln.extraction.models import ExtractMeta, FilterConfig
from datakiln.extraction.native_diagnostics import NativeComparison
from datakiln.extraction.probe_metadata import MetadataProbe, run_extraction
from datakiln.extraction.probes import (
    ProbeOptions,
    StatementProbe,
    write_record,
)
from datakiln.extraction.renderers import QueryDirector, SQLGenerator
from datakiln.extraction.strategies import StreamStrategy, TempTableStrategy
from datakiln.metadata.database import Session

logger = logging.getLogger(__name__)


def report_path(args: Namespace) -> Path:
    """Choose the shared filename prefix of a diagnostic report and log.

    :param args: Parsed diagnostic command arguments.
    :type args: argparse.Namespace
    :return: Explicit path or a new timestamped report path.
    :rtype: pathlib.Path
    """
    if args.diagnostic_report:
        return args.diagnostic_report
    stamp = datetime.now().strftime('%Y%m%d-%H%M%S-%f')
    return Path(args.directory) / f'filter-diagnostics-{stamp}.jsonl'


def add_arguments(parser: ArgumentParser) -> None:
    """Add the optional filter diagnostic suite to DBMS extraction.

    :param parser: DBMS extraction parser.
    :type parser: argparse.ArgumentParser
    """
    parser.add_argument('--diagnose-filters', action='store_true',
                        help='Probe the --include tables with the selected '
                             'strategy, preparing prerequisites serially.')
    parser.add_argument('--diagnostic-explain', action='store_true',
                        help='Write estimated plans to PLAN_TABLE and read '
                             'structural plan metadata; requires its access.')
    parser.add_argument('--diagnostic-report', type=Path,
                        help='New JSON lines report; existing files refuse '
                             'to be overwritten.')
    parser.add_argument('--diagnostic-seed-counts', type=int, nargs='+',
                        default=[100, 1000, 10000, 0],
                        help='Read-only seed counts to compare; '
                             '0 uses all seeds.')
    parser.add_argument('--diagnostic-chunk-sizes', type=int, nargs='+',
                        default=[10000, 50000, 0],
                        help='Read-only chunk sizes with all seeds fixed; '
                             '0 puts each filter in one parameter.')
    parser.add_argument('--diagnostic-max-rows', type=int, default=10000,
                        help='Maximum fetched rows per probe; 0 fetches all.')
    parser.add_argument('--diagnostic-timeout', type=int, default=60,
                        help='Oracle call timeout and fetch-loop budget '
                             'in seconds per probe.')
    parser.add_argument('--diagnostic-repeats', type=int, default=1,
                        help='Number of repetitions of all comparisons.')


def configure(args: Namespace, graph: Any) -> ProbeOptions:
    """Validate a diagnostic run and create its report before connecting.

    :param args: Parsed DBMS extraction options.
    :type args: argparse.Namespace
    :param graph: Selected extraction graph.
    :type graph: datakiln.extraction.models.ExtractGraph
    :return: Validated probe settings.
    :rtype: datakiln.extraction.probes.ProbeOptions
    """
    if args.dialect != 'oracle' or not args.include:
        raise ValueError('Filter diagnostics require Oracle and --include.')
    if not args.data or args.metadata:
        raise ValueError('Filter diagnostics require data and no metadata.')
    if args.diagnostic_timeout <= 0 or args.diagnostic_repeats <= 0:
        raise ValueError('Diagnostic timeout and repeats must be positive.')
    sizes = args.diagnostic_seed_counts + args.diagnostic_chunk_sizes
    if args.diagnostic_max_rows < 0 or any(size < 0 for size in sizes):
        raise ValueError('Diagnostic sizes and row limits cannot be negative.')

    # A probe does not export rows that another task could depend on.
    for task in graph.export:
        if (graph.requirements([task]) - {task}) & graph.export:
            raise ValueError('Diagnostic targets must not depend on each '
                             'other; run them separately.')

    # Use a distinct report and refuse any explicitly reused destination.
    path = report_path(args)
    with path.open('x', encoding='utf-8'):
        pass
    options = ProbeOptions(
        path, tuple(args.diagnostic_seed_counts),
        tuple(args.diagnostic_chunk_sizes), args.diagnostic_max_rows,
        args.diagnostic_timeout, args.diagnostic_repeats,
        args.diagnostic_explain)
    write_record(path, {'event': 'settings',
                        'strategy': 'read_only' if args.read_only
                        else 'temptable',
                        'seed_counts': options.seed_counts,
                        'chunk_sizes': options.chunk_sizes,
                        'max_rows': options.max_rows,
                        'timeout_seconds': options.timeout,
                        'repeats': options.repeats,
                        'explain': options.explain})
    logger.info("Report '%s': Running filter diagnostics serially.", path)
    return options


@dataclass(frozen=True)
class PreparedFilter:
    """Typed seeds loaded once and reused across every comparison."""

    config: FilterConfig
    seeds: list[tuple] = field(repr=False)
    types: list[Any]

    def sample(self, count: int) -> list[tuple]:
        """Select a deterministic, evenly spaced subset of the seeds.

        :param count: Maximum seed count, or zero for all seeds.
        :type count: int
        :return: Selected seeds in stable order.
        :rtype: list[tuple]
        """
        if not count or count >= len(self.seeds):
            return self.seeds
        return self.seeds[::max(1, len(self.seeds) // count)][:count]


class FilterComparison:
    """Build comparable extraction statements from cached filter inputs."""

    def __init__(self, strategy: StreamStrategy, task: ExtractMeta):
        """Prepare the filters before running timed source queries.

        :param strategy: Strategy that resolves prerequisite cohorts.
        :type strategy: datakiln.extraction.strategies.StreamStrategy
        :param task: Extraction task to diagnose.
        :type task: datakiln.extraction.models.ExtractMeta
        """
        self.strategy = strategy
        self.task = task
        self.filters = {}
        self.seed_count = 0
        self.chunk_size = task.chunk_size

        # Sorting removes differences caused by set iteration order.
        for group in task.filters:
            for config in group:
                if config.resolved_cohort and not config.is_sql_cohort:
                    seeds = strategy.get_cohort_set(config, task=task)
                    seeds.sort(key=lambda row: tuple(
                        (value is not None, str(value)) for value in row))
                    # Reuse extraction's type resolution for a fair comparison.
                    types = strategy._seed_types(config, task, seeds)  # pylint: disable=protected-access # noqa: E501
                    self.filters[id(config)] = PreparedFilter(
                        config, seeds, types)

    def process_filter(self, filter_config: FilterConfig, session: Session,
                       task: ExtractMeta) -> list[Any]:
        """Supply the selected seed count to the existing query builder.

        :param filter_config: Filter being compiled.
        :type filter_config: datakiln.extraction.models.FilterConfig
        :param session: Query-building session.
        :type session: datakiln.metadata.database.Session
        :param task: Extraction task being compiled.
        :type task: datakiln.extraction.models.ExtractMeta
        :return: Existing filter with only its seed inputs changed.
        :rtype: list[typing.Any]
        """
        prepared = self.filters.get(id(filter_config))
        if prepared is None:
            return self.strategy.process_filter(filter_config, session, task)
        return [filter_config.to_where(
            prepared.sample(self.seed_count), prepared.types, self.chunk_size)]

    def run(self, options: ProbeOptions) -> None:
        """Compare cohort sizes, then chunk sizes with all seeds fixed.

        :param options: Probe budgets, comparisons and report destination.
        :type options: datakiln.extraction.probes.ProbeOptions
        """
        probe = StatementProbe(self.strategy.engine, options)
        for repeat in range(1, options.repeats + 1):
            # One variable changes at a time; zero denotes the full set.
            for count in options.seed_counts:
                self.seed_count = count
                self.chunk_size = self.task.chunk_size
                self._run_variant(probe, 'seed_count', repeat)
            for size in options.chunk_sizes:
                self.seed_count = 0
                self.chunk_size = size
                self._run_variant(probe, 'chunk_size', repeat)

    def _run_variant(self, probe: StatementProbe, variant: str,
                     repeat: int) -> None:
        """Decode each filter separately, then run the combined query."""
        labels = {'table': self.task.full_name, 'strategy': 'read_only',
                  'variant': variant,
                  'repeat': repeat, 'seed_limit': self.seed_count,
                  'chunk_size': self.chunk_size, 'seed_counts': []}
        for prepared in self.filters.values():
            labels['seed_counts'].append(len(prepared.sample(self.seed_count)))
        for index, prepared in enumerate(self.filters.values()):
            seeds = prepared.sample(self.seed_count)

            # Count the decoded relation without reading the source table.
            if seeds:
                relation = prepared.config.seed_table(
                    seeds, prepared.types, self.chunk_size)
                statement = select(sql_count()).select_from(relation)
                probe.run(statement, dict(
                    labels, kind='decode', filter_index=index,
                    seed_types=list(map(str, prepared.types))))

        # Preserve the original AND/OR logic and all source projections.
        with self.strategy.session() as session:
            statement = QueryDirector.build_queries(
                session, self.task, strategy=self)
        run_extraction(probe, statement, dict(labels, kind='extract'))

        # Isolate each seed predicate, keeping this variant's seed sample.
        for index, prepared in enumerate(self.filters.values()):
            isolated = SQLGenerator.base_select(
                self.task.schema, self.task.table).where(
                    prepared.config.to_where(
                        prepared.sample(self.seed_count), prepared.types,
                        self.chunk_size))
            run_extraction(probe, isolated, dict(
                labels, kind='extract_filter', filter_index=index))


class DiagnosticMixin:
    """Extract prerequisites normally and probe selected terminal tasks."""

    def __init__(self, db_params: dict, directory: Path,
                 options: ProbeOptions):
        """Configure a serial diagnostic extraction with either strategy.

        :param db_params: Source connection parameters.
        :type db_params: dict
        :param directory: Dataset directory.
        :type directory: pathlib.Path
        :param options: Report destination and probe settings.
        :type options: datakiln.extraction.probes.ProbeOptions
        """
        super().__init__(db_params, directory)
        self.options = options

    def drop(self, name: str) -> None:
        """Clean up prerequisites created by this run, leaving probes alone.

        :param name: Temporary name passed by the extraction executor.
        :type name: str
        """
        # Targets are measured without creating their export or staging table.
        if name in self.created_tables:
            super().drop(name)

    def execute(self, task: ExtractMeta, export_to: Path | None = None):
        """Probe targets without overwriting their existing data files.

        :param task: Task to extract or diagnose.
        :type task: datakiln.extraction.models.ExtractMeta
        :param export_to: Export directory for ordinary prerequisites.
        :type export_to: pathlib.Path, optional
        """
        if task not in self.graph.export:
            return super().execute(task, export_to)

        # Preparation failures retain the table name without SQL or values.
        write_record(self.options.output,
                     {'event': 'prepare', 'table': task.full_name})
        logger.info("Table '%s': Preparing filter diagnostics.",
                    task.full_name)
        mode = 'read_only' if self.is_read_only else 'temptable'
        metadata = MetadataProbe(self.engine, self.options)
        metadata.run(select(literal(1)), {
            'table': task.full_name, 'kind': 'metadata', 'strategy': mode})
        if self.is_read_only:
            comparison = FilterComparison(self, task)
            if not comparison.filters:
                raise ValueError(
                    f"No seed filters to diagnose for '{task.full_name}'")
        else:
            comparison = NativeComparison(self, task)
        comparison.run(self.options)
        return None


class DiagnosticStrategy(DiagnosticMixin, StreamStrategy):
    """Diagnose JSON seed filters with read-only source access."""


class TempDiagnosticStrategy(DiagnosticMixin, TempTableStrategy):
    """Diagnose native predicates with staged source cohorts."""
