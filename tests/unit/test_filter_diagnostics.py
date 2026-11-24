"""Offline checks of the filter diagnostic suite and its safety limits."""
import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import (
    Integer,
    create_engine,
    event,
    literal,
    select,
    union_all,
)
from sqlalchemy.dialects import oracle
from sqlalchemy.exc import DBAPIError, NoSuchTableError

from datakiln import build_dbms, cli
from datakiln.cli import get_parser
from datakiln.extraction import probes
from datakiln.extraction.diagnostics import (
    DiagnosticStrategy,
    FilterComparison,
    PreparedFilter,
    TempDiagnosticStrategy,
    configure,
)
from datakiln.extraction.models import ExtractGraph, ExtractMeta, FilterConfig
from datakiln.extraction.native_diagnostics import NativeComparison
from datakiln.extraction.probe_metadata import MetadataProbe
from datakiln.extraction.probes import ProbeOptions, StatementProbe
from datakiln.extraction.renderers import QueryDirector
from datakiln.extraction.strategies import StreamStrategy


@pytest.fixture(name='engine')
def fixture_engine():
    """Provide an in-memory engine and close its pooled connections."""
    engine = create_engine('sqlite://')
    yield engine
    engine.dispose()


@pytest.fixture(name='table_filter')
def fixture_table_filter():
    """Provide a filter whose seeds are supplied by a prerequisite table."""
    return FilterConfig(['order_id'], {
        'schema': 'source', 'table': 'orders', 'columns': ['order_id']})


@pytest.fixture(name='options')
def fixture_options(tmp_path):
    """Provide a report destination and small limits for offline probes."""
    path = tmp_path / 'report.jsonl'
    path.touch()
    return ProbeOptions(path, (2, 0), (2, 0), max_rows=2)


@pytest.fixture(name='comparison')
def fixture_comparison(tmp_path, monkeypatch, table_filter):
    """Prepare a two-filter task without connecting to a source database."""
    strategy = StreamStrategy({}, tmp_path)
    monkeypatch.setattr(strategy, 'get_cohort_set',
                        lambda *args, **kwargs: [(3,), (1,), (2,)])
    monkeypatch.setattr(strategy, '_seed_types',
                        lambda *args: [Integer()])
    task = ExtractMeta('results', 'source', chunk_size=2)
    task.filters = [[table_filter], [FilterConfig(['other_id'], {
        'schema': 'source', 'table': 'orders', 'columns': ['order_id']})]]
    return FilterComparison(strategy, task)


def test_sample_stable_subset(table_filter):
    """Test sampling retains stable order and treats zero as all seeds."""
    prepared = PreparedFilter(table_filter, [(i,) for i in range(10)], [])
    assert prepared.sample(3) == [(0,), (3,), (6,)]
    assert prepared.sample(0) == prepared.seeds
    assert prepared.sample(100) == prepared.seeds


def test_sample_empty(table_filter):
    """Test empty seed relations remain empty at every sample size."""
    prepared = PreparedFilter(table_filter, [], [])
    assert prepared.sample(10) == []


def test_chunking_preserves_payload(table_filter):
    """Test smaller parameters preserve every seed in one statement."""
    statement = select(literal(1)).where(table_filter.to_where(
        [(1,), (2,), (3,)], [Integer()], 2))
    compiled = statement.compile(dialect=oracle.dialect())
    seeds = []
    for name, value in compiled.params.items():
        if name.startswith('seed_list_'):
            seeds.extend(json.loads(value))
    assert seeds == [[1], [2], [3]]
    assert str(compiled).count('JSON_TABLE') == 2
    assert 'UNION ALL' in str(compiled)


def test_comparison_preserves_filter_logic(comparison):
    """Test sampling preserves both OR branches of the original task."""
    comparison.seed_count = 2
    # The real director is used with the prepared filter provider.
    statement = QueryDirector.build_queries(
        None, comparison.task, strategy=comparison)
    compiled = statement.compile(dialect=oracle.dialect())
    assert ' OR ' in str(compiled)
    assert 'src.order_id' in str(compiled)
    assert 'src.other_id' in str(compiled)
    assert str(compiled).count('JSON_TABLE') == 2
    for name, value in compiled.params.items():
        if name.startswith('seed_list_'):
            assert json.loads(value) == [[1], [2]]


def test_probe_limits_rows_and_hides_values(options, engine):
    """Test probes cap fetched rows and report no result or bind values."""
    statement = union_all(select(literal('sensitive-seed-value')),
                          select(literal('another-sensitive-value')),
                          select(literal('third-sensitive-value')))
    probe = StatementProbe(engine, options)
    result = probe.run(statement,
                       {'table': 'source.results', 'kind': 'extract'})
    assert result['status'] == 'row_limit'
    assert result['rows_fetched'] == 2
    assert result['parameter_count'] == 3
    assert result['parameter_characters'] == sum(map(len, [
        'sensitive-seed-value', 'another-sensitive-value',
        'third-sensitive-value']))
    assert 'sensitive' not in options.output.read_text()
    events = []
    for line in options.output.read_text().splitlines():
        events.append(json.loads(line)['event'])
    assert events == ['start', 'phase', 'phase', 'phase', 'finish']


def test_probe_full_consumption(options, engine):
    """Test zero row limit consumes the complete result."""
    unlimited = ProbeOptions(options.output, max_rows=0)
    result = StatementProbe(engine, unlimited).run(
        select(literal(1)), {'table': 'source.results', 'kind': 'extract'})
    assert result['status'] == 'complete'
    assert result['rows_fetched'] == 1


def test_probe_empty_result(options, engine):
    """Test a query returning no rows is recorded as complete."""
    result = StatementProbe(engine, options).run(
        select(literal(1)).where(literal(False)),
        {'table': 'source.results', 'kind': 'extract'})
    assert result['status'] == 'complete'
    assert result['rows_fetched'] == 0


def test_decode_count(options, engine):
    """Test decode probes retain only the aggregate seed count."""
    result = StatementProbe(engine, options).run(
        select(literal(123)), {'table': 'source.results', 'kind': 'decode'})
    assert result['decoded_seeds'] == 123
    assert result['status'] == 'complete'


def test_database_error_is_sanitized(options, monkeypatch, engine):
    """Test failed probes omit SQL, parameters and database message text."""
    def fail_connect():
        raise DBAPIError('private SQL', {'seed': 'private bind'},
                         RuntimeError('private database message'))

    monkeypatch.setattr(engine, 'connect', fail_connect)
    result = StatementProbe(engine, options).run(
        select(literal('private seed')),
        {'table': 'source.results', 'kind': 'extract'})
    assert result['status'] == 'database_error'
    assert result['phase'] == 'connect'
    assert 'private' not in options.output.read_text()


def test_fetch_elapsed_budget(options, monkeypatch, engine):
    """Test fetching stops at its elapsed budget after the first row."""
    clock = iter(range(100))
    monkeypatch.setattr(probes, 'monotonic', lambda: next(clock))
    bounded = ProbeOptions(options.output, timeout=1, max_rows=0)
    result = StatementProbe(engine, bounded).run(
        select(literal(1)), {'table': 'source.results', 'kind': 'extract'})
    assert result['status'] == 'time_limit'
    assert result['rows_fetched'] == 1


def test_timeout_allows_next_probe(options, monkeypatch, engine):
    """Test a database timeout is recorded and later probes still run."""
    probe = StatementProbe(engine, options)
    execute = probe._execute  # pylint: disable=protected-access

    def timeout(_connection, _statement, record):
        record['phase'] = 'first_fetch'
        raise DBAPIError(None, None, RuntimeError('private timeout detail'))

    monkeypatch.setattr(probe, '_execute', timeout)
    failed = probe.run(select(literal(1)),
                       {'table': 'source.results', 'kind': 'extract'})
    monkeypatch.setattr(probe, '_execute', execute)
    passed = probe.run(select(literal(1)),
                       {'table': 'source.results', 'kind': 'extract'})
    assert failed['status'] == 'database_error'
    assert failed['phase'] == 'first_fetch'
    assert passed['status'] == 'complete'
    assert 'private timeout detail' not in options.output.read_text()


def test_failed_probe_discards_connection(options, monkeypatch, engine):
    """Test a failed probe invalidates its pooled database connection."""
    discarded = []
    event.listen(engine, 'invalidate',
                 lambda *_args: discarded.append(True))
    probe = StatementProbe(engine, options)

    def timeout(_connection, _statement, _record):
        raise DBAPIError(None, None, RuntimeError('timeout'))

    monkeypatch.setattr(probe, '_execute', timeout)
    probe.run(select(literal(1)),
              {'table': 'source.results', 'kind': 'extract'})
    assert discarded == [True]


def test_driver_full_code_is_preserved(options, monkeypatch, engine):
    """Test driver timeout codes survive when the Oracle code is zero."""
    detail = SimpleNamespace(code=0, full_code='DPY-4024')
    probe = StatementProbe(engine, options)

    def timeout(_connection, _statement, _record):
        raise DBAPIError(None, None, RuntimeError(detail))

    monkeypatch.setattr(probe, '_execute', timeout)
    result = probe.run(select(literal(1)),
                       {'table': 'source.results', 'kind': 'extract'})
    assert result['database_full_code'] == 'DPY-4024'


def test_fetch_failure_invalidates_before_cursor_cleanup(options):
    """Test a broken fetch invalidates before attempting cursor cleanup."""
    actions = []

    def broken_fetch(_size):
        raise DBAPIError(None, None, RuntimeError('DPY-4024: timeout'))

    result = SimpleNamespace(fetchmany=broken_fetch,
                             close=lambda: actions.append('close_cursor'))
    driver = SimpleNamespace(call_timeout=123)
    connection = SimpleNamespace(
        invalidated=False, execute=lambda _query: result,
        connection=SimpleNamespace(driver_connection=driver))

    def invalidate():
        connection.invalidated = True
        actions.append('invalidate')

    connection.invalidate = invalidate

    @contextmanager
    def connect():
        yield connection

    engine = SimpleNamespace(dialect=oracle.dialect(), connect=connect)
    record = StatementProbe(engine, options).run(select(literal(1)), {
        'table': 'source.results', 'kind': 'extract'})
    assert actions == ['invalidate']
    assert record['phase'] == 'first_fetch'
    assert record['database_full_code'] == 'DPY-4024'
    assert record['first_fetch_seconds'] >= 0


@pytest.mark.parametrize('strategy_class', [
    DiagnosticStrategy, TempDiagnosticStrategy])
def test_probe_cleanup_does_not_drop_uncreated_targets(
        tmp_path, options, strategy_class):
    """Test executor cleanup skips staging tables never created for probes."""
    strategy = strategy_class({}, tmp_path, options)
    # No connection parameters are available; attempting source DDL fails.
    strategy.drop('tmp_uncreated_target')
    assert not strategy.created_tables


@pytest.mark.parametrize('metadata_error', [False, True])
def test_oracle_timeout_restored(options, monkeypatch, metadata_error):
    """Test healthy connections recover their timeout after metadata errors."""
    driver = SimpleNamespace(call_timeout=123)
    connection = SimpleNamespace(
        connection=SimpleNamespace(driver_connection=driver),
        invalidated=False)

    @contextmanager
    def connect():
        yield connection

    engine = SimpleNamespace(dialect=oracle.dialect(), connect=connect)
    probe = StatementProbe(engine, options)
    calls = []

    def execute(_connection, _statement, record):
        calls.append(driver.call_timeout)
        if metadata_error:
            raise NoSuchTableError('not_present')
        record['status'] = 'complete'

    monkeypatch.setattr(probe, '_execute', execute)
    probe.run(select(literal(1)),
              {'table': 'source.results', 'kind': 'extract'})
    assert calls == [60000]
    assert driver.call_timeout == 123


def test_sweep_runs_decode_and_extract(comparison, options, monkeypatch):
    """Test every variant runs seed decoding and the combined extraction."""
    recorded = []

    def record(_self, statement, labels):
        recorded.append((statement.compile(dialect=oracle.dialect()), labels))

    @contextmanager
    def session():
        yield None

    monkeypatch.setattr(StatementProbe, 'run', record)
    monkeypatch.setattr(comparison.strategy, 'session', session)
    comparison.strategy.__dict__['engine'] = None
    comparison.run(options)
    assert len(recorded) == 20
    extracts = []
    for _, labels in recorded:
        if labels['kind'] == 'extract':
            extracts.append(labels)
    assert [labels['seed_counts'] for labels in extracts] == [
        [2, 2], [3, 3], [3, 3], [3, 3]]
    assert [labels['chunk_size'] for labels in extracts] == [2, 2, 2, 0]


def test_isolated_filters_remove_other_predicates(
        comparison, options, monkeypatch):
    """Test isolated queries contain just one of the original predicates."""
    recorded = []

    def record(_self, statement, labels):
        if labels['kind'] == 'extract_filter':
            recorded.append((str(statement.compile(dialect=oracle.dialect())),
                             labels['filter_index']))

    @contextmanager
    def session():
        yield None

    monkeypatch.setattr(StatementProbe, 'run', record)
    monkeypatch.setattr(comparison.strategy, 'session', session)
    comparison.strategy.__dict__['engine'] = None
    comparison.run(options)
    assert len(recorded) == 8
    for sql, index in recorded:
        assert ('src.order_id' in sql) == (index == 0)
        assert ('src.other_id' in sql) == (index == 1)


def test_native_comparison_keeps_staging_predicates(
        comparison, options, monkeypatch):
    """Test temp-table probes use native predicates and no JSON seeds."""
    strategy = TempDiagnosticStrategy({}, comparison.strategy.directory,
                                      options)
    strategy.__dict__['engine'] = None
    strategy.cohorts['source.orders'] = 'tmp_orders'
    recorded = []

    @contextmanager
    def session():
        yield None

    monkeypatch.setattr(strategy, 'session', session)
    monkeypatch.setattr(strategy, 'process_filter',
                        lambda config, *_args: [config.to_exists(strategy)])
    monkeypatch.setattr(MetadataProbe, 'run', lambda *_args: None)
    monkeypatch.setattr(StatementProbe, 'run', lambda _self, query, labels:
                        recorded.append((str(query.compile(
                            dialect=oracle.dialect())), labels)))
    NativeComparison(strategy, comparison.task).run(options)
    assert len(recorded) == 3
    assert ' OR ' in recorded[0][0]
    for sql, labels in recorded:
        assert 'tmp_orders' in sql
        assert 'JSON_TABLE' not in sql
        assert labels['strategy'] == 'temptable'
    assert 'src.other_id' not in recorded[1][0]
    assert 'src.order_id' not in recorded[2][0]


def diagnostic_args(tmp_path):
    """Build the same arguments accepted by the public command."""
    return get_parser().parse_args([
        'extract', 'dbms', str(tmp_path), '--dialect', 'oracle',
        '--host', 'example.invalid', '--port', '1521', '--database', 'ORCL',
        '--diagnose-filters', '--include', 'source.results',
        '--diagnostic-report', str(tmp_path / 'report.jsonl')])


def test_configuration_refuses_overwrite(tmp_path):
    """Test a diagnostic run refuses to overwrite an existing report."""
    args = diagnostic_args(tmp_path)
    args.diagnostic_report.write_text('keep me')
    with pytest.raises(FileExistsError):
        configure(args, ExtractGraph([ExtractMeta('results', 'source')]))
    assert args.diagnostic_report.read_text() == 'keep me'


def test_cli_uses_a_separate_diagnostic_log(tmp_path, monkeypatch):
    """Test diagnostic logging uses the report path and refuses overwrites."""
    args = diagnostic_args(tmp_path)
    seen = []
    args.run = lambda parsed: seen.append(parsed.diagnostic_report)
    monkeypatch.setattr(cli, 'get_parser', lambda: SimpleNamespace(
        parse_args=lambda _args: args))
    monkeypatch.setattr(cli, 'setup_logging',
                        lambda *_args, **kwargs: seen.append(kwargs))
    cli.main([])
    assert args.log_file == f'{args.diagnostic_report}.log'
    assert seen == [{'overwrite': False}, args.diagnostic_report]


@pytest.mark.parametrize('option,value', [
    ('diagnostic_timeout', 0), ('diagnostic_repeats', 0),
    ('diagnostic_max_rows', -1), ('diagnostic_seed_counts', [-1]),
    ('diagnostic_chunk_sizes', [-1]), ('dialect', 'postgresql'),
    ('include', None), ('metadata', True), ('data', False)])
def test_configuration_rejects_invalid_options(tmp_path, option, value):
    """Test invalid diagnostic settings fail before any database access."""
    args = diagnostic_args(tmp_path)
    setattr(args, option, value)
    with pytest.raises(ValueError):
        configure(args, ExtractGraph([ExtractMeta('results', 'source')]))
    assert not args.diagnostic_report.exists()


def test_configuration_records_settings(tmp_path):
    """Test the report starts with all limits needed to reproduce the run."""
    args = diagnostic_args(tmp_path)
    options = configure(args, ExtractGraph([ExtractMeta('results', 'source')]))
    record = json.loads(options.output.read_text())
    assert record['event'] == 'settings'
    assert record['seed_counts'] == [100, 1000, 10000, 0]
    assert record['timeout_seconds'] == 60


def test_configuration_rejects_dependent_targets(tmp_path):
    """Test a probe cannot replace data another diagnostic target needs."""
    args = diagnostic_args(tmp_path)
    parent = ExtractMeta('orders', 'source')
    child = ExtractMeta('results', 'source', filters=[{
        'columns': ['order_id'], 'reference': {
            'schema': 'source', 'table': 'orders', 'columns': ['order_id']}}])
    with pytest.raises(ValueError, match='must not depend'):
        configure(args, ExtractGraph([parent, child]))
    assert not args.diagnostic_report.exists()


def test_target_exports_are_not_written(tmp_path, options, monkeypatch):
    """Test diagnostic targets preserve existing data exports."""
    task = ExtractMeta('results', 'source')
    strategy = DiagnosticStrategy({}, tmp_path, options)
    strategy.graph = ExtractGraph([task])
    strategy.__dict__['engine'] = None
    existing = tmp_path / 'results.csv.gz'
    existing.write_bytes(b'existing export')
    calls = []
    monkeypatch.setattr(FilterComparison, '__init__',
                        lambda self, *_args: setattr(self, 'filters', {1: 1}))
    monkeypatch.setattr(FilterComparison, 'run',
                        lambda _self, settings: calls.append(settings))
    monkeypatch.setattr(MetadataProbe, 'run', lambda *_args: None)
    strategy.execute(task, export_to=tmp_path)
    assert calls == [options]
    assert existing.read_bytes() == b'existing export'


@pytest.mark.parametrize('read_only', [True, False])
def test_command_uses_selected_strategy(tmp_path, monkeypatch, read_only):
    """Test the public command honors the strategy and forces serial work."""
    args = diagnostic_args(tmp_path)
    args.parallel = 4
    args.read_only = read_only
    extract = tmp_path / 'extract'
    extract.mkdir()
    (extract / 'results.json').write_text(json.dumps({
        'table': 'results', 'schema': 'source'}))
    calls = []

    class Executor:
        """Record the strategy and concurrency chosen by the command."""

        def __init__(self, strategy, graph, max_workers):
            calls.append((strategy, graph, max_workers))

        def execute(self, data_dir):
            """Record the extraction destination without accessing a DB."""
            calls.append(data_dir)

    monkeypatch.setattr(build_dbms, 'ParallelExecutor', Executor)
    monkeypatch.setattr(DiagnosticStrategy, 'cleanup',
                        lambda _self: calls.append('cleanup'))
    monkeypatch.setattr(TempDiagnosticStrategy, 'cleanup',
                        lambda _self: calls.append('cleanup'))
    build_dbms.run(args)
    strategy, graph, workers = calls[0]
    expected = DiagnosticStrategy if read_only else TempDiagnosticStrategy
    assert isinstance(strategy, expected)
    assert strategy.is_read_only == read_only
    assert workers == 1
    assert {task.full_name for task in graph.export} == {'source.results'}
    assert calls[1:] == [tmp_path / 'data', 'cleanup']
