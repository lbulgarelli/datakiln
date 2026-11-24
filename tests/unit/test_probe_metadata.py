"""Check metadata and estimated-plan collection without Oracle access."""
import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import (
    Column,
    Index,
    Integer,
    MetaData,
    Table,
    create_engine,
    literal,
    or_,
    select,
)
from sqlalchemy.dialects import oracle
from sqlalchemy.exc import DBAPIError

from datakiln.extraction.models import FilterConfig
from datakiln.extraction.probe_metadata import (
    MetadataProbe,
    PlanProbe,
    run_extraction,
)
from datakiln.extraction.probes import ProbeOptions, StatementProbe
from datakiln.extraction.rewrites import disjoint_union_all


@pytest.fixture(name='options')
def fixture_options(tmp_path):
    """Provide a new report for metadata probes."""
    path = tmp_path / 'report.jsonl'
    path.touch()
    return ProbeOptions(path)


@pytest.mark.parametrize('kind', ['metadata', 'cohort_metadata'])
def test_metadata_records_types_and_indexes(options, kind):
    """Test metadata preserves index column order without defaults."""
    engine = create_engine('sqlite://')
    source = Table('results', MetaData(),
                   Column('id', Integer, server_default='42'),
                   Column('other_id', Integer))
    Index('results_pair', source.c.other_id, source.c.id)
    try:
        source.metadata.create_all(engine)
        result = MetadataProbe(engine, options).run(select(literal(1)), {
            'table': 'main.results', 'kind': kind})
        assert result['status'] == 'complete'
        assert result['columns'][0]['type'] == 'INTEGER'
        assert result['indexes'][0]['columns'] == ['other_id', 'id']
        if kind == 'cohort_metadata':
            assert result['cohort_rows'] == 0
        assert 'default' not in options.output.read_text()
    finally:
        engine.dispose()


def test_missing_metadata_is_reported(options):
    """Test missing metadata is recorded rather than aborting the suite."""
    engine = create_engine('sqlite://')
    try:
        record = MetadataProbe(engine, options).run(select(literal(1)), {
            'table': 'main.not_present', 'kind': 'metadata'})
        assert record['status'] == 'database_error'
        assert record['error_type'] == 'NoSuchTableError'
    finally:
        engine.dispose()


@pytest.fixture(name='plan_engine')
def fixture_plan_engine():
    """Record Oracle statements while returning only fake plan metadata."""
    statements = []
    row = {'id': 0, 'parent_id': None, 'cardinality': 100, 'cost': 10,
           'bytes': 300, 'operation': 'SELECT STATEMENT', 'options': None,
           'object_owner': None, 'object_name': None}

    @contextmanager
    def execute(statement):
        statements.append(statement)
        yield SimpleNamespace(mappings=lambda: SimpleNamespace(
            fetchmany=lambda _size: [row]))

    driver = SimpleNamespace(call_timeout=0)
    connection = SimpleNamespace(
        dialect=oracle.dialect(), invalidated=False, execute=execute,
        connection=SimpleNamespace(driver_connection=driver))

    @contextmanager
    def connect():
        yield connection

    engine = SimpleNamespace(dialect=connection.dialect, connect=connect)
    return engine, statements


@pytest.mark.parametrize('rewrite', [False, True])
def test_plan_preserves_clob_bind_types(options, plan_engine, rewrite):
    """Test EXPLAIN PLAN retains CLOB binds without literal seed values."""
    config = FilterConfig(['order_id'], {
        'schema': 'source', 'table': 'orders', 'columns': ['order_id']})
    predicate = config.to_where([(987654321,)], [Integer()], 100)
    statement = select(literal(1)).where(or_(predicate, predicate))
    if rewrite:
        statement = disjoint_union_all(statement)
    engine, statements = plan_engine
    result = PlanProbe(engine, options).run(statement, {
        'table': 'source.results', 'kind': 'plan'})
    assert result['status'] == 'complete'
    assert result['plan_steps'][0]['cardinality'] == 100
    explained = statements[0].compile(dialect=oracle.dialect())
    clobs = []
    for original in explained.binds.values():
        if str(original.type) == 'CLOB':
            clobs.append(original)
    assert clobs
    assert '987654321' not in str(explained)
    assert '987654321' not in options.output.read_text()
    assert 'access_predicates' not in str(statements[1])
    assert 'filter_predicates' not in str(statements[1])


@pytest.mark.parametrize('enabled', [True, False])
def test_plan_collection_is_opt_in(options, monkeypatch, enabled):
    """Test an estimated plan is collected only when explicitly enabled."""
    settings = ProbeOptions(options.output, explain=enabled)
    recorded = []
    monkeypatch.setattr(StatementProbe, 'run',
                        lambda _self, _query, labels:
                        recorded.append(labels['kind']))
    probe = StatementProbe(None, settings)
    run_extraction(probe, select(literal(1)), {
        'table': 'source.results', 'kind': 'extract_filter'})
    expected = ['plan', 'extract_filter'] if enabled else ['extract_filter']
    assert recorded == expected


def test_plan_permission_error_does_not_stop_extraction(options, monkeypatch):
    """Test unavailable PLAN_TABLE access leaves extraction probes runnable."""
    engine = create_engine('sqlite://')
    settings = ProbeOptions(options.output, explain=True)

    def unavailable(_self, _connection, _statement, _record):
        raise DBAPIError(None, None, RuntimeError('ORA-01031: private detail'))

    monkeypatch.setattr(PlanProbe, '_execute', unavailable)
    try:
        run_extraction(StatementProbe(engine, settings), select(literal(1)), {
            'table': 'main.results', 'kind': 'extract'})
        finished = []
        for line in options.output.read_text().splitlines():
            record = json.loads(line)
            if record['event'] == 'finish':
                finished.append(record)
        assert finished[0]['database_full_code'] == 'ORA-01031'
        assert finished[1]['status'] == 'complete'
        assert 'private detail' not in options.output.read_text()
    finally:
        engine.dispose()
