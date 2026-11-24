"""Verify shared seed CTEs and duplicate-safe explicit join comparisons."""
import json
from collections import Counter
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    cast,
    create_engine,
    literal,
    select,
    union_all,
)
from sqlalchemy.dialects import oracle

from datakiln.extraction.cte_diagnostics import CteComparison
from datakiln.extraction.diagnostics import PreparedFilter
from datakiln.extraction.models import ExtractMeta, FilterConfig
from datakiln.extraction.probes import ProbeOptions, StatementProbe
from datakiln.extraction.renderers import QueryDirector


@pytest.fixture(name='comparison')
def fixture_comparison():
    """Provide paired filters that share the same reference column."""
    task = ExtractMeta('items', 'main')
    task.filters = []
    filters = {}
    for name in ('a', 'b'):
        config = FilterConfig([name], {
            'schema': 'main', 'table': 'cohort', 'columns': ['id']})
        task.filters.append([config])
        # Different raw values become the same typed numeric key.
        filters[id(config)] = PreparedFilter(
            config, [(1,), ('01',), ('1',)], [Integer()])
    return SimpleNamespace(task=task, filters=filters, seed_count=0,
                           chunk_size=10000, strategy=SimpleNamespace(
                               engine=SimpleNamespace(
                                   dialect=oracle.dialect())))


def test_cte_shares_identical_typed_seed_data(comparison):
    """Test two predicates reuse one JSON bind and one typed seed CTE."""
    cte = CteComparison(comparison)
    query = QueryDirector.build_queries(None, comparison.task, strategy=cte)
    compiled = query.compile(dialect=oracle.dialect())
    assert len(cte.shared) == 1
    assert str(compiled).count('JSON_TABLE') == 1
    assert str(compiled).count('cohort_seed_1') >= 3
    assert 'SELECT DISTINCT' in str(compiled)
    assert sum(name.startswith('seed_list_')
               for name in compiled.params) == 1


@pytest.mark.parametrize('difference', ['values', 'types'])
def test_cte_does_not_share_incompatible_inputs(comparison, difference):
    """Test matching reference names do not merge different data or types."""
    second = comparison.task.filters[1][0]
    values = [(2,)] if difference == 'values' else [(1,), ('01',), ('1',)]
    types = [Integer()] if difference == 'values' else [String(20)]
    comparison.filters[id(second)] = PreparedFilter(second, values, types)
    cte = CteComparison(comparison)
    query = QueryDirector.build_queries(None, comparison.task, strategy=cte)
    assert len(cte.shared) == 2
    sql = str(query.compile(dialect=oracle.dialect()))
    assert sql.count('JSON_TABLE') == 2
    if difference == 'types':
        assert sql.count('SELECT DISTINCT') == 1


@pytest.fixture(name='source')
def fixture_source(monkeypatch, comparison):
    """Execute equivalent typed seed relations on a local SQLite database."""
    engine = create_engine('sqlite://')
    table = Table('items', MetaData(), Column('a', Integer),
                  Column('b', Integer), Column('payload', String))

    def seed_table(values, types, _chunk_size=None):
        rows = []
        for row in values:
            columns = []
            for index, (value, kind) in enumerate(zip(row, types)):
                columns.append(cast(literal(value), kind).label(f'c{index}'))
            rows.append(select(*columns))
        query = rows[0] if len(rows) == 1 else union_all(*rows)
        return query.subquery('seed')

    monkeypatch.setattr(FilterConfig, 'seed_table', staticmethod(seed_table))
    comparison.strategy.engine = engine
    try:
        table.metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(table.insert(), [
                {'a': 1, 'b': 1, 'payload': 'same'},
                {'a': 1, 'b': 1, 'payload': 'same'},
                {'a': 1, 'b': 2, 'payload': 'left'},
                {'a': 2, 'b': 1, 'payload': 'right'},
                {'a': None, 'b': 1, 'payload': 'nullable'},
                {'a': 2, 'b': 2, 'payload': 'neither'}])
            yield connection, comparison
    finally:
        engine.dispose()


@pytest.mark.parametrize('grouping', ['or', 'and'])
def test_join_preserves_overlap_and_physical_duplicates(source, grouping):
    """Test joins preserve duplicate source rows and eliminate seed fanout."""
    connection, comparison = source
    if grouping == 'and':
        comparison.task.filters = [[group[0] for group in
                                    comparison.task.filters]]
    cte = CteComparison(comparison)
    assert cte.joinable
    # Use the same filter provider without the CTE substitution as baseline.
    comparison.process_filter = lambda config, *_args: [config.to_where(
        comparison.filters[id(config)].seeds, [Integer()], 10000)]
    original = QueryDirector.build_queries(
        None, comparison.task, strategy=comparison)
    exists = QueryDirector.build_queries(None, comparison.task, strategy=cte)
    joined = cte._join_query(None)  # pylint: disable=protected-access
    expected = Counter(map(tuple, connection.execute(original)))
    assert expected == Counter(map(tuple, connection.execute(exists)))
    assert expected == Counter(map(tuple, connection.execute(joined)))
    assert expected[(1, 1, 'same')] == 2
    assert sum(expected.values()) == (5 if grouping == 'or' else 2)


@pytest.mark.parametrize('unsafe', ['null', 'range', 'expression', 'string'])
def test_join_skips_filters_without_unique_matches(comparison, unsafe):
    """Test unsafe join predicates remain available only through EXISTS."""
    first = comparison.task.filters[0][0]
    if unsafe == 'null':
        comparison.filters[id(first)].seeds.append((None,))
    elif unsafe == 'range':
        first.reference.operators = ['<']
    elif unsafe == 'expression':
        first.columns = ['ABS(src.a)']
    else:
        comparison.filters[id(first)] = PreparedFilter(
            first, [('A',), ('a',)], [String(20)])
    assert not CteComparison(comparison).joinable


def test_join_sql_projects_only_source_columns(comparison):
    """Test joined seed keys are excluded from the extraction projection."""
    cte = CteComparison(comparison)
    query = cte._join_query(None)  # pylint: disable=protected-access
    sql = str(query.compile(dialect=oracle.dialect()))
    assert sql.count('JSON_TABLE') == 1
    assert 'SELECT src.*' in sql
    assert 'JOIN cohort_seed_1 match_0_0' in sql
    assert 'NOT (EXISTS' in sql


def test_cte_exists_preserves_null_wildcards(source):
    """Test seed deduplication preserves unrestricted NULL seed conditions."""
    connection, comparison = source
    first = comparison.task.filters[0][0]
    comparison.filters[id(first)].seeds.append((None,))
    comparison.process_filter = lambda config, *_args: [config.to_where(
        comparison.filters[id(config)].seeds, [Integer()], 10000)]
    original = QueryDirector.build_queries(None, comparison.task,
                                           strategy=comparison)
    cte = CteComparison(comparison)
    query = QueryDirector.build_queries(None, comparison.task, strategy=cte)
    assert not cte.joinable
    actual = Counter(map(tuple, connection.execute(query)))
    assert actual == Counter(map(tuple, connection.execute(original)))
    assert sum(actual.values()) == 6


def test_comparison_keeps_settings_and_optional_plans(
        comparison, tmp_path, monkeypatch):
    """Test both CTE forms use the same limits and get separate plans."""
    @contextmanager
    def session():
        yield None

    comparison.strategy.session = session
    probe = StatementProbe(comparison.strategy.engine, ProbeOptions(
        tmp_path / 'report.jsonl', explain=True))
    recorded = []
    monkeypatch.setattr(StatementProbe, 'run', lambda self, _query, labels:
                        recorded.append((self.options, labels)))
    CteComparison(comparison).run(probe, {
        'table': 'main.items', 'seed_limit': 100, 'repeat': 1,
        'chunk_size': 10000, 'variant': 'seed_count'})
    assert [labels['kind'] for _, labels in recorded] == [
        'plan', 'extract_cte_exists', 'plan', 'extract_cte_join']
    for options, labels in recorded:
        assert options is probe.options
        assert labels['shared_seed_ctes'] == 1
        assert labels['seed_limit'] == 100


def test_unsafe_join_is_reported_and_exists_still_runs(
        comparison, tmp_path, monkeypatch):
    """Test unsafe joins get a skip record without losing the EXISTS probe."""
    first = comparison.task.filters[0][0]
    comparison.filters[id(first)].seeds.append((None,))

    @contextmanager
    def session():
        yield None

    comparison.strategy.session = session
    path = tmp_path / 'report.jsonl'
    path.touch()
    probe = StatementProbe(comparison.strategy.engine, ProbeOptions(path))
    kinds = []
    monkeypatch.setattr(StatementProbe, 'run', lambda _self, _query, labels:
                        kinds.append(labels['kind']))
    CteComparison(comparison).run(probe, {'table': 'main.items'})
    assert kinds == ['extract_cte_exists']
    record = json.loads(path.read_text())
    assert record['event'] == 'skip'
    assert record['kind'] == 'extract_cte_join'
    assert record['status'] == 'unsupported'
