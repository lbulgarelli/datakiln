"""Verify diagnostic OR expansion preserves source-row multiplicity."""
from collections import Counter
from itertools import product

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    and_,
    create_engine,
    exists,
    literal,
    literal_column,
    or_,
    select,
)
from sqlalchemy.dialects import oracle

from datakiln.extraction.probe_metadata import run_extraction
from datakiln.extraction.probes import ProbeOptions, StatementProbe
from datakiln.extraction.rewrites import disjoint_union_all


@pytest.fixture(name='source')
def fixture_source():
    """Provide nullable predicates and physically duplicated source rows."""
    engine = create_engine('sqlite://')
    table = Table('source', MetaData(), Column('a', Integer),
                  Column('b', Integer), Column('c', Integer),
                  Column('payload', String))
    try:
        table.metadata.create_all(engine)
        rows = []
        for first, second, third in product((None, 0, 1, 2), repeat=3):
            row = {'a': first, 'b': second, 'c': third, 'payload': 'same'}
            rows.extend([row, dict(row)])
        with engine.begin() as connection:
            connection.execute(table.insert(), rows)
            yield connection, table.alias('src')
    finally:
        engine.dispose()


@pytest.mark.parametrize('projection', ['all', 'payload'])
@pytest.mark.parametrize('scenario', ['two', 'three', 'and_groups', 'nulls'])
def test_union_preserves_rows_and_duplicates(source, projection, scenario):
    """Test OR expansion preserves overlap, UNKNOWN and duplicate rows."""
    connection, table = source
    conditions = {
        'two': or_(table.c.a == 1, table.c.b == 1),
        'three': or_(table.c.a == 1, table.c.b == 1, table.c.c == 1),
        'and_groups': or_(and_(table.c.a == 1, table.c.c > 0),
                          and_(table.c.b == 1, table.c.c.is_(None))),
        'nulls': or_(table.c.a.is_(None), table.c.b.is_(None)),
    }
    # A payload-only projection makes many distinct rows look identical.
    columns = literal_column('*') if projection == 'all' else table.c.payload
    original = select(columns).select_from(table).where(conditions[scenario])
    rewritten = disjoint_union_all(original)
    assert rewritten is not None
    expected = Counter(map(tuple, connection.execute(original)))
    actual = Counter(map(tuple, connection.execute(rewritten)))
    assert actual == expected
    assert sum(actual.values()) > len(actual)


def test_union_compiles_for_oracle_without_changing_original(source):
    """Test Oracle receives disjoint branches while the original stays OR."""
    _, table = source
    original = select(table).where(or_(table.c.a == 1, table.c.b == 1))
    before = str(original.compile(dialect=oracle.dialect()))
    rewritten = str(disjoint_union_all(original).compile(
        dialect=oracle.dialect()))
    assert 'UNION ALL' in rewritten
    assert 'CASE WHEN' in rewritten
    assert ' OR ' not in rewritten
    assert 'DISTINCT' not in rewritten
    assert str(original.compile(dialect=oracle.dialect())) == before


@pytest.mark.parametrize('grouped', [False, True])
def test_exists_branches_use_not_exists_and_preserve_rows(source, grouped):
    """Test existence predicates use anti-conditions without CASE wrappers."""
    connection, table = source
    first = exists(select(literal(1)).where(literal_column('src.a') == 1))
    second = exists(select(literal(1)).where(literal_column('src.b') == 1))
    if grouped:
        first = and_(first, exists(select(literal(1)).where(
            literal_column('src.c') == 1)))
    original = select(table).where(or_(first, second))
    rewritten = disjoint_union_all(original)
    expected = Counter(map(tuple, connection.execute(original)))
    assert Counter(map(tuple, connection.execute(rewritten))) == expected
    assert 0 < sum(expected.values()) < 128
    sql = str(rewritten.compile(dialect=oracle.dialect()))
    assert 'UNION ALL' in sql
    assert 'NOT ' in sql
    assert 'CASE' not in sql


@pytest.mark.parametrize('filtered', [False, True])
def test_union_skips_queries_without_top_level_or(source, filtered):
    """Test unfiltered and AND-only queries do not add a union comparison."""
    _, table = source
    original = select(table)
    if filtered:
        original = original.where(and_(table.c.a == 1, table.c.b == 1))
    assert disjoint_union_all(original) is None


def test_union_gets_same_labels_limits_and_optional_plan(
        source, tmp_path, monkeypatch):
    """Test the rewrite is an additional probe with matching run settings."""
    _, table = source
    original = select(table).where(or_(table.c.a == 1, table.c.b == 1))
    options = ProbeOptions(tmp_path / 'report.jsonl', explain=True)
    recorded = []
    monkeypatch.setattr(StatementProbe, 'run',
                        lambda self, query, labels:
                        recorded.append((self.options, query, labels)))
    labels = {'table': 'source.results', 'strategy': 'read_only',
              'kind': 'extract', 'variant': 'seed_count', 'seed_limit': 100,
              'chunk_size': 10000, 'repeat': 1}
    run_extraction(StatementProbe(None, options), original, labels)
    assert [entry[2]['kind'] for entry in recorded] == [
        'plan', 'extract', 'plan', 'extract_union_all']
    assert recorded[2][2]['query_kind'] == 'extract_union_all'
    for settings, _, metadata in recorded:
        assert settings is options
        assert metadata['seed_limit'] == 100
        assert metadata['variant'] == 'seed_count'
    assert labels['kind'] == 'extract'
