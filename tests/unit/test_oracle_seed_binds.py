"""Check repeated Oracle seed binds without a live Oracle connection."""
from collections import Counter
from types import SimpleNamespace

import pytest
from sqlalchemy import Integer, or_
from sqlalchemy.dialects import oracle

from datakiln.extraction.dialects import bind_oracle_seeds_as_lobs
from datakiln.extraction.models import FilterConfig
from datakiln.extraction.renderers import SQLGenerator
from datakiln.extraction.rewrites import disjoint_union_all


@pytest.fixture(name='compiled')
def fixture_compiled():
    """Compile the rewrite whose first seed bind appears in two branches."""
    predicates = []
    for column in ('first_id', 'second_id'):
        config = FilterConfig([column], {
            'schema': 'source', 'table': 'orders', 'columns': ['id']})
        predicates.append(config.to_where([(123,), (456,)], [Integer()], 100))
    statement = SQLGenerator.base_select('source', 'results').where(
        or_(*predicates))
    return disjoint_union_all(statement).compile(dialect=oracle.dialect())


@pytest.mark.parametrize('batch_size', [1, 2])
def test_repeated_seed_names_are_converted_once(compiled, batch_size):
    """Test each seed name becomes one LOB per execution parameter set."""
    calls = []

    def create_lob(_kind, value):
        # Match the driver constraint that exposed the second conversion.
        if not isinstance(value, (str, bytes)):
            raise TypeError('expecting string or bytes')
        lob = object()
        calls.append(lob)
        return lob

    cursor = SimpleNamespace(connection=SimpleNamespace(createlob=create_lob))
    context = SimpleNamespace(dialect=oracle.dialect(), compiled=compiled)
    parameters = []
    for _ in range(batch_size):
        parameters.append(dict(compiled.params))
    names = Counter(compiled.bind_names.values())
    seeds = set(name for name in names if name.startswith('seed_list_'))
    assert max(names[name] for name in seeds) > 1
    assert len(seeds) == 2

    bind_oracle_seeds_as_lobs(
        dict.fromkeys(compiled.bind_names, 'CLOB'), cursor, None,
        parameters, context)
    assert len(calls) == len(seeds) * batch_size
    for params in parameters:
        for name, value in params.items():
            if name in seeds:
                assert value in calls
            else:
                assert value == compiled.params[name]


def test_non_oracle_parameters_stay_unchanged(compiled):
    """Test other database dialects do not invoke Oracle LOB creation."""
    parameters = [dict(compiled.params)]
    context = SimpleNamespace(dialect=SimpleNamespace(name='postgresql'))
    bind_oracle_seeds_as_lobs(
        dict.fromkeys(compiled.bind_names, 'CLOB'), None, None,
        parameters, context)
    assert parameters == [compiled.params]
