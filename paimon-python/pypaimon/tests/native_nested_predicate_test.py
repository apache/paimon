# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Existing projected ROW predicates must use native scans and reads."""

from contextlib import ExitStack
from datetime import datetime
from decimal import Decimal
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.native_plan import _predicate_to_native


pytestmark = pytest.mark.native_plan


def _table(path, mode):
    catalog = CatalogFactory.create({'warehouse': str(path)})
    catalog.create_database('db', True)
    detail = pa.struct([('score', pa.int32()), ('label', pa.string()),
                        ('amount', pa.decimal128(12, 2)), ('instant', pa.timestamp('ms'))])
    schema = pa.schema([('id', pa.int32()), ('profile', pa.struct([('detail', detail), ('unused', pa.string())])),
                       ('keep', pa.string())])
    options = {'bucket': '1' if mode == 'pk' else '-1', 'write.native.enabled': 'false',
               'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false', 'parquet.block.size': '1'}
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'pk' else None, options=options), False)
    table = catalog.get_table('db.t')

    def profile(score, label):
        return {'detail': {'score': score, 'label': label, 'amount': Decimal('1.25'),
                           'instant': datetime(2024, 2, 29, 12)}, 'unused': 'unselected'}
    initial = [{'id': 1, 'profile': profile(7, 'old'), 'keep': 'a'},
               {'id': 2, 'profile': profile(1, 'first'), 'keep': 'b'},
               {'id': 3, 'profile': None, 'keep': 'c'},
               {'id': 4, 'profile': {'detail': None, 'unused': 'x'}, 'keep': 'd'},
               {'id': 5, 'profile': profile(None, None), 'keep': 'e'},
               {'id': 6, 'profile': profile(9, '中文'), 'keep': 'f'}]
    changed = [{'id': 1, 'profile': profile(30, 'new'), 'keep': 'g'},
               {'id': 2, 'profile': profile(7, 'winner'), 'keep': 'h'}]
    for rows in [initial, changed]:
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    return table


_CASES = [
    ('score', 'equal', [7]), ('score', 'not_equal', [7]),
    ('score', 'less_than', [7]), ('score', 'greater_or_equal', [7]),
    ('score', 'is_null', []), ('score', 'is_not_null', []),
    ('score', 'is_in', [[None, 7, 9]]), ('score', 'is_not_in', [[7]]),
    ('score', 'between', [1, 9]), ('score', 'not_between', [1, 9]),
    ('label', 'equal', ['中文']), ('label', 'startswith', ['w']),
    ('label', 'contains', ['中']), ('label', 'endswith', ['文']),
    ('label', 'like', ['_文']), ('amount', 'equal', [Decimal('1.25')]),
    ('instant', 'greater_or_equal', [datetime(2024, 1, 1)]),
]


def _read(table, native, stream, case, projection, limit, compound):
    view = table.copy({'read.native.enabled': str(native).lower(),
                       'scan.native-plan.enabled': str(native).lower()})
    builder = view.new_stream_read_builder() if stream else view.new_read_builder()
    column, method, literals = case
    leaf = 'profile.detail.' + column
    builder.with_projection([leaf, 'id'])
    predicates = builder.new_predicate_builder()
    predicate = getattr(predicates, method)('profile_detail_' + column, *literals)
    wire = _predicate_to_native(predicate)
    assert wire['field'] == 'profile'
    assert wire['nested_path'] == ['detail', column]
    assert _predicate_to_native(predicate.new_index(predicate.index)) == wire
    assert _predicate_to_native(predicate.new_literals(predicate.literals)) == wire
    if compound:
        top = predicates.equal('id', 6)
        combine = PredicateBuilder.and_predicates if compound == 'and' else PredicateBuilder.or_predicates
        predicate = combine([predicate, top])
    builder.with_filter(predicate).with_projection(projection or [leaf, 'id'])
    if limit is not None:
        builder.with_limit(limit)
    if stream:
        # A snapshot plan isolates read semantics from the streaming poll loop.
        splits = view.new_read_builder().new_scan().plan().splits()
    else:
        scan = builder.new_scan()
        with ExitStack() as stack:
            if native:
                stack.enter_context(patch.object(scan.file_scanner, 'scan',
                                                 side_effect=AssertionError('Python scan fallback')))
            splits = scan.plan().splits()
    read = builder.new_read()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(read, '_create_split_read',
                                             side_effect=AssertionError('Python read fallback')))
        result = read.to_arrow_batch_reader(splits).read_all()
    return result.sort_by('id').to_pylist()


@pytest.mark.parametrize('mode', ['append', 'pk'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('case', _CASES, ids=[c[0] + '-' + c[1] for c in _CASES])
def test_nested_projection_filters_stay_native(tmp_path, mode, stream, case):
    table = _table(tmp_path, mode)
    expected = _read(table, False, stream, case, None, None, None)
    actual = _read(table, True, stream, case, None, None, None)
    assert actual == expected
    if case == ('score', 'equal', [7]):
        assert [row['id'] for row in actual] == ([2] if mode == 'pk' else [1, 2])
    elif case == ('score', 'is_null', []):
        assert [row['id'] for row in actual] == [3, 4, 5]


@pytest.mark.parametrize('mode', ['append', 'pk'])
@pytest.mark.parametrize('projection', [
    None, ['profile', 'profile.detail.score', 'id'],
    ['profile.detail.score', 'profile.detail.score', 'id'],
])
@pytest.mark.parametrize('compound', [None, 'and', 'or'])
@pytest.mark.parametrize('limit', [None, 1])
def test_nested_filter_composition_and_projection_changes(tmp_path, mode, projection, compound, limit):
    table = _table(tmp_path, mode)
    case = ('score', 'equal', [7])
    expected = _read(table, False, False, case, projection, limit, compound)
    actual = _read(table, True, False, case, projection, limit, compound)
    # LIMIT has no row-order contract. Here all matching IDs are equal or the
    # OR includes id6; validate limited rows against the complete result.
    if limit:
        allowed = _read(table, False, False, case, projection, None, compound)
        assert len(actual) == min(len(allowed), limit)
        assert all(row in allowed for row in actual)
    else:
        assert actual == expected


@pytest.mark.parametrize('mode', ['append', 'pk'])
@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('nested_first', [False, True])
@pytest.mark.parametrize('nested_filter', [False, True])
@pytest.mark.parametrize('partitioned', [False, True])
@pytest.mark.parametrize('compound', [None, 'and', 'or'])
def test_nested_predicate_alias_collisions(tmp_path, mode, native, nested_first, nested_filter, partitioned, compound):
    import pickle

    schema = pa.schema([('id', pa.int32()), ('a', pa.struct([('b', pa.int32())])), ('a_b', pa.int32())])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        schema, primary_keys=(['a_b', 'id'] if partitioned else ['id']) if mode == 'pk' else None,
        partition_keys=['a_b'] if partitioned else None,
        options={'bucket': '8' if mode == 'pk' else '-1', 'write.native.enabled': 'false',
                 'metadata.stats-mode': 'full'}), False)
    table = catalog.get_table('db.t')
    for row in [
        {'id': 11, 'a': {'b': 7}, 'a_b': 99},
        {'id': 12, 'a': {'b': 99}, 'a_b': 7},
        {'id': 13, 'a': None, 'a_b': 7},
    ]:
        writer_builder = table.new_batch_write_builder()
        writer, commit = writer_builder.new_write(), writer_builder.new_commit()
        try:
            # Separate files expose false stats pruning when the unrelated
            # real a_b column excludes the nested literal 7.
            writer.write_arrow(pa.Table.from_pylist([row], schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()

    table = table.copy({'read.native.enabled': str(native).lower(),
                        'scan.native-plan.enabled': str(native).lower()})
    projection = ['a.b', 'a_b', 'id'] if nested_first else ['a_b', 'a.b', 'id']
    builder = table.new_read_builder().with_projection(projection)
    # The second occurrence of a_b receives a suffix, whether its source is a
    # nested leaf or a real top-level column. Predicates retain that identity.
    alias = 'a_b' if nested_first == nested_filter else 'a_b__0'
    predicate = builder.new_predicate_builder().equal(alias, 7)
    predicate = pickle.loads(pickle.dumps(predicate.new_index(predicate.index)))
    wire = _predicate_to_native(predicate)
    assert wire['field'] == ('a' if nested_filter else 'a_b')
    assert wire.get('nested_path') == (['b'] if nested_filter else None)
    if partitioned and nested_first and nested_filter:
        with pytest.raises(ValueError, match='Partition filter must reference only partition keys'):
            builder.with_partition_filter(predicate)
    if compound == 'and':
        predicate = PredicateBuilder.and_predicates([
            predicate, builder.new_predicate_builder().less_or_equal('id', 12)])
    elif compound == 'or':
        predicate = PredicateBuilder.or_predicates([
            predicate, builder.new_predicate_builder().equal('id', 13)])
    builder.with_filter(predicate)
    scan, read = builder.new_scan(), builder.new_read()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(scan.file_scanner, 'scan',
                                             side_effect=AssertionError('Python scan fallback')))
            stack.enter_context(patch.object(read, '_create_split_read',
                                             side_effect=AssertionError('Python read fallback')))
        rows = read.to_arrow_batch_reader(scan.plan().splits()).read_all().to_pylist()
    expected = {11} if nested_filter else {12, 13}
    if compound == 'and':
        expected &= {11, 12}
    elif compound == 'or':
        expected |= {13}
    assert sorted(row['id'] for row in rows) == sorted(expected)


@pytest.mark.parametrize('mode', ['append', 'pk'])
@pytest.mark.parametrize('evolution', ['promote', 'rename', 'replace'])
@pytest.mark.parametrize('projection', [None, 'full'])
def test_nested_predicate_schema_evolution(tmp_path, mode, evolution, projection):
    from pypaimon.schema.data_types import AtomicType
    from pypaimon.schema.schema_change import SchemaChange

    table = _table(tmp_path, mode)
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    path = ['profile', 'detail', 'score']
    if evolution == 'promote':
        changes = [SchemaChange.update_column_type(path, AtomicType('BIGINT'))]
        case = ('score', 'equal', [7])
    elif evolution == 'rename':
        changes = [SchemaChange.rename_column(path, 'renamed')]
        case = ('renamed', 'equal', [7])
    else:
        changes = [SchemaChange.drop_column(path), SchemaChange.add_column(path, AtomicType('INT'))]
        case = ('score', 'is_null', [])
    catalog.alter_table('db.t', changes, False)
    table = catalog.get_table('db.t')
    if projection == 'full':
        projection = ['profile', 'profile.detail.' + case[0], 'id']
    expected = _read(table, False, False, case, projection, None, None)
    actual = _read(table, True, False, case, projection, None, None)
    assert actual == expected
