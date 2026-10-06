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

"""One Java-compatible read type drives nested, MAP and VARIANT reads."""

import json
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.data.generic_variant import GenericVariant
from pypaimon.data.map_shared_shredding import is_map_selected_keys_field
from pypaimon.read.datasource.split_provider import CatalogSplitProvider, PreResolvedSplitProvider
from pypaimon.read.read_type import output_schema
from pypaimon.schema.data_types import PyarrowFieldParser, RowType

pytestmark = pytest.mark.native_plan


def _table(tmp_path, mode, shredded, variant_shredded=False):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    variant_type = GenericVariant.to_arrow_array([]).type
    schema = pa.schema([
        pa.field('id', pa.int32(), nullable=False),
        ('profile', pa.struct([('detail', pa.struct([('score', pa.int32()), ('unused', pa.string())])),
                               ('unused', pa.string())])),
        ('attrs', pa.map_(pa.string(), pa.int64())),
        *([('payload', variant_type)] if not shredded else []),
    ])
    options = {'read.native.enabled': 'true', 'write.native.enabled': 'true',
               'scan.native-plan.enabled': 'true', 'file.format': 'parquet'}
    if mode == 'pk':
        options['bucket'] = '1'
    elif mode == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    if variant_shredded:
        options['variant.shreddingSchema'] = json.dumps({'type': 'ROW', 'fields': [{
            'name': 'payload', 'type': {'type': 'ROW', 'fields': [
                {'name': 'ratio', 'type': 'DOUBLE'}, {'name': 'unused', 'type': 'STRING'}]}}]})
    if shredded:
        options.update({'fields.attrs.map.storage-layout': 'shared-shredding',
                        'fields.attrs.map.shared-shredding.max-columns': '1'})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'pk' else None, options=options), False)
    table = catalog.get_table('default.t')
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    values = [
        {'id': 1, 'profile': {'detail': {'score': 7, 'unused': 'x'}, 'unused': 'x'},
         'attrs': [('first', 10), ('overflow', 20), ('', 30)], 'payload': {'ratio': 1.25}},
        {'id': 2, 'profile': None, 'attrs': None, 'payload': None},
        {'id': 3, 'profile': {'detail': None, 'unused': 'x'}, 'attrs': [], 'payload': {}},
    ]

    def write(rows):
        variants = GenericVariant.to_arrow_array([
            GenericVariant.from_python(row['payload']) if row['payload'] is not None else None for row in rows])
        data = pa.table({name: variants if name == 'payload' else [row[name] for row in rows]
                         for name in schema.names}, schema=schema)
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(data)
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    write(values)
    if mode == 'pk':
        # Strict extraction must only see the winning row, after PK merging.
        write([dict(values[0], payload={'ratio': 2.5})])
        values[0]['payload'] = {'ratio': 2.5}
    return table, values


@pytest.mark.parametrize('mode', ['append', 'de', 'pk'])
@pytest.mark.parametrize('shredded', [False, True])
@pytest.mark.parametrize('stream', [False, True])
def test_nested_map_variant_share_read_type(tmp_path, mode, shredded, stream):
    table, values = _table(tmp_path, mode, shredded)
    projection = {'score': 'profile.detail.score', 'overflow': "attrs['overflow']",
                  'empty_key': "attrs['']", 'missing': "attrs['missing']",
                  'id_copy': 'id', 'id': 'id'}
    if not shredded:
        projection['ratio'] = "variant_get(payload, '$.ratio', 'float')"
    builder = table.new_stream_read_builder() if stream else table.new_read_builder()
    builder.with_projection(projection)
    requested = builder.read_type()
    assert [field.name for field in requested] == ['profile', 'attrs', 'id'] + (['payload'] if not shredded else [])
    assert isinstance(requested[0].type, RowType)
    assert [field.name for field in requested[0].type.fields] == ['detail']
    assert [field.name for field in requested[0].type.fields[0].type.fields] == ['score']
    assert is_map_selected_keys_field(requested[1])
    assert requested[1].description == '__PAIMON_MAP_SELECTED_KEYS:overflow;;missing'
    if not shredded:
        assert requested[3].type.fields[0].description == '__VARIANT_METADATA$.ratio;true;UTC'
    # Use a snapshot plan for stream readers so overlapping PK change records
    # still verify snapshot merging, independent of changelog policy.
    splits = table.new_read_builder().new_scan().plan().splits()
    read = builder.new_read()
    assert read.read_type == requested
    expected = [{'score': 7, 'overflow': 20, 'empty_key': 30, 'missing': None,
                 'ratio': values[0]['payload']['ratio'], 'id_copy': 1, 'id': 1},
                {'score': None, 'overflow': None, 'empty_key': None, 'missing': None,
                 'ratio': None, 'id_copy': 2, 'id': 2},
                {'score': None, 'overflow': None, 'empty_key': None, 'missing': None,
                 'ratio': None, 'id_copy': 3, 'id': 3}]
    if shredded:
        for row in expected:
            del row['ratio']
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        for parallelism in (1, 2):
            assert read.to_arrow(splits, parallelism=parallelism).sort_by('id').to_pylist() == expected
            result = read.to_arrow_batch_reader(splits, parallelism=parallelism).read_all()
            assert result.sort_by('id').to_pylist() == expected
    assert set(read._native_read_kwargs()) == {'predicate', 'limit', 'read_type'}


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('mode', ['append', 'de', 'pk'])
def test_list_output_order_and_nullable_parents(tmp_path, native, mode):
    table, _ = _table(tmp_path, mode, True)
    table = table.copy({'read.native.enabled': str(native).lower()})
    builder = table.new_read_builder().with_projection(['profile.detail.score', "attrs['overflow']", 'id'])
    read = builder.new_read()
    splits = builder.new_scan().plan().splits()
    expected = [{'profile_detail_score': 7, 'attrs_overflow': 20, 'id': 1},
                {'profile_detail_score': None, 'attrs_overflow': None, 'id': 2},
                {'profile_detail_score': None, 'attrs_overflow': None, 'id': 3}]
    if native:
        with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
            result = read.to_arrow(splits).sort_by('id').to_pylist()
    else:
        # ROW fallback in DE remains deliberately unsupported in the existing
        # Python split reader; Native is exercised above for that mode.
        if mode == 'de':
            with pytest.raises(NotImplementedError, match='ROW nested-field'):
                read.to_arrow(splits)
            return
        result = read.to_arrow(splits).sort_by('id').to_pylist()
        rows = [tuple(row.get_field(i) for i in range(3)) for row in read.to_iterator(splits)]
        assert sorted(rows, key=lambda row: row[2]) == [(7, 20, 1), (None, None, 2), (None, None, 3)]
    assert result == expected


def test_builder_replacement_and_ray_provider_use_one_read_type(tmp_path):
    table, _ = _table(tmp_path, 'append', False)
    selections = {'score': 'profile.detail.score', 'value': "attrs['overflow']",
                  'ratio': "try_variant_get(payload, '$.ratio', 'float')"}
    builder = table.new_read_builder().with_projection(selections)
    read = builder.new_read()
    provider = PreResolvedSplitProvider(table, [], read.read_type, output_projection=read.output_projection)
    catalog = CatalogSplitProvider('default.t', {'warehouse': str(tmp_path)}, projection=selections)
    assert provider.read_type() == catalog.read_type() == builder.read_type()
    assert provider.output_projection() == catalog.output_projection() == builder._output_projection
    assert output_schema(PyarrowFieldParser.from_paimon_schema(provider.read_type()),
                         provider.output_projection()).names == ['score', 'value', 'ratio']
    builder.with_projection(['id'])
    assert [field.name for field in builder.read_type()] == ['id']
    assert builder.new_read().to_arrow(builder.new_scan().plan().splits()).column_names == ['id']
    builder.with_projection([])
    empty = builder.new_read().to_arrow(builder.new_scan().plan().splits())
    assert empty.num_rows == 3 and empty.num_columns == 0
    assert not any(key in builder.__dict__ for key in ('_projection', '_nested_paths', '_resolved_read_type'))


@pytest.mark.parametrize('mode', ['append', 'de', 'pk'])
def test_nested_variant_and_map_types_are_structured_native_requests(tmp_path, mode):
    from pypaimon.read.variant_read_type import with_variant_extractions
    from pypaimon.read.read_type import project_read_type

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('outer', pa.struct([
        ('unused', pa.string()), ('payload', GenericVariant.to_arrow_array([]).type),
        ('attrs', pa.map_(pa.string(), pa.int64()))]))])
    options = {'file.format': 'parquet', 'read.native.enabled': 'true', 'write.native.enabled': 'true'}
    if mode == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
                        'data-evolution.nested-field.enabled': 'true'})
    if mode == 'pk':
        options['bucket'] = '1'
    catalog.create_table('default.nested', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'pk' else None, options=options), False)
    table = catalog.get_table('default.nested')
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    payload = GenericVariant.to_arrow_array([GenericVariant.from_python({'ratio': 1.25}), None, None])
    outer = pa.StructArray.from_arrays([
        pa.array(['x', None, 'y']), payload,
        pa.array([[('wanted', 10)], None, []], type=pa.map_(pa.string(), pa.int64()))],
        fields=list(schema.field('outer').type), mask=pa.array([False, True, False]))
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_arrays([pa.array([1, 2, 3], type=pa.int32()), outer], schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    requested = project_read_type(table.fields, [['outer', 'payload'], ['outer', 'attrs', 'wanted'], ['id']])
    requested = with_variant_extractions(requested, {('outer', 'payload'): {
        'paths': ['$.ratio'], 'target_type': pa.float32(), 'fail_on_error': True}})
    read_builder = table.new_read_builder().with_read_type(requested)
    read = read_builder.new_read()
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        result = read.to_arrow(read_builder.new_scan().plan().splits()).sort_by('id')
    assert result.to_pylist() == [
        {'outer': {'payload': {'0': 1.25}, 'attrs': {'wanted': 10}}, 'id': 1},
        {'outer': None, 'id': 2},
        {'outer': {'payload': None, 'attrs': {'wanted': None}}, 'id': 3}]
    named = table.new_read_builder().with_projection({
        'ratio': "variant_get(outer.payload, '$.ratio', 'float')",
        'wanted': "outer.attrs['wanted']", 'id': 'id'})
    read = named.new_read()
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        assert read.to_arrow(named.new_scan().plan().splits()).sort_by('id').to_pylist() == [
            {'ratio': 1.25, 'wanted': 10, 'id': 1}, {'ratio': None, 'wanted': None, 'id': 2},
            {'ratio': None, 'wanted': None, 'id': 3}]


@pytest.mark.parametrize('mode', ['append', 'de', 'pk'])
def test_shredded_variant_projection_retains_fallbacks_and_nulls(tmp_path, mode):
    table, values = _table(tmp_path, mode, False, variant_shredded=True)
    builder = table.new_read_builder().with_projection({
        'ratio': "variant_get(payload, '$.ratio', 'float')",
        'wrong_case': "try_variant_get(payload, '$.Ratio', 'float')",
        'id': 'id'})
    read = builder.new_read()
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        assert read.to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pylist() == [
            {'ratio': values[0]['payload']['ratio'], 'wrong_case': None, 'id': 1},
            {'ratio': None, 'wrong_case': None, 'id': 2},
            {'ratio': None, 'wrong_case': None, 'id': 3}]


@pytest.mark.parametrize('native', [False, True])
def test_explicit_nested_read_type_preserves_structured_output(tmp_path, native):
    table, _ = _table(tmp_path, 'append', False)
    from pypaimon.read.read_type import project_read_type
    table = table.copy({'read.native.enabled': str(native).lower()})
    fields = project_read_type(table.fields, [['profile', 'detail', 'score'], ['id']])
    builder = table.new_read_builder().with_read_type(fields)
    result = builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id')
    assert result.to_pylist() == [
        {'profile': {'detail': {'score': 7}}, 'id': 1}, {'profile': None, 'id': 2},
        {'profile': {'detail': None}, 'id': 3}]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('mode', ['append', 'de', 'pk'])
@pytest.mark.parametrize('entrypoint', ['read_type', 'projection'])
def test_empty_reader_request_keeps_filtered_rows_and_limit(tmp_path, native, mode, entrypoint):
    table, _ = _table(tmp_path, mode, True)
    table = table.copy({'read.native.enabled': str(native).lower()})
    builder = table.new_read_builder()
    if entrypoint == 'read_type':
        builder.with_read_type([])
    else:
        builder.with_projection([])
    builder.with_filter(table.new_read_builder().new_predicate_builder().greater_than('id', 1)).with_limit(1)
    splits = builder.new_scan().plan().splits()
    read = builder.new_read()
    for result in (read.to_arrow(splits), read.to_arrow_batch_reader(splits).read_all()):
        assert result.num_columns == 0
        assert result.num_rows == 1
    rows = list(read.to_iterator(splits))
    assert len(rows) == 1
    assert len(rows[0]) == 0


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('mode', ['append', 'de', 'pk'])
def test_named_map_alias_does_not_change_physical_predicate(tmp_path, native, mode):
    table, _ = _table(tmp_path, mode, True)
    table = table.copy({'read.native.enabled': str(native).lower()})
    builder = table.new_read_builder().with_projection({'id': "attrs['overflow']"})
    builder.with_filter(table.new_read_builder().new_predicate_builder().equal('id', 1))
    read = builder.new_read()
    splits = builder.new_scan().plan().splits()
    for result in (read.to_arrow(splits), read.to_arrow_batch_reader(splits).read_all()):
        assert result.to_pydict() == {'id': [20]}


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('mode', ['append', 'pk'])
@pytest.mark.parametrize('projection', [
    ['profile.detail.score', 'profile.detail.unused', 'profile.unused'],
    ['profile', 'profile.detail.score'],
    ['profile.detail.score', 'profile.detail.score'],
])
def test_leaf_predicate_survives_whole_row_and_all_children(tmp_path, native, mode, projection):
    table, _ = _table(tmp_path, mode, True)
    table = table.copy({'read.native.enabled': str(native).lower()})
    splits = table.new_read_builder().new_scan().plan().splits()
    builder = table.new_read_builder().with_projection(['profile.detail.score'])
    predicate = builder.new_predicate_builder().equal('profile_detail_score', 7)
    builder.with_filter(predicate).with_projection(projection)
    builder.new_predicate_builder().equal('profile_detail_score', 7)
    read = builder.new_read()
    leaf_index = projection.index('profile.detail.score')
    for result in (read.to_arrow(splits), read.to_arrow_batch_reader(splits).read_all()):
        assert result.num_rows == 1
        assert result.column(leaf_index).to_pylist() == [7]
    rows = [tuple(row.get_field(i) for i in range(len(row))) for row in read.to_iterator(splits)]
    assert len(rows) == 1
    assert rows[0][leaf_index] == 7
    if len(set(projection)) != len(projection):
        duplicate = builder.new_predicate_builder().equal('profile_detail_score__0', 7)
        builder.with_filter(duplicate)
        assert builder.new_read().to_arrow(splits).num_rows == 1


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('projection', [['profile'], ['id']])
@pytest.mark.parametrize('combine', ['leaf', 'and', 'or'])
def test_removed_leaf_predicate_is_rejected_instead_of_ignored(tmp_path, native, projection, combine):
    table, _ = _table(tmp_path, 'append', True)
    table = table.copy({'read.native.enabled': str(native).lower()})
    builder = table.new_read_builder().with_projection(['profile.detail.score'])
    predicate = builder.new_predicate_builder().equal('profile_detail_score', 7)
    if combine != 'leaf':
        physical = table.new_read_builder().new_predicate_builder().greater_than('id', 0)
        predicate = (PredicateBuilder.and_predicates if combine == 'and'
                     else PredicateBuilder.or_predicates)([predicate, physical])
    builder.with_filter(predicate).with_projection(projection)
    with pytest.raises(ValueError, match='profile_detail_score'):
        builder.new_read()


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
def test_whole_map_and_selected_key_share_the_reader(tmp_path, native, mode):
    table, _ = _table(tmp_path, mode, True)
    table = table.copy({'read.native.enabled': str(native).lower()})
    builder = table.new_read_builder().with_projection(['attrs', "attrs['overflow']", 'id'])
    splits = builder.new_scan().plan().splits()
    read = builder.new_read()
    for result in (read.to_arrow(splits), read.to_arrow_batch_reader(splits).read_all()):
        result = result.sort_by('id')
        assert result.column('attrs_overflow').to_pylist() == [20, None, None]
        assert result.column('attrs').to_pylist() == [[('first', 10), ('overflow', 20), ('', 30)], None, []]
    rows = sorted([(row.get_field(2), row.get_field(1)) for row in read.to_iterator(splits)])
    assert rows == [(1, 20), (2, None), (3, None)]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
@pytest.mark.parametrize('parent, child, mask_target', [
    ('profile', 'profile.detail.score', 'profile'),
    ('attrs', "attrs['overflow']", 'attrs'),
    ('attrs', "attrs['overflow']", 'id'),
])
def test_whole_parent_and_child_apply_authorization_before_extraction(
        tmp_path, native, mode, parent, child, mask_target):
    from pypaimon.catalog.filesystem_catalog import FileSystemCatalog
    from pypaimon.catalog.table_query_auth import TableQueryAuthResult

    table, _ = _table(tmp_path, mode, True)
    table = table.copy({'read.native.enabled': str(native).lower(), 'query-auth.enabled': 'true'})
    builder = table.new_read_builder().with_projection([parent, child, 'id'])
    auth = TableQueryAuthResult(None, {mask_target: json.dumps({'name': 'NULL'})})
    with patch.object(FileSystemCatalog, 'auth_table_query', return_value=auth):
        splits = builder.new_scan().plan().splits()
        read = builder.new_read()
        for result in (read.to_arrow(splits), read.to_arrow_batch_reader(splits).read_all()):
            assert result.num_rows == 3
            if mask_target == parent:
                assert result.column(0).to_pylist() == [None] * 3
                assert result.column(1).to_pylist() == [None] * 3
            else:
                assert result.column('id').to_pylist() == [None] * 3
                assert sorted(value for value in result.column(1).to_pylist() if value is not None) == [20]
        rows = [tuple(row.get_field(i) for i in range(len(row))) for row in read.to_iterator(splits)]
        assert len(rows) == 3
        if mask_target == parent:
            assert all(row[0] is None and row[1] is None for row in rows)
        else:
            assert all(row[2] is None for row in rows)
            assert sorted(row[1] for row in rows if row[1] is not None) == [20]


def test_clipped_variant_reads_only_selected_paths_for_all_json_kinds(tmp_path):
    table, _ = _table(tmp_path, 'append', False, variant_shredded=True)
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    variants = GenericVariant.to_arrow_array([GenericVariant.from_python(value) for value in [42, None, []]])
    data = pa.table({'id': [4, 5, 6], 'profile': [None] * 3,
                     'attrs': [[('wanted', 7)]] * 3, 'payload': variants}, schema=schema)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    for function in ['variant_get', 'try_variant_get']:
        reader = table.new_read_builder().with_projection({
            'ratio': "%s(payload, '$.ratio', 'float')" % function,
            'wanted': "attrs['wanted']", 'id': 'id'})
        read = reader.new_read()
        with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
            result = read.to_arrow(reader.new_scan().plan().splits()).sort_by('id').to_pylist()
        assert result == [{'ratio': 1.25, 'wanted': None, 'id': 1},
                          {'ratio': None, 'wanted': None, 'id': 2},
                          {'ratio': None, 'wanted': None, 'id': 3},
                          {'ratio': None, 'wanted': 7, 'id': 4},
                          {'ratio': None, 'wanted': 7, 'id': 5},
                          {'ratio': None, 'wanted': 7, 'id': 6}]


@pytest.mark.parametrize('default', [False, True])
def test_map_aggregation_projects_keys_after_merging_original_values(tmp_path, default):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('seq', pa.int32()), ('attrs', pa.map_(pa.string(), pa.int64()))])
    options = {'bucket': '1', 'merge-engine': 'partial-update', 'read.native.enabled': 'true',
               'write.native.enabled': 'true', 'fields.seq.sequence-group': 'attrs'}
    options['fields.default-aggregate-function' if default else 'fields.attrs.aggregate-function'] = 'merge_map'
    catalog.create_table('default.agg', Schema.from_pyarrow_schema(schema, primary_keys=['id'], options=options), False)
    table = catalog.get_table('default.agg')
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    for seq, attrs in [(1, [('a', 7)]), (2, [('b', 9)])]:
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.table({'id': [1], 'seq': [seq], 'attrs': [attrs]}, schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    builder = table.new_read_builder().with_projection({'a': "attrs['a']", 'b': "attrs['b']"})
    read = builder.new_read()
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        assert read.to_arrow(builder.new_scan().plan().splits()).to_pylist() == [{'a': 7, 'b': 9}]


def test_reversed_nested_variant_fields_keep_their_own_paths(tmp_path):
    from pypaimon.read.read_type import project_read_type
    from pypaimon.read.variant_read_type import with_variant_extractions

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    variant = GenericVariant.to_arrow_array([]).type
    schema = pa.schema([('outer', pa.struct([('inner', pa.struct([
        ('v1', variant), ('v2', variant), ('unused', pa.int32())])), ('ordinary', pa.int32())]))])
    catalog.create_table('default.reverse', Schema.from_pyarrow_schema(schema, options={
        'read.native.enabled': 'true', 'write.native.enabled': 'true', 'file.format': 'parquet'}), False)
    table = catalog.get_table('default.reverse')
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    v1 = GenericVariant.to_arrow_array([GenericVariant.from_python({'a': 7})])
    v2 = GenericVariant.to_arrow_array([GenericVariant.from_python({'b': 9})])
    inner_type = schema.field('outer').type['inner'].type
    inner = pa.StructArray.from_arrays([v1, v2, pa.array([123], type=pa.int32())], fields=list(inner_type))
    outer = pa.StructArray.from_arrays([inner, pa.array([5], type=pa.int32())], fields=list(schema.field('outer').type))
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_arrays([outer], schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    requested = project_read_type(table.fields, [
        ['outer', 'inner', 'v2'], ['outer', 'inner', 'v1'], ['outer', 'ordinary']])
    requested = with_variant_extractions(requested, {
        ('outer', 'inner', name): {'paths': [path], 'target_type': pa.float32(), 'fail_on_error': True}
        for name, path in [('v2', '$.b'), ('v1', '$.a')]})
    builder = table.new_read_builder().with_read_type(requested)
    read = builder.new_read()
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        assert read.to_arrow(builder.new_scan().plan().splits()).to_pylist() == [
            {'outer': {'inner': {'v2': {'0': 9.0}, 'v1': {'0': 7.0}}, 'ordinary': 5}}]
    builder.with_projection({'second': "variant_get(outer.inner.v2, '$.b', 'float')",
                             'first': "variant_get(outer.inner.v1, '$.a', 'float')", 'ordinary': 'outer.ordinary'})
    read = builder.new_read()
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        assert read.to_arrow(builder.new_scan().plan().splits()).to_pylist() == [
            {'second': 9.0, 'first': 7.0, 'ordinary': 5}]
