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

import datetime
import json

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.table_read import TableRead
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind


def field_ref(name, index):
    return {'name': 'FIELD_REF', 'fieldRef': {'name': name, 'index': index, 'type': 'STRING'}}


def rules(mask=None, row_filter=None):
    return TableQueryAuthResult(
        [row_filter] if row_filter is not None else None,
        {'secret': json.dumps(mask or {'name': 'NULL'})})


def arrow_rows(data):
    return [dict(zip(data.column_names, values))
            for values in zip(*(column.to_pylist() for column in data.columns))]


@pytest.fixture(params=['append', 'primary-key', 'data-evolution',
                        'partitioned-append', 'partitioned-primary-key'])
def table(request, tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    fields = pa.schema([('id', pa.int32()), ('secret', pa.string()), ('salt', pa.string())])
    options = {'file.format': 'parquet', 'write.native.enabled': 'false',
               'source.split.target-size': '1 b', 'source.split.open-file-cost': '1 b'}
    if request.param == 'data-evolution':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    primary_key = 'primary-key' in request.param
    partitions = ['secret'] if request.param.startswith('partitioned') else []
    if primary_key:
        options['bucket'] = '1'
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        fields, primary_keys=['id'] + partitions if primary_key else [],
        partition_keys=partitions, options=options), False)
    raw = catalog.get_table('db.t')
    rows = [{'id': 1, 'secret': 'alpha', 'salt': 'hidden'},
            {'id': 2, 'secret': 'beta', 'salt': 'visible'},
            {'id': 3, 'secret': 'gamma', 'salt': 'hidden'}]
    if primary_key:
        rows.append(rows[0])
    for row in rows:
        builder = raw.new_batch_write_builder()
        writer = builder.new_write()
        try:
            writer.write_arrow(pa.Table.from_pydict({name: [value] for name, value in row.items()}, schema=fields))
            builder.new_commit().commit(writer.prepare_commit())
        finally:
            writer.close()
    return catalog.get_table('db.t').copy({'query-auth.enabled': 'true'})


def authorize(table, result):
    table.catalog_environment.table_query_auth = lambda options, identifier: lambda select: result


def read(table, predicate, projection=None, limit=None, method='arrow'):
    builder = table.new_read_builder().with_filter(predicate)
    if projection is not None:
        builder.with_projection(projection)
    if limit is not None:
        builder.with_limit(limit)
    splits = builder.new_scan().plan().splits()
    reader = builder.new_read()
    if method == 'iterator':
        return [tuple(row.get_field(i) for i in range(len(reader.read_type)))
                for row in reader.to_iterator(splits)]
    if method == 'batches':
        return arrow_rows(reader.to_arrow_batch_reader(splits).read_all())
    return arrow_rows(reader.to_arrow(splits))


def stream_read(table, builder, phase, follow_up_snapshot=2):
    scan = builder.new_streaming_scan()
    snapshots = table.snapshot_manager()
    try:
        if phase == 'initial':
            plan = scan._create_initial_plan(snapshots.get_latest_snapshot())
        elif phase == 'follow_up':
            plan = scan._create_follow_up_plan(snapshots.get_snapshot_by_id(follow_up_snapshot))
        else:
            plan = scan._create_catch_up_plan(follow_up_snapshot, snapshots.get_latest_snapshot())
        return arrow_rows(builder.new_read().to_arrow(plan.splits()))
    finally:
        scan._prefetch_executor.shutdown(wait=True)


def selection_sensitive(table, masked_rules):
    selected = []

    def auth(select):
        selected.append(select)
        return masked_rules if 'secret' in select else TableQueryAuthResult(None, None)

    table.catalog_environment.table_query_auth = lambda options, identifier: auth
    return selected


@pytest.mark.parametrize('condition,expected', [('is_null', [1, 2, 3]), ('raw_equal', [])])
def test_user_filter_sees_masked_values(table, condition, expected):
    authorize(table, rules())
    predicates = PredicateBuilder(table.fields)
    predicate = predicates.is_null('secret') if condition == 'is_null' else predicates.equal('secret', 'alpha')
    rows = read(table, predicate)
    assert sorted(row['id'] for row in rows) == expected
    assert all(row['secret'] is None for row in rows)


@pytest.mark.parametrize('method', ['arrow', 'batches', 'iterator'])
def test_unprojected_mask_target_and_input(table, method):
    authorize(table, rules(field_ref('salt', 2)))
    predicate = PredicateBuilder(table.fields).equal('secret', 'visible')
    rows = read(table, predicate, projection=['id'], method=method)
    assert rows == ([(2,)] if method == 'iterator' else [{'id': 2}])


@pytest.mark.parametrize('compound,expected', [('and', [2, 3]), ('or', [1, 2, 3])])
def test_compound_predicate_keeps_masked_condition_intact(table, compound, expected):
    authorize(table, rules())
    predicates = PredicateBuilder(table.fields)
    combine = PredicateBuilder.and_predicates if compound == 'and' else PredicateBuilder.or_predicates
    predicate = combine([predicates.is_null('secret'), predicates.greater_than('id', 1)])
    assert sorted(row['id'] for row in read(table, predicate, projection=['id'])) == expected


def test_limit_is_applied_after_masked_filter(table):
    authorize(table, rules(field_ref('salt', 2)))
    predicate = PredicateBuilder(table.fields).equal('secret', 'visible')
    assert read(table, predicate, projection=['id'], limit=1) == [{'id': 2}]


def test_plan_all_files_authorizes_every_column_before_exposing_inventory(table):
    from pypaimon.catalog.catalog_exception import TableNoPermissionException
    table = table.copy({'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false'})
    selected = []

    def auth(select):
        selected.append(select)
        if select is None:
            raise TableNoPermissionException(table.identifier)
        return TableQueryAuthResult(None, None)

    table.catalog_environment.table_query_auth = lambda options, identifier: auth
    scan = table.new_read_builder().with_projection(['id']).new_scan()
    assert scan.plan().splits()
    with pytest.raises(TableNoPermissionException):
        scan.plan_all_files()
    assert selected == [['id'], None]


def test_filter_columns_are_authorized(table):
    selected = []

    def auth(select):
        selected.append(select)
        return rules(field_ref('salt', 2)) if 'secret' in select else TableQueryAuthResult(None, None)

    table.catalog_environment.table_query_auth = lambda options, identifier: auth
    predicate = PredicateBuilder(table.fields).equal('secret', 'visible')
    assert read(table, predicate, projection=['id']) == [{'id': 2}]
    assert selected == [['id', 'secret']]


@pytest.mark.parametrize('table', ['partitioned-append', 'partitioned-primary-key'], indirect=True)
def test_partition_filter_columns_are_authorized(table):
    selected = selection_sensitive(table, rules())
    predicate = PredicateBuilder(table.fields).equal('secret', 'alpha')
    scan = table.new_read_builder().with_projection(['id']).with_partition_filter(predicate).new_scan()
    with pytest.raises(ValueError, match='partition filters cannot reference masked columns'):
        scan.plan()
    assert selected == [['id', 'secret']]


@pytest.mark.parametrize('phase', ['initial', 'follow_up', 'catch_up'])
def test_streaming_filter_columns_are_authorized(table, phase):
    selected = selection_sensitive(table, rules(field_ref('salt', 2)))
    predicate = PredicateBuilder(table.fields).equal('secret', 'visible')
    builder = (table.new_stream_read_builder().with_filter(predicate)
               .with_projection(['id']).with_include_row_kind(False))
    assert stream_read(table, builder, phase) == [{'id': 2}]
    assert selected and all(select == ['id', 'secret'] for select in selected)


@pytest.mark.parametrize('masked_value,expected', [('visible', [2, 3]), ('beta', [3])])
def test_or_arms_match_independently(table, masked_value, expected):
    authorize(table, rules(field_ref('salt', 2)))
    predicates = PredicateBuilder(table.fields)
    # id 2 matches only the masked arm, id 3 only the other; 'beta' is id 2's raw value
    predicate = PredicateBuilder.or_predicates([
        predicates.equal('secret', masked_value), predicates.equal('id', 3)])
    assert sorted(row['id'] for row in read(table, predicate, projection=['id'])) == expected


def test_unmasked_conjunct_is_pushed_to_split_reads(table, monkeypatch):
    authorize(table, rules())
    pushed = []
    create_split_read = TableRead._create_split_read

    def recording(self, *args, **kwargs):
        split_read = create_split_read(self, *args, **kwargs)
        pushed.append(split_read.predicate)
        return split_read

    monkeypatch.setattr(TableRead, '_create_split_read', recording)
    predicates = PredicateBuilder(table.fields)
    unmasked = predicates.greater_than('id', 1)
    for combine, expected_push, expected_ids in [
            (PredicateBuilder.and_predicates, unmasked, [2, 3]),
            (PredicateBuilder.or_predicates, None, [1, 2, 3])]:
        del pushed[:]
        predicate = combine([unmasked, predicates.is_null('secret')])
        assert sorted(row['id'] for row in read(table, predicate, projection=['id'])) == expected_ids
        assert pushed and all(p == expected_push for p in pushed)


def test_row_authorization_precedes_mask_and_user_filter(table):
    row_filter = json.dumps({'kind': 'LEAF', 'transform': field_ref('secret', 1),
                             'function': 'EQUAL', 'literals': ['beta']})
    authorize(table, rules(row_filter=row_filter))
    predicate = PredicateBuilder(table.fields).is_null('secret')
    assert read(table, predicate, projection=['id', 'secret']) == [{'id': 2, 'secret': None}]


def test_row_only_authorization_with_user_filter(table):
    row_filter = json.dumps({'kind': 'LEAF', 'transform': field_ref('secret', 1),
                             'function': 'EQUAL', 'literals': ['beta']})
    authorize(table, TableQueryAuthResult([row_filter], None))
    predicate = PredicateBuilder(table.fields).greater_than('id', 1)
    assert read(table, predicate, projection=['id', 'secret']) == [{'id': 2, 'secret': 'beta'}]


def test_unmasked_conjunct_keeps_its_pushdown(table):
    authorize(table, rules())
    predicates = PredicateBuilder(table.fields)
    unmasked = predicates.greater_than('id', 1)
    predicate = PredicateBuilder.and_predicates([unmasked, predicates.is_null('secret')])
    scan = table.new_read_builder().with_filter(predicate).new_scan()
    scan.plan()
    assert scan.file_scanner.predicate == unmasked
    assert scan.file_scanner.auth_has_non_partition_filter


def test_same_scan_refreshes_masked_filter_rules(table):
    state = [rules()]
    table.catalog_environment.table_query_auth = lambda options, identifier: lambda select: state[0]
    predicate = PredicateBuilder(table.fields).is_null('secret')
    builder = table.new_read_builder().with_filter(predicate)
    scan = builder.new_scan()
    reader = builder.new_read()
    for auth, ids in [(rules(), [1, 2, 3]), (TableQueryAuthResult(None, None), []), (rules(), [1, 2, 3])]:
        state[0] = auth
        rows = arrow_rows(reader.to_arrow(scan.plan().splits()))
        assert sorted(row['id'] for row in rows) == ids


def test_explain_keeps_masked_filter_candidates(table):
    authorize(table, rules())
    predicate = PredicateBuilder(table.fields).is_null('secret')
    builder = table.new_read_builder().with_filter(predicate)
    plan, _ = builder.new_scan().scan_with_stats()
    rows = arrow_rows(builder.new_read().to_arrow(plan.splits()))
    assert sorted(row['id'] for row in rows) == [1, 2, 3]


@pytest.mark.parametrize('table', ['partitioned-append', 'partitioned-primary-key'], indirect=True)
def test_explicit_partition_filter_on_masked_key_is_rejected(table):
    authorize(table, rules())
    predicate = PredicateBuilder(table.fields).equal('secret', 'alpha')
    scan = table.new_read_builder().with_partition_filter(predicate).new_scan()
    with pytest.raises(ValueError, match='partition filters cannot reference masked columns'):
        scan.plan()


@pytest.mark.parametrize('phase', ['initial', 'follow_up', 'catch_up'])
def test_streaming_user_filter_runs_after_masking(table, phase):
    authorize(table, rules(field_ref('salt', 2)))
    predicate = PredicateBuilder(table.fields).equal('secret', 'visible')
    builder = (table.new_stream_read_builder().with_filter(predicate)
               .with_projection(['id']).with_include_row_kind(False))
    assert stream_read(table, builder, phase) == [{'id': 2}]


def test_row_predicate_indices_follow_the_widened_read_type(table):
    authorize(table, rules(field_ref('salt', 2)))
    predicate = PredicateBuilder(table.fields).startswith('secret', 'vis')
    assert read(table, predicate, projection=['salt', 'id']) == [{'salt': 'visible', 'id': 2}]


def test_write_scan_restores_filter_after_rules_are_removed(table):
    state = [rules()]
    table.catalog_environment.table_query_auth = lambda options, identifier: lambda select: state[0]
    predicate = PredicateBuilder(table.fields).is_null('secret')
    builder = table.new_read_builder().with_filter(predicate)
    scan = builder.new_scan()
    scan.plan()
    state[0] = None
    splits = scan.plan_all_files().splits()
    assert scan.file_scanner.predicate == predicate
    assert not scan.file_scanner.auth_has_non_partition_filter
    assert arrow_rows(builder.new_read().to_arrow(splits)) == []


def test_zero_column_projection_preserves_filtered_row_count(table):
    authorize(table, rules())
    predicate = PredicateBuilder(table.fields).is_null('secret')
    builder = table.new_read_builder().with_filter(predicate).with_projection([])
    result = builder.new_read().to_arrow(builder.new_scan().plan().splits())
    assert result.num_columns == 0
    assert result.num_rows == 3


@pytest.mark.parametrize('table', ['primary-key', 'partitioned-primary-key'], indirect=True)
@pytest.mark.parametrize('include_row_kind', [False, True])
@pytest.mark.parametrize('phase', ['initial', 'follow_up'])
def test_streaming_string_filter_preserves_row_kind(table, include_row_kind, phase):
    authorize(table, rules(field_ref('salt', 2)))
    predicates = PredicateBuilder(table.fields)
    predicate = PredicateBuilder.and_predicates([
        predicates.startswith('secret', 'vis'), predicates.equal('id', 2)])
    builder = (table.new_stream_read_builder().with_filter(predicate)
               .with_projection(['id']).with_include_row_kind(include_row_kind))
    expected = {'id': 2}
    if include_row_kind:
        expected['_row_kind'] = '+I'
    assert stream_read(table, builder, phase) == [expected]


@pytest.mark.parametrize('table', ['partitioned-append'], indirect=True)
def test_chunk_shuffle_rejects_post_mask_partition_filter(table):
    authorize(table, rules(field_ref('salt', 2)))
    predicate = PredicateBuilder(table.fields).equal('secret', 'visible')
    scan = table.new_read_builder().with_filter(predicate).new_scan().with_chunk_shuffle(1, 1)
    with pytest.raises(ValueError, match='chunk_shuffle.*authorization'):
        scan.plan()


@pytest.mark.parametrize('table', ['partitioned-append'], indirect=True)
@pytest.mark.parametrize('with_partition_filter', [False, True])
def test_chunk_shuffle_keeps_safe_authorized_chunks(table, with_partition_filter):
    authorize(table, TableQueryAuthResult(None, {'salt': json.dumps({'name': 'NULL'})}))
    builder = table.new_read_builder()
    if with_partition_filter:
        builder.with_filter(PredicateBuilder(table.fields).equal('secret', 'alpha'))
    splits = builder.new_scan().with_chunk_shuffle(1, 1).plan().splits()
    chunks = [arrow_rows(builder.new_read().to_arrow([split])) for split in splits]
    assert all(len(chunk) == 1 for chunk in chunks)
    assert sorted(row['id'] for chunk in chunks for row in chunk) == ([1] if with_partition_filter else [1, 2, 3])
    assert all(row['salt'] is None for chunk in chunks for row in chunk)


def table_from_rows(tmp_path, fields, rows, primary_keys=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    options = {'file.format': 'parquet', 'write.native.enabled': 'false'}
    if primary_keys:
        options['bucket'] = '1'
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        fields, primary_keys=primary_keys or [], options=options), False)
    table = catalog.get_table('db.t')
    for values in [rows, rows[:1]] if primary_keys else [rows]:
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        try:
            writer.write_arrow(pa.Table.from_pydict(
                {name: [row[name] for row in values] for name in fields.names}, schema=fields))
            builder.new_commit().commit(writer.prepare_commit())
        finally:
            writer.close()
    return catalog.get_table('db.t').copy({'query-auth.enabled': 'true'})


def test_post_mask_row_filter_preserves_null_maps(tmp_path):
    fields = pa.schema([('id', pa.int32()), ('s', pa.string()), ('m', pa.map_(pa.string(), pa.int64()))])
    table = table_from_rows(tmp_path, fields, [
        {'id': 1, 's': 'a', 'm': None}, {'id': 2, 's': 'a', 'm': []},
        {'id': 3, 's': 'b', 'm': None}, {'id': 4, 's': 'a', 'm': [('key', 1)]},
    ], primary_keys=['id'])
    authorize(table, TableQueryAuthResult(None, {'s': json.dumps({
        'name': 'UPPER', 'inputs': [{'index': 1, 'name': 's', 'type': 'STRING'}]})}))
    predicates = PredicateBuilder(table.fields)
    predicate = PredicateBuilder.and_predicates([predicates.startswith('s', 'A'), predicates.is_null('m')])
    assert read(table, predicate, projection=['id', 'm']) == [{'id': 1, 'm': None}]


@pytest.mark.parametrize('method', ['arrow', 'iterator'])
@pytest.mark.parametrize('compound', [None, 'or-null', 'and-string'])
def test_post_mask_filter_uses_declared_predicate_types(tmp_path, method, compound):
    fields = pa.schema([('id', pa.int32()), ('score', pa.int32()), ('label', pa.string())])
    table = table_from_rows(tmp_path, fields, [
        {'id': 1, 'score': 100, 'label': 'keep'}, {'id': 2, 'score': 10, 'label': 'keep'},
        {'id': 10, 'score': 2, 'label': 'keep'}, {'id': 100, 'score': 1, 'label': 'keep'}])
    authorize(table, TableQueryAuthResult(None, {'id': json.dumps({
        'name': 'CAST', 'fieldRef': {'index': 1, 'name': 'score', 'type': 'INT'}, 'type': 'STRING'})}))
    predicates = PredicateBuilder(table.fields)
    predicate = predicates.greater_than('id', 2)
    if compound == 'or-null':
        predicate = PredicateBuilder.or_predicates([predicate, predicates.not_equal('score', None)])
    elif compound == 'and-string':
        predicate = PredicateBuilder.and_predicates([predicate, predicates.startswith('label', 'keep')])
    result = read(table, predicate, projection=['id'], method=method)
    assert result == ([('100',), ('10',)] if method == 'iterator' else [{'id': 100}, {'id': 10}])


@pytest.mark.parametrize('condition,pattern', [
    ('equal', '100'), ('startswith', '1'), ('endswith', '00'), ('contains', '0'), ('like', '1_0')])
@pytest.mark.parametrize('with_equality', [False, True])
@pytest.mark.parametrize('method', ['arrow', 'batches'])
def test_post_mask_string_filter_uses_declared_type(tmp_path, condition, pattern, with_equality, method):
    fields = pa.schema([('id', pa.int32()), ('label', pa.string()), ('score', pa.int32())])
    table = table_from_rows(tmp_path, fields, [
        {'id': 1, 'label': 'hidden', 'score': 100}, {'id': 2, 'label': '100', 'score': 234},
        {'id': 3, 'label': '100', 'score': None}])
    authorize(table, TableQueryAuthResult(None, {'label': json.dumps({
        'name': 'FIELD_REF', 'fieldRef': {'index': 2, 'name': 'score', 'type': 'INT'}})}))
    predicates = PredicateBuilder(table.fields)
    predicate = getattr(predicates, condition)('label', pattern)
    if with_equality:
        predicate = PredicateBuilder.and_predicates([predicate, predicates.equal('label', '100')])
    assert read(table, predicate, projection=['id', 'label'], method=method) == [{'id': 1, 'label': '100'}]


@pytest.mark.parametrize('table', ['primary-key', 'partitioned-primary-key'], indirect=True)
def test_streaming_post_mask_filter_keeps_delete_row_kind(table):
    builder = table.copy({'query-auth.enabled': 'false'}).new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_row(GenericRow([2, 'beta', 'visible'], table.fields, RowKind.DELETE))
        builder.new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()
    authorize(table, rules(field_ref('salt', 2)))
    predicate = PredicateBuilder(table.fields).equal('secret', 'visible')
    read_builder = (table.new_stream_read_builder().with_filter(predicate)
                    .with_projection(['id']).with_include_row_kind(True))
    delete_snapshot = table.snapshot_manager().get_latest_snapshot().id
    rows = stream_read(table, read_builder, 'follow_up', follow_up_snapshot=delete_snapshot)
    assert rows == [{'_row_kind': '-D', 'id': 2}]


@pytest.mark.parametrize('method', ['arrow', 'iterator'])
@pytest.mark.parametrize('with_other_arm', [False, True])
def test_not_in_with_null_skips_masked_value_conversion(tmp_path, method, with_other_arm):
    fields = pa.schema([('id', pa.int32()), ('salt', pa.string())])
    table = table_from_rows(tmp_path, fields, [{'id': 1, 'salt': 'hidden'}, {'id': 2, 'salt': 'kept'}])
    # masked INT values are strings that cannot be cast back
    authorize(table, TableQueryAuthResult(None, {'id': json.dumps(field_ref('salt', 1))}))
    predicates = PredicateBuilder(table.fields)
    predicate = predicates.is_not_in('id', [2, None])
    if with_other_arm:
        predicate = PredicateBuilder.or_predicates([predicate, predicates.equal('salt', 'kept')])
    rows = read(table, predicate, projection=['salt'], method=method)
    if not with_other_arm:
        assert rows == []
    else:
        assert rows == ([('kept',)] if method == 'iterator' else [{'salt': 'kept'}])


@pytest.mark.parametrize('method', ['arrow', 'iterator'])
@pytest.mark.parametrize('case,expected', [
    ('startswith', [1, 2]), ('equal', [1, 2]), ('null-literal', [3]), ('is-null', [2, 4])])
def test_masked_values_outside_declared_type_compare_as_null(tmp_path, method, case, expected):
    fields = pa.schema([('id', pa.int32()), ('score', pa.int32()), ('secret', pa.int32()), ('public', pa.string())])
    # one batch: secret reads '100' and '7' as INT, 'REDACTED' and 'ALLOW' as NULL
    table = table_from_rows(tmp_path, fields, [
        {'id': 1, 'score': None, 'secret': 1, 'public': '100'},
        {'id': 2, 'score': 1, 'secret': 2, 'public': 'REDACTED'},
        {'id': 3, 'score': None, 'secret': 3, 'public': '7'},
        {'id': 4, 'score': 4, 'secret': 4, 'public': 'ALLOW'}])
    authorize(table, TableQueryAuthResult(None, {'secret': json.dumps(field_ref('public', 3))}))
    p = PredicateBuilder(table.fields)
    predicate = {
        'startswith': PredicateBuilder.or_predicates([p.startswith('public', 'R'), p.greater_than('secret', 50)]),
        'equal': PredicateBuilder.or_predicates([p.equal('public', 'REDACTED'), p.greater_than('secret', 50)]),
        'null-literal': PredicateBuilder.or_predicates([
            p.not_equal('score', None), p.less_than('secret', 50),
            PredicateBuilder.and_predicates([p.equal('public', 'ALLOW'), p.greater_than('secret', 50)])]),
        'is-null': p.is_null('secret'),
    }[case]
    rows = read(table, predicate, projection=['id'], method=method)
    assert rows == ([(i,) for i in expected] if method == 'iterator' else [{'id': i} for i in expected])


@pytest.mark.parametrize('kind', ['ARRAY', 'ROW', 'MAP'])
def test_null_checks_on_retyped_nested_masks(tmp_path, kind):
    target, source, values, type_name = {
        'ARRAY': (pa.list_(pa.int32()), pa.list_(pa.string()), [['1'], ['redacted'], None, ['3']], 'ARRAY<STRING>'),
        'ROW': (pa.struct([('a', pa.int32())]), pa.struct([('a', pa.string())]),
                [{'a': '1'}, {'a': 'redacted'}, None, {'a': '3'}], 'ROW<a STRING>'),
        'MAP': (pa.map_(pa.string(), pa.int32()), pa.map_(pa.string(), pa.string()),
                [[('k', '1')], [('k', 'redacted')], None, [('k', '3')]], 'MAP<STRING, STRING>'),
    }[kind]
    fields = pa.schema([('id', pa.int32()), ('v', target), ('src', source)])
    table = table_from_rows(tmp_path, fields, [{'id': i + 1, 'v': None, 'src': v} for i, v in enumerate(values)])
    authorize(table, TableQueryAuthResult(None, {'v': json.dumps(
        {'name': 'FIELD_REF', 'fieldRef': {'name': 'src', 'index': 2, 'type': type_name}})}))
    predicates = PredicateBuilder(table.fields)
    assert read(table, predicates.is_null('v'), projection=['id']) == [{'id': 3}]
    assert read(table, predicates.is_not_null('v'), projection=['id']) == [{'id': 1}, {'id': 2}, {'id': 4}]


@pytest.mark.parametrize('target,source,type_name,values,literal', [
    (pa.int32(), pa.float64(), 'DOUBLE', [1.5, 0.5], 1),
    (pa.int32(), pa.int64(), 'BIGINT', [2 ** 40, 1], 5),
    (pa.timestamp('ms'), pa.timestamp('us'), 'TIMESTAMP(6)', [1500, 500],
     datetime.datetime(1970, 1, 1, microsecond=1000))])
def test_numeric_and_timestamp_masks_compare_exactly(tmp_path, target, source, type_name, values, literal):
    fields = pa.schema([('id', pa.int32()), ('v', target), ('src', source)])
    table = table_from_rows(tmp_path, fields, [{'id': i + 1, 'v': None, 'src': v} for i, v in enumerate(values)])
    authorize(table, TableQueryAuthResult(None, {'v': json.dumps(
        {'name': 'FIELD_REF', 'fieldRef': {'name': 'src', 'index': 2, 'type': type_name}})}))
    predicate = PredicateBuilder(table.fields).greater_than('v', literal)
    assert read(table, predicate, projection=['id']) == [{'id': 1}]


def test_nested_filter_alias_authorizes_its_source_column(tmp_path):
    fields = pa.schema([('id', pa.int32()), ('s', pa.struct([('a', pa.string()), ('b', pa.int32())]))])
    table = table_from_rows(tmp_path, fields, [
        {'id': 1, 's': {'a': 'keep', 'b': 1}}, {'id': 2, 's': {'a': 'drop', 'b': 2}}])
    selected = []

    def auth(select):
        selected.append(select)
        return TableQueryAuthResult(None, None)

    table.catalog_environment.table_query_auth = lambda options, identifier: auth
    builder = table.new_read_builder().with_projection(['id', 's.a'])
    builder.with_filter(builder.new_predicate_builder().equal('s_a', 'keep'))
    assert arrow_rows(builder.new_read().to_arrow(builder.new_scan().plan().splits())) == [{'id': 1, 's_a': 'keep'}]
    assert selected == [['id', 's']]


def test_unmasked_filter_keeps_unprojected_columns_with_nested_projection(tmp_path):
    fields = pa.schema([('id', pa.int32()), ('s', pa.struct([('a', pa.string()), ('b', pa.int32())])),
                        ('secret', pa.string())])
    table = table_from_rows(tmp_path, fields, [
        {'id': 1, 's': {'a': 'keep', 'b': 1}, 'secret': 'x'}, {'id': 2, 's': {'a': 'drop', 'b': 2}, 'secret': 'y'},
        {'id': 3, 's': {'a': 'drop', 'b': 3}, 'secret': 'z'}])
    authorize(table, rules())
    builder = table.new_read_builder().with_projection(['s.a', 'secret'])
    # the nested split read cannot evaluate this OR, since 'id' is not projected
    builder.with_filter(PredicateBuilder.or_predicates([
        builder.new_predicate_builder().equal('s_a', 'keep'), PredicateBuilder(table.fields).equal('id', 2)]))
    rows = arrow_rows(builder.new_read().to_arrow(builder.new_scan().plan().splits()))
    assert sorted(row['s_a'] for row in rows) == ['drop', 'keep']


def test_unmasked_null_comparison_after_merge_matches_no_rows(tmp_path):
    fields = pa.schema([pa.field('id', pa.int32(), nullable=False), ('score', pa.int32()), ('secret', pa.string())])
    table = table_from_rows(tmp_path, fields, [
        {'id': 1, 'score': 10, 'secret': 'x'}, {'id': 2, 'score': None, 'secret': 'y'}], primary_keys=['id'])
    authorize(table, rules())
    predicate = PredicateBuilder(table.fields).not_equal('score', None)
    assert read(table, predicate, projection=['id', 'score']) == []
