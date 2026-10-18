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

"""Nullable primary keys retain Java equality across Python and Native I/O."""

from contextlib import ExitStack
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.tests import native_plan_rest_test
from pypaimon.write.native_write import NativeTableWrite

rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = pytest.mark.native_plan
_SCHEMA = pa.schema([('pt', pa.int32()), ('id', pa.int32()), ('value', pa.int32())])


def _table(catalog, name, bucket, engine='deduplicate', primary_key=True, extra=None):
    options = {'write.native.enabled': 'false', 'source.split.target-size': '1b'}
    if primary_key:
        options.update({'bucket': str(bucket), 'primary-key.nullable': 'true', 'merge-engine': engine})
        if engine == 'aggregation':
            options['fields.value.aggregate-function'] = 'sum'
    else:
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
                        'deletion-vectors.enabled': 'true'})
    options.update(extra or {})
    # Current Java also overrides an explicitly declared NOT NULL key when enabled.
    schema = pa.schema([pa.field(field.name, field.type, nullable=not primary_key or field.name == 'value')
                        for field in _SCHEMA])
    catalog.create_table('default.' + name, Schema.from_pyarrow_schema(
        schema, primary_keys=['pt', 'id'] if primary_key else [],
        partition_keys=['pt'] if primary_key else [], options=options), False)
    table = catalog.get_table('default.' + name)
    assert all(field.type.nullable for field in table.fields)
    return table


def _write(table, rows, native, stream=False, identifier=1, row_input=False):
    table = table.copy({'write.native.enabled': str(native).lower()})
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite) == native
    try:
        if row_input:
            for row in rows:
                writer.write_row(GenericRow(list(row), table.fields))
        else:
            arrays = [pa.array([row[index] for row in rows], type=field.type)
                      for index, field in enumerate(_SCHEMA)]
            writer.write_arrow(pa.Table.from_arrays(arrays, schema=_SCHEMA))
        if native:
            assert writer._python_writer is None
        messages = writer.prepare_commit(identifier) if stream else writer.prepare_commit()
        commit.commit(messages, identifier) if stream else commit.commit(messages)
        return messages
    finally:
        writer.close()
        commit.close()


def _read(table, native, null_field=None, all_files=False):
    table = table.copy({'read.native.enabled': str(native).lower(),
                        'scan.native-plan.enabled': str(native).lower()})
    builder = table.new_read_builder()
    if null_field:
        builder.with_filter(builder.new_predicate_builder().is_null(null_field))
    scan, read = builder.new_scan(), builder.new_read()
    with ExitStack() as stack:
        if native:
            if not all_files:
                stack.enter_context(patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('Python scan')))
            stack.enter_context(patch.object(read, '_create_split_read', side_effect=AssertionError('Python read')))
        splits = (scan.plan_all_files() if all_files else scan.plan()).splits()
        rows = read.to_arrow(splits).to_pylist()
    # All values in these fixtures are non-negative except the seeded MERGE target.
    return sorted((tuple(row[name] for name in _SCHEMA.names) for row in rows),
                  key=lambda row: tuple(-1000 if value is None else value for value in row))


@pytest.mark.parametrize('bucket', [1, 3, -1], ids=['one-bucket', 'fixed', 'dynamic'])
@pytest.mark.parametrize('engine', ['deduplicate', 'partial-update', 'aggregation'])
@pytest.mark.parametrize('native_first', [False, True], ids=['python-first', 'native-first'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
def test_nullable_composite_keys_across_writer_restarts(rest_catalog, bucket, engine, native_first, stream):
    table = _table(rest_catalog[0], 'nullable_keys', bucket, engine)
    _write(table, [(None, None, 10), (1, None, 100), (1, 0, 1)], native_first, stream, 1)
    first = table.snapshot_manager().get_latest_snapshot()
    # Restart with the other writer: a null key must retain its dynamic bucket.
    _write(table, [(None, None, 20), (1, None, 200), (None, 0, 2)], not native_first, stream, 2)
    values = (30, 300) if engine == 'aggregation' else (20, 200)
    expected = [(None, None, values[0]), (None, 0, 2), (1, None, values[1]), (1, 0, 1)]
    for native in [False, True]:
        assert _read(table, native) == expected
        assert _read(table, native, 'id') == [expected[0], expected[2]]
        assert _read(table, native, 'pt') == expected[:2]
        assert _read(table._copy_with_snapshot(first), native) == [(None, None, 10), (1, None, 100), (1, 0, 1)]


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('bucket', [3, -1], ids=['fixed', 'dynamic'])
def test_nullable_key_row_input_matches_arrow_input(rest_catalog, native, bucket):
    table = _table(rest_catalog[0], 'nullable_row', bucket)
    _write(table, [(None, None, 10), (None, 0, 20)], native, row_input=True)
    _write(table, [(None, None, 30), (1, None, 40)], native)
    for native_read in [False, True]:
        assert _read(table, native_read) == [(None, None, 30), (None, 0, 20), (1, None, 40)]


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('engine', ['deduplicate', 'partial-update', 'aggregation', 'first-row'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
def test_nullable_primary_key_source_uses_sql_merge_equality(rest_catalog, native, engine, stream):
    source = _table(rest_catalog[0], 'nullable_source', -1, engine)
    _write(source, [(None, None, 10), (None, 1, 11), (1, None, 12), (1, 1, 100)], False)
    _write(source, [(None, None, 20), (None, 1, 21), (1, None, 22), (1, 1, 200)], True)
    target = _table(rest_catalog[0], 'target', -1, primary_key=False)
    _write(target, [(None, None, -1), (None, 1, -3), (1, None, -4), (1, 1, -2)], False)
    target = target.copy({'write.native.enabled': str(native).lower()})
    builder = target.new_stream_write_builder() if stream else target.new_batch_write_builder()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._normalize_source',
                                      side_effect=AssertionError('Python source materialization')))
            stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._build_tables',
                                      side_effect=AssertionError('Python merge')))
        messages = builder.new_update().merge_into(
            source, on=['pt', 'id'], when_matched=[WhenMatched.update({'value': source_col('value')})],
            when_not_matched=[WhenNotMatched('*')], **({'commit_identifier': 37} if stream else {}))
    commit = builder.new_commit()
    try:
        commit.commit(messages, 37) if stream else commit.commit(messages)
    finally:
        commit.close()
    values = {'aggregation': (30, 32, 34, 300), 'first-row': (10, 11, 12, 100)}.get(engine, (20, 21, 22, 200))
    for native_read in [False, True]:
        # Storage merges null PKs, but SQL MERGE ON does not match NULL to NULL.
        assert _read(target, native_read) == [
            (None, None, -1), (None, None, values[0]), (None, 1, -3), (None, 1, values[1]),
            (1, None, -4), (1, None, values[2]), (1, 1, values[3])]
    assert _read(source, True, all_files=engine == 'first-row') == [
        (None, None, values[0]), (None, 1, values[1]), (1, None, values[2]), (1, 1, values[3])]
    assert source.snapshot_manager().get_latest_snapshot().id == 2


@pytest.mark.parametrize('bucket', [3, -1], ids=['fixed', 'dynamic'])
@pytest.mark.parametrize('key_type,zero', [
    (pa.bool_(), False), (pa.int64(), 0), (pa.string(), ''), (pa.binary(), b''),
    (pa.decimal128(20, 2), Decimal('0.00')), (pa.date32(), date(1970, 1, 1)),
    (pa.timestamp('us'), datetime(1970, 1, 1)),
], ids=['boolean', 'bigint', 'string', 'bytes', 'decimal', 'date', 'timestamp'])
def test_nullable_scalar_key_distinguishes_null_from_default_value(rest_catalog, bucket, key_type, zero):
    catalog, _ = rest_catalog
    schema = pa.schema([pa.field('id', key_type, nullable=False), ('value', pa.int32())])
    catalog.create_table('default.scalar_keys', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'], options={'bucket': str(bucket), 'primary-key.nullable': 'true'}), False)
    table = catalog.get_table('default.scalar_keys')
    input_schema = pa.schema([('id', key_type), ('value', pa.int32())])
    # Three separate writers exercise native index serialization, Python index
    # restore and native restore after a Python row write.
    for native, values in [(True, [(None, 10), (zero, 100)]),
                           (False, [(None, 20), (zero, 200)]), (True, [(None, 30)])]:
        builder = table.copy({'write.native.enabled': str(native).lower()}).new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        assert isinstance(writer, NativeTableWrite) == native
        try:
            if native:
                writer.write_arrow(pa.Table.from_arrays([
                    pa.array([row[index] for row in values], type=field.type)
                    for index, field in enumerate(input_schema)], schema=input_schema))
                assert writer._python_writer is None
            else:
                for row in values:
                    writer.write_row(GenericRow(list(row), table.fields))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    for native in [False, True]:
        builder = table.copy({'read.native.enabled': str(native).lower(),
                              'scan.native-plan.enabled': str(native).lower()}).new_read_builder()
        scan, read = builder.new_scan(), builder.new_read()
        with ExitStack() as stack:
            if native:
                stack.enter_context(patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('Python scan')))
                stack.enter_context(patch.object(read, '_create_split_read', side_effect=AssertionError('Python read')))
            rows = read.to_arrow(scan.plan().splits()).to_pylist()
        assert sorted(rows, key=lambda row: row['id'] is not None) == [
            {'id': None, 'value': 30}, {'id': zero, 'value': 200}]


@pytest.mark.parametrize('first_native', [False, True], ids=['python-first', 'native-first'])
def test_null_key_restores_nonzero_dynamic_bucket(rest_catalog, first_native):
    table = _table(rest_catalog[0], 'nonzero_null_key', -1,
                   extra={'dynamic-bucket.target-row-num': '1'})
    initial = _write(table, [(None, 0, 100), (None, None, 10)], first_native)
    assert {message.bucket for message in initial} == {0, 1}
    for native, value in [(not first_native, 20), (first_native, 30)]:
        messages = _write(table, [(None, None, value)], native)
        assert {message.bucket for message in messages} == {1}
    for native in [False, True]:
        assert _read(table, native) == [(None, None, 30), (None, 0, 100)]


def test_null_partition_requires_complete_dynamic_bucket_index(rest_catalog):
    from pypaimon.write.row_key_extractor import DynamicBucketRowKeyExtractor

    table = _table(rest_catalog[0], 'missing_hash', -1)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        # Simulate existing data whose HASH metadata is incomplete. Restoring
        # this partition must fail instead of assigning its keys new buckets.
        writer.row_key_extractor = DynamicBucketRowKeyExtractor(table.table_schema)
        writer.write_arrow(pa.Table.from_arrays([
            pa.array([None], type=pa.int32()), pa.array([None], type=pa.int32()),
            pa.array([10], type=pa.int32())], schema=_SCHEMA))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    writer = table.new_batch_write_builder().new_write()
    try:
        with pytest.raises(RuntimeError, match='has data files but no complete HASH index'):
            writer.write_arrow(pa.Table.from_arrays([
                pa.array([None], type=pa.int32()), pa.array([None], type=pa.int32()),
                pa.array([20], type=pa.int32())], schema=_SCHEMA))
    finally:
        writer.close()
