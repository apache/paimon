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

"""Temporal row keys must reach matching before any lossy construction."""

from contextlib import ExitStack
from datetime import date, datetime, time, timedelta, timezone
import sys
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.native_update import NativeTableUpsert, _upsert_row_batches, create_native_upsert
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]
_UNSAFE_KINDS = ['timestamp', 'seconds', 'local-zone', 'offset-precision', 'time', 'datetime-date',
                 'aware-time', 'naive-ltz', 'aware-ntz']
_TICK_KINDS = ['date-ticks', 'timestamp-ticks', 'time-ticks']


def _case(kind, exact=False):
    micros = 123000 if exact else 123456
    if kind == 'date-ticks':
        return pa.date32(), date(2020, 1, 1), pa.int32(), 18262
    if kind == 'timestamp-ticks':
        return pa.timestamp('ms'), datetime(2020, 1, 1), pa.int64(), 1577836800000
    if kind == 'time-ticks':
        return pa.time32('ms'), time(12, 34, 56, 123000), pa.int32(), 45296123
    if kind == 'date':
        return pa.date32(), date(2020, 1, 1), pa.date32(), date(2020, 1, 1)
    if kind == 'datetime-date':
        return pa.date32(), date(2020, 1, 1), pa.timestamp('us'), datetime(2020, 1, 1)
    if kind == 'aware-time':
        return (pa.time32('ms'), time(12, 34, 56, 123000), pa.time64('us'),
                time(12, 34, 56, 123000, tzinfo=timezone.utc))
    if kind == 'naive-ltz':
        return (pa.timestamp('ms', tz='UTC'), datetime(2020, 1, 1, tzinfo=timezone.utc),
                pa.timestamp('us'), datetime(2020, 1, 1))
    if kind == 'aware-ntz':
        return (pa.timestamp('ms'), datetime(2020, 1, 1), pa.timestamp('us', tz='UTC'),
                datetime(2020, 1, 1, tzinfo=timezone.utc))
    if kind == 'offset-precision':
        if sys.version_info < (3, 7):
            pytest.skip('Sub-minute timezone offsets require Python 3.7 or newer')
        initial = datetime(2020, 1, 1, microsecond=122000, tzinfo=timezone.utc)
        value = datetime(2020, 1, 1, microsecond=123000, tzinfo=timezone(timedelta(microseconds=500)))
        return pa.timestamp('ms', tz='UTC'), initial, pa.timestamp('us', tz='UTC'), value
    if kind == 'time':
        return pa.time32('ms'), time(12, 34, 56, 123000), pa.time64('us'), time(12, 34, 56, micros)
    if kind == 'seconds':
        value = datetime(2020, 1, 1, microsecond=0 if exact else micros)
        return pa.timestamp('s'), datetime(2020, 1, 1), pa.timestamp('us'), value
    if kind == 'local-zone':
        initial = datetime(2020, 1, 1, microsecond=123000, tzinfo=timezone.utc)
        value = datetime(2020, 1, 1, 8, microsecond=micros, tzinfo=timezone(timedelta(hours=8)))
        return pa.timestamp('ms', tz='UTC'), initial, pa.timestamp('us', tz='UTC'), value
    return (pa.timestamp('ms'), datetime(2020, 1, 1, microsecond=123000),
            pa.timestamp('us'), datetime(2020, 1, 1, microsecond=micros))


def _append(table, data):
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_arrow(data)
        builder.new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()


def _tables(catalog, key_type, initial, native, historical):
    name = 'default.temporal_row_keys'
    schema = pa.schema([('key', key_type), ('value', pa.int32())])
    catalog.create_table(name, Schema.from_pyarrow_schema(schema, options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }), False)
    latest = catalog.get_table(name)
    _append(latest, pa.table({'key': [initial], 'value': [10]}, schema=schema))
    target = latest
    if historical:
        catalog.alter_table(name, [SchemaChange.add_column('extra', AtomicType('INT'))])
        latest = catalog.get_table(name)
        _append(latest, pa.table({'key': [None], 'value': [90], 'extra': [900]},
                                 schema=schema.append(pa.field('extra', pa.int32()))))
        target = latest.copy({'scan.snapshot-id': '1'})
    return latest, target.copy({'write.native.enabled': str(native).lower()})


def _rows(table):
    read = table.new_read_builder().with_projection(table.field_names + ['_ROW_ID'])
    return read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('_ROW_ID').to_pylist()


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('historical', [False, True], ids=['current', 'historical'])
@pytest.mark.parametrize('kind', _UNSAFE_KINDS)
def test_lossy_temporal_row_key_does_not_overwrite_existing_rows(
        native_rest_catalog, native, stream, historical, kind):
    key_type, initial, source_type, value = _case(kind)
    latest, target = _tables(native_rest_catalog, key_type, initial, native, historical)
    before = _rows(latest)
    fields = Schema.from_pyarrow_schema(pa.schema([('key', source_type), ('value', pa.int32())])).fields
    row = GenericRow([value, 99], fields)
    builder = target.new_stream_write_builder() if stream else target.new_batch_write_builder()
    original = TableUpsertByKey._upsert_row_partition
    with patch.object(TableUpsertByKey, '_upsert_row_partition', autospec=True, side_effect=original) as fallback:
        update = builder.new_update().with_update_type(['value'])
        messages = update.upsert_by_key([row], ['key'], 17) if stream else update.upsert_by_key([row], ['key'])
        fallback.assert_called_once()
    assert messages and all(message.check_from_snapshot is None for message in messages)
    publisher = latest.new_stream_write_builder() if stream else latest.new_batch_write_builder()
    committer = publisher.new_commit()
    try:
        committer.commit(messages, 17) if stream else committer.commit(messages)
    finally:
        committer.close()
    expected = {'key': pa.array([value], type=key_type)[0].as_py(), 'value': 99, '_ROW_ID': len(before)}
    if historical:
        expected['extra'] = None
    assert _rows(latest) == before + [expected]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('kind', ['timestamp', 'seconds', 'local-zone', 'time', 'date', 'null'])
def test_exact_temporal_row_keys_keep_native_matching(native_rest_catalog, native, kind):
    key_type, initial, source_type, value = _case(kind, exact=True)
    if kind == 'null':
        initial = value = None
    latest, target = _tables(native_rest_catalog, key_type, initial, native, False)
    fields = Schema.from_pyarrow_schema(pa.schema([('key', source_type), ('value', pa.int32())])).fields
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(TableUpsertByKey, '_upsert_row_partition',
                                             side_effect=AssertionError('Exact temporal key fell back')))
        messages = target.new_batch_write_builder().new_update().with_update_type(['value']).upsert_by_key(
            [GenericRow([value, 99], fields)], ['key'])
    assert messages and all(message.check_from_snapshot == 1 for message in messages)
    latest.new_batch_write_builder().new_commit().commit(messages)
    assert _rows(latest) == [{'key': initial, 'value': 99, '_ROW_ID': 0}]


@pytest.mark.parametrize('kind', _UNSAFE_KINDS + _TICK_KINDS)
def test_lossy_temporal_keys_are_rejected_before_constructing_core_updater(native_rest_catalog, kind):
    key_type, initial, _, value = _case(kind)
    _, target = _tables(native_rest_catalog, key_type, initial, True, False)
    native_table = Mock()
    with patch('pypaimon.write.native_update.native_runtime_available', return_value=True), \
            patch('pypaimon.write.native_update.create_native_write_table', return_value=native_table):
        writer = create_native_upsert(target, 'temporal-identity', [{'key': value, 'value': 99}], ['key'], ['value'])
    assert writer is None
    native_table.new_batch_write_builder.assert_not_called()


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('kind', _TICK_KINDS)
def test_integer_row_keys_are_not_reinterpreted_as_existing_temporal_keys(native_rest_catalog, native, kind):
    key_type, initial, source_type, value = _case(kind)
    latest, target = _tables(native_rest_catalog, key_type, initial, native, False)
    fields = Schema.from_pyarrow_schema(pa.schema([('key', source_type), ('value', pa.int32())])).fields
    original = TableUpsertByKey._upsert_row_partition
    with patch.object(TableUpsertByKey, '_upsert_row_partition', autospec=True, side_effect=original) as fallback:
        messages = target.new_batch_write_builder().new_update().with_update_type(['value']).upsert_by_key(
            [GenericRow([value, 99], fields)], ['key'])
        fallback.assert_called_once()
    assert messages and all(message.check_from_snapshot is None for message in messages)
    latest.new_batch_write_builder().new_commit().commit(messages)
    assert _rows(latest) == [
        {'key': initial, 'value': 10, '_ROW_ID': 0},
        {'key': initial, 'value': 99, '_ROW_ID': 1},
    ]


def test_temporal_precision_check_preserves_fold_and_equivalent_instants(native_rest_catalog):
    zoneinfo = pytest.importorskip('zoneinfo')
    initial = datetime(2021, 11, 7, 6, 30, microsecond=123000, tzinfo=timezone.utc)
    _, target = _tables(native_rest_catalog, pa.timestamp('ms', tz='UTC'), initial, True, False)
    inputs = [
        initial,
        initial.astimezone(timezone(timedelta(hours=8))),
        datetime(2021, 11, 7, 1, 30, microsecond=123000, tzinfo=zoneinfo.ZoneInfo('America/New_York'), fold=1),
        initial.astimezone(timezone(timedelta(microseconds=500))),
    ]
    for value in inputs:
        native_table = Mock()
        with patch('pypaimon.write.native_update.native_runtime_available', return_value=True), \
                patch('pypaimon.write.native_update.create_native_write_table', return_value=native_table):
            writer = create_native_upsert(target, 'temporal-identity',
                                          [{'key': value, 'value': 99}], ['key'], ['value'])
        assert isinstance(writer, NativeTableUpsert)
        native_table.new_batch_write_builder.assert_called_once()
        assert writer.data[0].column('key').to_pylist() == [initial]


def test_pytz_temporal_keys_keep_the_existing_instant_conversion(native_rest_catalog):
    pytz = pytest.importorskip('pytz')
    initial = datetime(2021, 11, 7, 6, 30, microsecond=123000, tzinfo=timezone.utc)
    value = pytz.timezone('America/New_York').localize(datetime(2021, 11, 7, 1, 30, 0, 123000), is_dst=False)
    _, target = _tables(native_rest_catalog, pa.timestamp('ms', tz='UTC'), initial, True, False)
    native_table = Mock()
    with patch('pypaimon.write.native_update.native_runtime_available', return_value=True), \
            patch('pypaimon.write.native_update.create_native_write_table', return_value=native_table):
        writer = create_native_upsert(target, 'temporal-identity', [{'key': value, 'value': 99}], ['key'], ['value'])
    assert isinstance(writer, NativeTableUpsert)
    assert writer.data[0].column('key').to_pylist() == [initial]


def test_partition_temporal_keys_also_preserve_their_original_identity(native_rest_catalog):
    schema = pa.schema([('key', pa.int32()), ('p', pa.timestamp('ms')), ('value', pa.int32())])
    native_rest_catalog.create_table('default.temporal_partition', Schema.from_pyarrow_schema(
        schema, partition_keys=['p'], options={
            'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true', 'write.native.enabled': 'true',
        }), False)
    table = native_rest_catalog.get_table('default.temporal_partition')
    native_table = Mock()
    with patch('pypaimon.write.native_update.native_runtime_available', return_value=True), \
            patch('pypaimon.write.native_update.create_native_write_table', return_value=native_table):
        writer = create_native_upsert(table, 'temporal-identity',
                                      [{'key': 1, 'p': datetime(2020, 1, 1, microsecond=123456), 'value': 99}],
                                      ['key'], ['value'])
    assert writer is None
    native_table.new_batch_write_builder.assert_not_called()


def test_temporal_keys_are_qualified_before_blob_payloads_in_any_row_shape():
    schema = pa.schema([('key', pa.timestamp('ms')), ('payload', pa.large_binary()), ('keep', pa.int32())])
    fields = Schema.from_pyarrow_schema(schema).fields
    rows = [
        {'key': datetime(2020, 1, 1, microsecond=123000), 'payload': b'first'},
        {'key': datetime(2020, 1, 1, microsecond=123456), 'payload': b'second', 'keep': None},
    ]
    blobs = Mock()
    blobs.to_batch.side_effect = AssertionError('Payload was consumed before temporal key qualification')
    assert _upsert_row_batches(rows, fields, schema, blob_rows=blobs, key_names=['key']) is None
    blobs.to_batch.assert_not_called()
