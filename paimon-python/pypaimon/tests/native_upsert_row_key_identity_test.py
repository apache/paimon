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

"""Row-to-Arrow transport must preserve the original upsert key identity."""

from contextlib import ExitStack
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsert
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.execution.native.write.native_update import NativeTableUpsert, _upsert_row_batches, create_native_upsert
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]


def _append(table, data):
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_arrow(data)
        builder.new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()


def _tables(catalog, key_type, key, native, historical):
    schema = pa.schema([('key', key_type), ('value', pa.int32())])
    catalog.create_table('default.row_keys', Schema.from_pyarrow_schema(schema, options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }), False)
    latest = catalog.get_table('default.row_keys')
    _append(latest, pa.table({'key': [key], 'value': [10]}, schema=schema))
    target = latest
    if historical:
        catalog.alter_table('default.row_keys', [SchemaChange.add_column('extra', AtomicType('INT'))])
        latest = catalog.get_table('default.row_keys')
        _append(latest, pa.table({'key': [42], 'value': [90], 'extra': [900]},
                                 schema=schema.append(pa.field('extra', pa.int32()))))
        target = latest.copy({'scan.snapshot-id': '1'})
    return latest, target.copy({'write.native.enabled': str(native).lower()})


def _rows(table):
    read = table.new_read_builder().with_projection(table.field_names + ['_ROW_ID'])
    return read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('_ROW_ID').to_pylist()


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('historical', [False, True], ids=['current', 'historical'])
@pytest.mark.parametrize('key_type,initial,source_type,key', [
    (pa.int32(), 1, pa.float64(), 1.5),
    (pa.float32(), 1.2, pa.float64(), 1.2),
], ids=['fractional', 'float32'])
def test_lossy_row_key_encoding_cannot_update_an_existing_key(
        native_rest_catalog, native, stream, historical, key_type, initial, source_type, key):
    latest, target = _tables(native_rest_catalog, key_type, initial, native, historical)
    before = _rows(latest)
    fields = Schema.from_pyarrow_schema(pa.schema([('key', source_type), ('value', pa.int32())])).fields
    row = GenericRow([key, 99], fields)
    builder = target.new_stream_write_builder() if stream else target.new_batch_write_builder()
    original = PythonTableUpsert._upsert_row_partition
    # The unsafe encoding is rejected before starting a core mutation. Python
    # still matches with the original key and constructs a distinct new row.
    with patch.object(PythonTableUpsert, '_upsert_row_partition', autospec=True, side_effect=original) as fallback:
        update = builder.new_update().with_update_type(['value'])
        messages = update.upsert_by_key([row], ['key'], 17) if stream else update.upsert_by_key([row], ['key'])
        fallback.assert_called_once()
    assert messages
    assert all(message.check_from_snapshot is None for message in messages)
    publisher = latest.new_stream_write_builder() if stream else latest.new_batch_write_builder()
    commit = publisher.new_commit()
    try:
        commit.commit(messages, 17) if stream else commit.commit(messages)
    finally:
        commit.close()
    after = _rows(latest)
    assert after[:-1] == before
    expected = {'key': pa.array([key], type=key_type)[0].as_py(), 'value': 99, '_ROW_ID': len(before)}
    if historical:
        expected['extra'] = None
    assert after[-1] == expected


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('key_type,initial,source_type,key', [
    (pa.int32(), 42, pa.float64(), 42.0),
    (pa.int32(), None, pa.int32(), None),
], ids=['integral-float', 'null'])
def test_exact_row_key_encodings_still_use_native_upsert(
        native_rest_catalog, native, key_type, initial, source_type, key):
    latest, target = _tables(native_rest_catalog, key_type, initial, native, False)
    fields = Schema.from_pyarrow_schema(pa.schema([('key', source_type), ('value', pa.int32())])).fields
    row = GenericRow([key, 99], fields)
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(PythonTableUpsert, '_upsert_row_partition',
                                             side_effect=AssertionError('Python exact-key fallback')))
        update = target.new_batch_write_builder().new_update().with_update_type(['value'])
        messages = update.upsert_by_key([row], ['key'])
    assert all(message.check_from_snapshot == 1 for message in messages)
    latest.new_batch_write_builder().new_commit().commit(messages)
    assert _rows(latest) == [{'key': pa.array([initial], type=key_type)[0].as_py(), 'value': 99, '_ROW_ID': 0}]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
def test_large_integer_row_keys_retain_the_existing_constructor_fallback(native_rest_catalog, native):
    key = 2 ** 53 + 2
    latest, target = _tables(native_rest_catalog, pa.float64(), float(key), native, False)
    fields = Schema.from_pyarrow_schema(pa.schema([('key', pa.int64()), ('value', pa.int32())])).fields
    original = PythonTableUpsert._upsert_row_partition
    with patch.object(PythonTableUpsert, '_upsert_row_partition', autospec=True, side_effect=original) as fallback:
        messages = target.new_batch_write_builder().new_update().with_update_type(['value']).upsert_by_key(
            [GenericRow([key, 99], fields)], ['key'])
        # PyArrow's constructor rejects Python integers outside its consecutive
        # DOUBLE range, even for an exactly representable value. The existing
        # Python matcher can compare it without constructing a DOUBLE key.
        fallback.assert_called_once()
    latest.new_batch_write_builder().new_commit().commit(messages)
    assert _rows(latest) == [{'key': float(key), 'value': 99, '_ROW_ID': 0}]


@pytest.mark.parametrize('key_type,keys,accepted', [
    (pa.int32(), [1.5], False),
    (pa.float64(), [2 ** 53 + 1, 42.0], False),
    (pa.float32(), [1.2], False),
    (pa.int32(), [42.0, None], True),
    (pa.float64(), [2 ** 53 - 1, 42.0], True),
    (pa.float64(), [2 ** 53 + 2], False),
    (pa.bool_(), [True, None], True),
    (pa.string(), ['a', None], True),
    (pa.binary(), [b'a', None], True),
    (pa.large_binary(), [b'a'], True),
    (pa.string(), [b'a'], False),
    (pa.large_string(), [b'a'], False),
    (pa.binary(), ['a'], False),
    (pa.large_binary(), ['a'], False),
    (pa.binary(1), ['a'], False),
], ids=['fractional', 'mixed-large-integer', 'float32', 'exact-and-null',
        'mixed-exact', 'exact-large-integer-constructor', 'boolean', 'string', 'binary', 'large-binary',
        'bytes-to-string', 'bytes-to-large-string', 'string-to-binary', 'string-to-large-binary',
        'string-to-fixed-binary'])
def test_row_transport_qualifies_keys_before_constructing_a_core_updater(
        native_rest_catalog, key_type, keys, accepted):
    _, target = _tables(native_rest_catalog, key_type, None, True, False)
    native_table = Mock()
    rows = [{'key': key, 'value': 99} for key in keys]
    with patch('pypaimon.execution.native.write.native_update.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.native_update.create_native_write_table', return_value=native_table):
        upsert = TableUpsertByKey(target, 'row-key-identity', BATCH_COMMIT_IDENTIFIER)
        writer = upsert._create_native_upsert(rows, ['key'], ['value'])
    if accepted:
        assert isinstance(writer, NativeTableUpsert)
        native_table.new_batch_write_builder.assert_called_once()
        encoded = [value for batch in writer.data for value in batch.column('key').to_pylist()]
        assert encoded == keys
    else:
        assert writer is None
        native_table.new_batch_write_builder.assert_not_called()


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('key_type,initial,source_type,key', [
    (pa.string(), 'a', pa.binary(), b'a'),
    (pa.binary(), b'a', pa.string(), 'a'),
], ids=['bytes-to-string', 'string-to-binary'])
def test_text_binary_row_key_constructors_cannot_change_match_identity(
        native_rest_catalog, native, key_type, initial, source_type, key):
    latest, target = _tables(native_rest_catalog, key_type, initial, native, False)
    fields = Schema.from_pyarrow_schema(pa.schema([('key', source_type), ('value', pa.int32())])).fields
    original = PythonTableUpsert._upsert_row_partition
    with patch.object(PythonTableUpsert, '_upsert_row_partition', autospec=True, side_effect=original) as fallback:
        messages = target.new_batch_write_builder().new_update().with_update_type(['value']).upsert_by_key(
            [GenericRow([key, 99], fields)], ['key'])
        fallback.assert_called_once()
    assert messages and all(message.check_from_snapshot is None for message in messages)
    latest.new_batch_write_builder().new_commit().commit(messages)
    assert _rows(latest) == [
        {'key': initial, 'value': 10, '_ROW_ID': 0},
        {'key': initial, 'value': 99, '_ROW_ID': 1},
    ]


def test_row_transport_checks_partition_keys_even_when_they_are_not_upsert_keys(native_rest_catalog):
    schema = pa.schema([('key', pa.int32()), ('p', pa.int32()), ('value', pa.int32())])
    native_rest_catalog.create_table('default.partition_key_identity', Schema.from_pyarrow_schema(
        schema, partition_keys=['p'], options={
            'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true', 'write.native.enabled': 'true',
        }), False)
    table = native_rest_catalog.get_table('default.partition_key_identity')
    native_table = Mock()
    with patch('pypaimon.execution.native.write.native_update.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.native_update.create_native_write_table', return_value=native_table):
        writer = create_native_upsert(table, 'row-partition-identity',
                                      [{'key': 1, 'p': 1.5, 'value': 99}], ['key'], ['value'])
    assert writer is None
    native_table.new_batch_write_builder.assert_not_called()


def test_numeric_key_qualification_precedes_blob_payload_encoding_across_row_shapes():
    schema = pa.schema([('key', pa.int32()), ('value', pa.large_binary()), ('keep', pa.int32())])
    fields = Schema.from_pyarrow_schema(schema).fields
    rows = [{'key': 1, 'value': b'first'}, {'key': 1.5, 'value': b'second', 'keep': None}]
    blobs = Mock()
    blobs.to_batch.side_effect = AssertionError('Encoded a payload before qualifying every key')
    assert _upsert_row_batches(rows, fields, schema, blob_rows=blobs, key_names=['key']) is None
    blobs.to_batch.assert_not_called()
