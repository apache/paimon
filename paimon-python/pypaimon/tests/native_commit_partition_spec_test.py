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

"""Public overwrite/truncate specs use Java's declared partition types."""

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.schema.data_types import AtomicType
from pypaimon.tests import native_plan_rest_test
from pypaimon.write.partition_spec import _partition_spec_value


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
rest_catalog = native_plan_rest_test.rest_catalog
native = pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')
_ENGINES = [False, pytest.param(True, marks=native)]
_CASES = [
    ('integer', pa.int32(), [1, 2], '1'),
    ('typed-integer', pa.int32(), [1, 2], 1),
    ('unicode-integer', pa.int32(), [12, 13], '１２'),
    ('boolean', pa.bool_(), [True, False], 'TRUE'),
    ('string', pa.string(), ['a/b = 中', 'other'], 'a/b = 中'),
    ('decimal', pa.decimal128(6, 2), [Decimal('12.35'), Decimal('13.00')], '12.345'),
    ('unicode-decimal', pa.decimal128(6, 2), [Decimal('12.35'), Decimal('13.00')], '１２.３４５'),
    ('decimal-overflow', pa.decimal128(3, 0), [None, Decimal('12')], '1000'),
    ('date', pa.date32(), [date(2024, 1, 2), date(2024, 1, 3)], '2024-01-02'),
    ('date-partial', pa.date32(), [date(2024, 1, 1), date(2024, 1, 3)], '2024-1'),
    ('date-epoch', pa.date32(), [date(1970, 1, 2), date(1970, 1, 3)], '1'),
    ('time', pa.time32('ms'), [time(12, 34, 56, 789000), time(12, 35)], '12:34:56.789'),
    ('time-rounding', pa.time32('ms'), [time(12, 34, 56, 790000), time(12, 35)], '12:34:56.7895'),
    ('time-fraction', pa.time32('ms'), [time(12, 34, 56, 12000), time(12, 35)], '12:34:56.\u00a0123'),
    ('time-offset', pa.time32('ms'), [time(13, 34, 56), time(13, 35)], '12:34:56+01:00'),
    ('time-epoch', pa.time32('ms'), [time(0, 0, 0, 1000), time(0, 0, 0, 2000)], '1'),
    ('timestamp', pa.timestamp('us'), [datetime(2024, 1, 2, 12, 34), datetime(2024, 1, 3)],
     '2024-01-02 12:34:00'),
    ('timestamp-iso', pa.timestamp('us'), [datetime(2024, 1, 2, 12, 34), datetime(2024, 1, 3)],
     '2024-01-02T12:34'),
    ('timestamp-repeated', pa.timestamp('us'), [datetime(2024, 1, 2, 12, 34), datetime(2024, 1, 3)],
     '2024-0101-0202 1212:3434:00T12:34'),
    ('timestamp-defaults', pa.timestamp('us'), [datetime(2024, 1, 1), datetime(2024, 1, 3)], '2024--'),
    ('timestamp-nanos', pa.timestamp('ns'), [datetime(2024, 1, 2, 12, 34, 0, 123456), datetime(2024, 1, 3)],
     '2024-01-02 12:34:00.123456000'),
    ('timestamp-overflow', pa.timestamp('s'), [datetime(1969, 12, 31, 23, 59, 59), datetime(1970, 1, 1)],
     '9223372036854775807'),
    ('timestamp-ltz', pa.timestamp('us', tz='UTC'),
     [datetime(2024, 1, 2, 12, 34, tzinfo=timezone.utc), datetime(2024, 1, 3, tzinfo=timezone.utc)],
     '2024-01-02 12:34:00'),
    ('float', pa.float32(), [1.2, 2.0], '1.2'),
    ('float-hex', pa.float32(), [4.0, 2.0], '0x1.p2'),
    ('float-trim', pa.float32(), [1.2, 2.0], '\x001.2\x1f'),
    ('null', pa.int32(), [None, 2], None),
    ('default-name', pa.int32(), [None, 2], '__DEFAULT_PARTITION__'),
]


def _table(catalog, data_type, values, use_native):
    schema = pa.schema([('id', pa.int64()), ('pt', data_type)])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['pt'], options={
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
            'dynamic-partition-overwrite': 'false',
        }), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': [1, 2], 'pt': values}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table.copy({'commit.native.enabled': str(use_native).lower()}), schema


def _ids(table):
    builder = table.new_read_builder()
    return sorted(row['id'] for row in builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist())


def _files(table):
    return set(Path(unquote(urlparse(table.table_path).path)).rglob('*'))


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('operation', ['truncate', 'overwrite'])
@pytest.mark.parametrize('name,data_type,values,text', _CASES, ids=[case[0] for case in _CASES])
def test_partition_specs_parse_declared_types(rest_catalog, use_native, operation, name, data_type, values, text):
    catalog, _ = rest_catalog
    if name == 'timestamp-ltz':
        # Java treats this text as process-local wall time, not as UTC.
        text = values[0].astimezone().replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')
    table, schema = _table(catalog, data_type, values, use_native)
    builder = table.new_batch_write_builder()
    writer = None
    if operation == 'overwrite':
        builder.overwrite({'pt': text})
        writer = builder.new_write()
        writer.write_arrow(pa.table({'id': [3], 'pt': values[:1]}, schema=schema))
    commit = builder.new_commit()
    try:
        if use_native:
            method = 'drop_partitions' if operation == 'truncate' else 'overwrite'
            guard = patch.object(commit.file_store_commit, method, side_effect=AssertionError('Python fallback'))
        else:
            guard = patch.object(commit, '_prepare_native_commit', return_value=None)
        with guard:
            if operation == 'truncate':
                commit.truncate_partitions([{'pt': text}])
            else:
                commit.commit(writer.prepare_commit())
        assert _ids(table) == ([2] if operation == 'truncate' else [2, 3])
        assert table.snapshot_manager().get_latest_snapshot().id == 2
    finally:
        if writer is not None:
            writer.close()
        commit.close()


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('operation', ['truncate', 'overwrite'])
@pytest.mark.parametrize('spec', [{'missing': '1'}, {'pt': 'bad'}, {'pt': '1.5'}, {'pt': '2147483648'},
                                  {'pt': ' 1'}, {'pt': '1_0'}, {'pt': '𝟙'}])
def test_invalid_specs_do_not_publish_or_delete(rest_catalog, use_native, operation, spec):
    catalog, server = rest_catalog
    table, _ = _table(catalog, pa.int32(), [1, 2], use_native)
    builder = table.new_batch_write_builder()
    if operation == 'overwrite':
        builder.overwrite(spec)
    commit = builder.new_commit()
    before = _files(table)
    try:
        with patch.object(server, '_table_commit_handle', wraps=server._table_commit_handle) as publish:
            with pytest.raises((ValueError, RuntimeError)):
                if operation == 'truncate':
                    commit.truncate_partitions([spec])
                else:
                    commit.commit([])
            publish.assert_not_called()
        assert table.snapshot_manager().get_latest_snapshot().id == 1
        assert _ids(table) == [1, 2]
        assert _files(table) == before
    finally:
        commit.close()


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('data_type,values,text', [
    (pa.bool_(), [True, False], 'on'),
    (pa.float32(), [1.0, 2.0], '1.23456789'),
    (pa.decimal128(6, 2), [Decimal('1'), Decimal('2')], 'not a decimal'),
    (pa.decimal128(6, 2), [Decimal('1'), Decimal('2')], '𝟙.3'),
    (pa.date32(), [date(2024, 1, 1), date(2024, 1, 2)], '2024-02-30'),
    (pa.date32(), [date(2024, 1, 1), date(2024, 1, 2)], '2024-\u00a01-01'),
    (pa.time32('ms'), [time(12, 34), time(12, 35)], '12:34+02:30'),
    (pa.time32('ms'), [time(12, 34), time(12, 35)], '\u00a012:34:00'),
    (pa.timestamp('us'), [datetime(2024, 1, 1), datetime(2024, 1, 2)], '2024-01-01 12:34'),
    (pa.timestamp('us'), [datetime(2024, 1, 1), datetime(2024, 1, 2)], '2024-00-01'),
    (pa.timestamp('us'), [datetime(2024, 1, 1), datetime(2024, 1, 2)], '2024-01-00'),
])
def test_invalid_typed_text_does_not_publish(rest_catalog, use_native, data_type, values, text):
    catalog, server = rest_catalog
    table, _ = _table(catalog, data_type, values, use_native)
    commit = table.new_batch_write_builder().new_commit()
    before = _files(table)
    try:
        with patch.object(server, '_table_commit_handle', wraps=server._table_commit_handle) as publish:
            with pytest.raises((ValueError, RuntimeError)):
                commit.truncate_partitions([{'pt': text}])
            publish.assert_not_called()
        assert table.snapshot_manager().get_latest_snapshot().id == 1
        assert _ids(table) == [1, 2]
        assert _files(table) == before
    finally:
        commit.close()


@pytest.mark.parametrize('use_native', _ENGINES)
def test_overwrite_partition_mismatch_preserves_prepared_files(rest_catalog, use_native):
    catalog, server = rest_catalog
    table, schema = _table(catalog, pa.int32(), [1, 2], use_native)
    builder = table.new_batch_write_builder().overwrite({'pt': '2'})
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': [3], 'pt': [1]}, schema=schema))
        messages = writer.prepare_commit()
        before = _files(table)
        with patch.object(server, '_table_commit_handle', wraps=server._table_commit_handle) as publish:
            with pytest.raises((ValueError, RuntimeError)):
                commit.commit(messages)
            publish.assert_not_called()
        assert table.snapshot_manager().get_latest_snapshot().id == 1
        assert _ids(table) == [1, 2]
        assert _files(table) == before
    finally:
        writer.close()
        commit.close()


def test_timestamp_spec_precision_is_independent_of_arrow_storage():
    assert _partition_spec_value('1970-01-01 00:00:00.123456', AtomicType('TIMESTAMP(1)')) == (
        datetime(1970, 1, 1, 0, 0, 0, 100000))
    value = _partition_spec_value('1970-01-01 00:00:00.123456789', AtomicType('TIMESTAMP(9)'))
    assert pa.scalar(value, type=pa.timestamp('ns')).value == 123456789


@pytest.mark.parametrize('sign', [-1, 1])
def test_numeric_timestamp_overflow_retains_millisecond_remainder(sign):
    value = _partition_spec_value(str(sign * 18446744073709552), AtomicType('TIMESTAMP(0)'))
    assert value == datetime(1970, 1, 1) + timedelta(milliseconds=sign * 384)


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('sign', [-1, 1])
def test_overflow_spec_does_not_truncate_another_partition(rest_catalog, use_native, sign):
    catalog, _ = rest_catalog
    table, _ = _table(catalog, pa.timestamp('s'),
                      [datetime(1970, 1, 1), datetime(1969, 12, 31, 23, 59, 59)], use_native)
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.truncate_partitions([{'pt': str(sign * 18446744073709552)}])
        assert _ids(table) == [1, 2]
        assert table.snapshot_manager().get_latest_snapshot().id == 2
    finally:
        commit.close()


@pytest.mark.parametrize('data_type,text', [
    (AtomicType('VARCHAR(2)', False), 'abc'),
    (AtomicType('VARBINARY(2)', False), '中'),
    (AtomicType('TINYINT'), '128'),
    (AtomicType('SMALLINT'), '-32769'),
    (AtomicType('BIGINT'), '9223372036854775808'),
])
def test_partition_spec_type_limits_are_validated(data_type, text):
    with pytest.raises(ValueError):
        _partition_spec_value(text, data_type)
