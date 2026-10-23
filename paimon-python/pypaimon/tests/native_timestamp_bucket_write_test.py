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

"""Nanosecond timestamp keys use Java's BinaryRow layout and bucket IDs."""

import struct
from contextlib import ExitStack
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from unittest.mock import patch

from pypaimon import Schema
from pypaimon.common.predicate import Predicate
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.row.generic_row import GenericRow, GenericRowDeserializer, GenericRowSerializer
from pypaimon.tests import native_plan_rest_test
from pypaimon.write.write_dispatch import NativeTableWrite
from pypaimon.execution.python.write.row_key_extractor import FixedBucketRowKeyExtractor


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
rest_catalog = native_plan_rest_test.rest_catalog
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]
# Java BinaryRowWriter stores millis in the variable area and nanoOfMillisecond
# in the low half of the fixed slot. These are default-function buckets mod 5.
_BUCKETS = {-1000001: 3, -1000000: 3, -1: 0, 0: 4, 1: 2, 2: 3,
            999999: 2, 1000000: 4, 1000001: 0, 1000002: 3}
_BOUNDARY_BUCKETS = {-9223372036854775807: 4, -9223372036854775001: 0, 9223372036854775807: 4}


def _bucket_cases(zone):
    # The ns endpoints cannot be represented by pandas in zones that shift
    # their displayed local time outside its range. UTC is a legal LTZ input.
    buckets = _BUCKETS.copy()
    if zone in (None, 'UTC'):
        buckets.update(_BOUNDARY_BUCKETS)
    return buckets


def _value(ticks, zone):
    value = pd.Timestamp(ticks, unit='ns', tz='UTC' if zone is not None else None)
    return value.tz_convert(zone) if zone is not None else value


@pytest.mark.parametrize('zone', [None, 'UTC', 'America/New_York', 'Asia/Shanghai'])
@pytest.mark.parametrize('precision', [3, 9], ids=['compact', 'noncompact'])
def test_timestamp_key_bytes_match_java_for_nanosecond_and_negative_epoch_values(zone, precision):
    name = 'TIMESTAMP_LTZ' if zone is not None else 'TIMESTAMP'
    field = DataField(0, 'key', AtomicType('%s(%d)' % (name, precision), nullable=False))
    extractor = FixedBucketRowKeyExtractor(TableSchema(
        fields=[field], primary_keys=['key'], options={'bucket': '5'}))
    for ticks, bucket in _bucket_cases(zone).items():
        value = _value(ticks, zone)
        millis, nanos = divmod(ticks, 1000000)
        if precision == 3:
            expected = struct.pack('>i', 1) + struct.pack('<qq', 0, millis)
        else:
            expected = struct.pack('>i', 1) + struct.pack('<qQq', 0, (16 << 32) | nanos, millis)
            assert extractor.extract_partition_bucket_row({'key': value})[1] == bucket
        assert GenericRowSerializer.to_bytes(GenericRow([value], [field])) == expected


@pytest.mark.parametrize('zone', [None, 'UTC', 'America/New_York', 'Asia/Shanghai'])
def test_timestamp_binary_row_stats_deserialize_without_truncating_nanoseconds(zone):
    name = 'TIMESTAMP_LTZ' if zone is not None else 'TIMESTAMP'
    fields = [DataField(0, 'key', AtomicType(name + '(9)', nullable=False))]
    for ticks in _bucket_cases(zone):
        millis, nanos = divmod(ticks, 1000000)
        encoded = struct.pack('>i', 1) + struct.pack('<qQq', 0, (16 << 32) | nanos, millis)
        result = GenericRowDeserializer.from_bytes(encoded, fields).values[0]
        assert result == _value(ticks, zone)
        assert GenericRowSerializer.to_bytes(GenericRow([result], fields)) == encoded


@pytest.mark.parametrize('zone', [None, 'America/New_York'])
@pytest.mark.parametrize('method', ['in', 'notIn'])
@pytest.mark.parametrize('with_null', [False, True])
def test_nanosecond_membership_literals_preserve_sql_null_semantics(zone, method, with_null):
    data_type = pa.timestamp('ns', tz='UTC' if zone is not None else None)
    data = pa.table({'key': pa.array([-1, 0, 1, 2, None], pa.int64()).cast(data_type)})
    values = [_value(1, zone)] + ([None] if with_null else [])
    predicate = Predicate(method=method, field='key', index=0, literals=values)
    result = ds.dataset(data).to_table(filter=predicate.to_arrow(data.schema))
    expected = [1] if method == 'in' else [] if with_null else [-1, 0, 2]
    assert result['key'].cast(pa.int64()).to_pylist() == expected


@pytest.mark.parametrize('zone', [None, 'America/New_York'])
@pytest.mark.parametrize('method', ['in', 'notIn'])
@pytest.mark.parametrize('plain_datetime', [False, True], ids=['pandas', 'datetime'])
@pytest.mark.parametrize('with_null', [False, True])
def test_nanosecond_membership_accepts_mixed_timestamp_resolutions(zone, method, plain_datetime, with_null):
    data_type = pa.timestamp('ns', tz='UTC' if zone is not None else None)
    data = pa.table({'key': pa.array([-1, 0, 1, 2, None], pa.int64()).cast(data_type)})
    zero = (datetime(1970, 1, 1, tzinfo=timezone.utc if zone is not None else None)
            if plain_datetime else _value(0, zone))
    values = [_value(1, zone), zero] + ([None] if with_null else [])
    predicate = Predicate(method=method, field='key', index=0, literals=values)
    result = ds.dataset(data).to_table(filter=predicate.to_arrow(data.schema))
    expected = [0, 1] if method == 'in' else [] if with_null else [-1, 2]
    assert result['key'].cast(pa.int64()).to_pylist() == expected
    assert all(not isinstance(value, pa.Scalar) for value in predicate.literals)


@pytest.mark.parametrize('zone', [None, 'America/New_York'])
@pytest.mark.parametrize('method', ['between', 'notBetween'])
def test_nanosecond_ranges_accept_zero_tail_literals_in_another_timezone(zone, method):
    data_type = pa.timestamp('ns', tz='UTC' if zone is not None else None)
    data = pa.table({'key': pa.array([-1, 0, 1, 2, None], pa.int64()).cast(data_type)})
    predicate = Predicate(method=method, field='key', index=0, literals=[_value(0, zone), _value(1, zone)])
    result = ds.dataset(data).to_table(filter=predicate.to_arrow(data.schema))
    expected = [0, 1] if method == 'between' else [-1, 2]
    assert result['key'].cast(pa.int64()).to_pylist() == expected


@pytest.mark.parametrize('zone', [None, 'UTC'])
@pytest.mark.parametrize('method', ['equal', 'in', 'between'])
def test_nanosecond_literal_normalization_keeps_timezone_awareness_validation(zone, method):
    data_type = pa.timestamp('ns', tz=zone)
    data = pa.table({'key': pa.array([0, 1, 2], pa.int64()).cast(data_type)})
    values = [_value(1, 'UTC' if zone is None else None), _value(2, zone)]
    predicate = Predicate(method=method, field='key', index=0, literals=values)
    with pytest.raises((pa.ArrowException, TypeError)):
        ds.dataset(data).to_table(filter=predicate.to_arrow(data.schema))


@pytest.mark.parametrize('zone', [None, 'UTC'])
@pytest.mark.parametrize('method', ['equal', 'in'])
def test_ordinary_datetime_literals_keep_their_wider_microsecond_range(zone, method):
    tzinfo = timezone.utc if zone is not None else None
    values = [datetime(1, 1, 1, tzinfo=tzinfo), datetime(9999, 12, 31, tzinfo=tzinfo)]
    data = pa.table({'key': values}, schema=pa.schema([('key', pa.timestamp('us', tz=zone))]))
    predicate = Predicate(method=method, field='key', index=0, literals=values)
    result = ds.dataset(data).to_table(filter=predicate.to_arrow(data.schema))
    assert result['key'].to_pylist() == (values[:1] if method == 'equal' else values)


@pytest.mark.parametrize('unit', ['ms', 'us'])
@pytest.mark.parametrize('zone', [None, 'America/New_York'])
@pytest.mark.parametrize('ticks', [-1, 1])
def test_nanosecond_comparisons_do_not_overflow_wider_timestamp_columns(unit, zone, ticks):
    tzinfo = timezone.utc if zone is not None else None
    values = [datetime(1, 1, 1, tzinfo=tzinfo), datetime(1970, 1, 1, tzinfo=tzinfo),
              datetime(9999, 12, 31, tzinfo=tzinfo), None]
    data = pa.table({'key': values}, schema=pa.schema([('key', pa.timestamp(unit, tz='UTC' if zone else None))]))
    literal = _value(ticks, zone)
    expected = {
        'equal': [], 'notEqual': values[:3],
        'lessThan': values[:2] if ticks > 0 else values[:1],
        'lessOrEqual': values[:2] if ticks > 0 else values[:1],
        'greaterThan': values[2:3] if ticks > 0 else values[1:3],
        'greaterOrEqual': values[2:3] if ticks > 0 else values[1:3],
    }
    for method, rows in expected.items():
        predicate = Predicate(method=method, field='key', index=0, literals=[literal])
        result = ds.dataset(data).to_table(filter=predicate.to_arrow(data.schema))
        assert result['key'].to_pylist() == rows, method


@pytest.mark.parametrize('unit', ['ms', 'us', 'ns'])
@pytest.mark.parametrize('zone', [None, 'America/New_York'])
@pytest.mark.parametrize('with_null', [False, True])
def test_nanosecond_sets_and_ranges_accept_wide_companion_literals(unit, zone, with_null):
    data_type = pa.timestamp(unit, tz='UTC' if zone is not None else None)
    data = pa.table({'key': pa.array([-1, 0, 1, None], pa.int64()).cast(data_type)})
    far = datetime(3000, 1, 1, tzinfo=timezone.utc if zone is not None else None)
    values = [_value(1, zone), far] + ([None] if with_null else [])
    for method in ['in', 'notIn']:
        predicate = Predicate(method=method, field='key', index=0, literals=values)
        result = ds.dataset(data).to_table(filter=predicate.to_arrow(data.schema))
        expected = ([1] if unit == 'ns' else []) if method == 'in' else (
            [] if with_null else [-1, 0] if unit == 'ns' else [-1, 0, 1])
        assert result['key'].cast(pa.int64()).to_pylist() == expected, method
    for method, expected in [('between', [1]), ('notBetween', [-1, 0])]:
        predicate = Predicate(method=method, field='key', index=0, literals=values[:2])
        result = ds.dataset(data).to_table(filter=predicate.to_arrow(data.schema))
        assert result['key'].cast(pa.int64()).to_pylist() == expected, method


def test_nanosecond_literals_filter_wide_millisecond_parquet_rows(rest_catalog):
    catalog, _ = rest_catalog
    schema = pa.schema([('key', pa.timestamp('ms')), ('value', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'}), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    far = datetime(3000, 1, 1)
    try:
        writer.write_arrow(pa.table({'key': [far, datetime(1970, 1, 1), None], 'value': [0, 1, 2]}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    nano = _value(1, None)
    cases = [('equal', [nano], []), ('lessThan', [nano], [1]), ('greaterThan', [nano], [0]),
             ('in', [nano, far], [0]), ('notIn', [nano, far], [1]), ('notIn', [nano, far, None], []),
             ('between', [_value(-1, None), far], [0, 1])]
    for method, literals, expected in cases:
        read = table.new_read_builder()
        read.with_filter(Predicate(method=method, field='key', index=0, literals=literals))
        result = read.new_read().to_arrow(read.new_scan().plan().splits())
        assert sorted(result['value'].to_pylist()) == expected, method


@pytest.mark.parametrize('unit,multiplier', [('ms', 1000000), ('us', 1000)])
@pytest.mark.parametrize('zone', [None, 'America/New_York'])
def test_nanosecond_literals_are_not_truncated_to_a_lower_precision_field(unit, multiplier, zone):
    data_type = pa.timestamp(unit, tz='UTC' if zone is not None else None)
    data = pa.table({'key': pa.array([-1, 0, 1], pa.int64()).cast(data_type)})
    for method, expected in [('equal', []), ('lessThan', [-multiplier, 0]), ('greaterThan', [multiplier])]:
        predicate = Predicate(method=method, field='key', index=0, literals=[_value(1, zone)])
        result = ds.dataset(data).to_table(filter=predicate.to_arrow(data.schema))
        assert result['key'].cast(pa.timestamp('ns', tz=data_type.tz)).cast(pa.int64()).to_pylist() == expected


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('row_input', [False, True], ids=['arrow', 'row'])
@pytest.mark.parametrize('zone', [None, 'UTC', 'America/New_York', 'Asia/Shanghai'])
def test_timestamp_row_and_sliced_arrow_writes_agree_with_precomputed_java_buckets(
        rest_catalog, native, row_input, zone):
    catalog, _ = rest_catalog
    schema = pa.schema([('key', pa.timestamp('ns', tz='UTC' if zone is not None else None)),
                        ('value', pa.string())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, primary_keys=['key'], options={
        'bucket': '5', 'write.native.enabled': str(native).lower(), 'commit.native.enabled': str(native).lower(),
        'read.native.enabled': str(native).lower(), 'scan.native-plan.enabled': str(native).lower()}), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    buckets = _bucket_cases(zone)
    values = [_value(ticks, zone) for ticks in buckets]
    try:
        assert isinstance(writer, NativeTableWrite) == native
        with ExitStack() as guards:
            if native:
                guards.enter_context(patch.object(writer, '_switch_to_python', side_effect=AssertionError('fallback')))
                guards.enter_context(patch.object(
                    commit.file_store_commit, 'commit', side_effect=AssertionError('Python commit fallback')))
            if row_input:
                for value in values:
                    writer.write_row(GenericRow([value, 'original'], table.fields))
            else:
                data = pa.table({'key': [_value(42, zone)] + values + [_value(47, zone)],
                                 'value': ['unwritten'] + ['original'] * len(values) + ['unwritten']}, schema=schema)
                writer.write_arrow(data.slice(1, len(values)))
            writer.write_arrow(pa.table({'key': [_value(1, zone)], 'value': ['updated']}, schema=schema), bucket=2)
            commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    with ExitStack() as guards:
        if native:
            guards.enter_context(patch(
                'pypaimon.execution.python.read.table_read.PythonTableRead._create_split_read',
                side_effect=AssertionError('Python read fallback')))
        read = table.new_read_builder()
        rows = {}
        for split in read.new_scan().plan().splits():
            result = read.new_read().to_arrow([split])
            for ticks, value in zip(result['key'].cast(pa.int64()).to_pylist(), result['value'].to_pylist()):
                assert buckets[ticks] == split.bucket
                assert ticks not in rows, 'One timestamp key was written to different LSM buckets'
                rows[ticks] = value
        assert rows == {ticks: 'updated' if ticks == 1 else 'original' for ticks in buckets}
        for ticks in [-1, 1, 2, 1000001]:
            filtered = table.new_read_builder()
            filtered.with_filter(filtered.new_predicate_builder().equal('key', _value(ticks, zone)))
            result = filtered.new_read().to_arrow(filtered.new_scan().plan().splits())
            assert result['key'].cast(pa.int64()).to_pylist() == [ticks]
        filtered = table.new_read_builder()
        predicate = filtered.new_predicate_builder()
        filtered.with_filter(predicate.and_predicates([
            predicate.greater_or_equal('key', _value(-1, zone)), predicate.less_than('key', _value(2, zone))]))
        result = filtered.new_read().to_arrow(filtered.new_scan().plan().splits())
        assert sorted(result['key'].cast(pa.int64()).to_pylist()) == [-1, 0, 1]
