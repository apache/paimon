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

"""Fixed-bucket writers honor Java's configured MOD routing."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.tests import native_plan_rest_test
from pypaimon.write.native_write import NativeTableWrite
from pypaimon.write.row_key_extractor import FixedBucketRowKeyExtractor


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
rest_catalog = native_plan_rest_test.rest_catalog
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]


def _schema(data_type, options=None):
    return Schema.from_pyarrow_schema(
        pa.schema([('id', data_type), ('value', pa.string())]), primary_keys=['id'],
        options={'bucket': '5', 'bucket-function.type': 'mod', **(options or {})})


def _extractor(schema):
    return FixedBucketRowKeyExtractor(TableSchema(
        fields=schema.fields, primary_keys=schema.primary_keys, options=schema.options))


@pytest.mark.parametrize('data_type,minimum,maximum', [
    (pa.int32(), -(2 ** 31), 2 ** 31 - 1),
    (pa.int64(), -(2 ** 63), 2 ** 63 - 1),
])
def test_mod_routes_rows_and_arrow_using_floor_mod(data_type, minimum, maximum):
    extractor = _extractor(_schema(data_type))
    values = [minimum, -17, -3, -1, 0, 5, 17, maximum, None]
    expected = [0 if value is None else value % 5 for value in values]
    assert [extractor.extract_partition_bucket_row({'id': value})[1] for value in values] == expected
    batch = pa.record_batch([pa.array(values, data_type)], names=['id'])
    assert extractor.extract_partition_bucket_batch(batch)[1] == expected


@pytest.mark.parametrize('data_type', [pa.int8(), pa.int16(), pa.float64(), pa.string()])
def test_mod_rejects_non_int_or_bigint_bucket_keys(data_type):
    with pytest.raises(ValueError, match='bucket key type must be INT or BIGINT'):
        _extractor(_schema(data_type))


def test_mod_requires_exactly_one_bucket_key():
    schema = Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32()), ('other', pa.int32())]),
        primary_keys=['id', 'other'], options={'bucket': '5', 'bucket-function.type': 'mod'})
    with pytest.raises(ValueError, match='bucket key must have exactly one field'):
        _extractor(schema)


@pytest.mark.parametrize('bucket_function,error', [
    ('hive', NotImplementedError), ('unknown', ValueError), ('', ValueError),
])
def test_python_cannot_silently_route_an_unsupported_bucket_function(bucket_function, error):
    with pytest.raises(error, match='[Bb]ucket function'):
        extractor = _extractor(_schema(pa.int32(), {'bucket-function.type': bucket_function}))
        extractor.extract_partition_bucket_row({'id': 17})


def test_none_bucket_function_retains_the_default_routing():
    unset = _extractor(_schema(pa.int32(), {'bucket-function.type': None}))
    default = _extractor(_schema(pa.int32(), {'bucket-function.type': 'default'}))
    batch = pa.record_batch([pa.array([-3, 0, 17], pa.int32())], names=['id'])
    assert unset.extract_partition_bucket_batch(batch) == default.extract_partition_bucket_batch(batch)
    for value in [-3, 0, 17]:
        assert unset.extract_partition_bucket_row({'id': value}) == default.extract_partition_bucket_row({'id': value})


def test_hive_single_bucket_retains_existing_row_and_arrow_routing():
    extractor = _extractor(_schema(pa.int32(), {'bucket-function.type': 'hive', 'bucket': '1'}))
    batch = pa.record_batch([pa.array([-3, 0, 17], pa.int32())], names=['id'])
    assert extractor.extract_partition_bucket_batch(batch)[1] == [0, 0, 0]
    for value in [-3, 0, 17]:
        assert extractor.extract_partition_bucket_row({'id': value})[1] == 0


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
def test_hive_precomputed_bucket_does_not_calculate_hashes(rest_catalog, native):
    catalog, _ = rest_catalog
    catalog.create_table('default.t', _schema(pa.int32(), {
        'bucket-function.type': 'hive', 'write.native.enabled': str(native).lower(),
        'commit.native.enabled': str(native).lower(), 'read.native.enabled': 'false',
        'scan.native-plan.enabled': 'false'}), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        assert isinstance(writer, NativeTableWrite) == native
        with ExitStack() as guard:
            if native:
                guard.enter_context(patch.object(writer, '_switch_to_python', side_effect=AssertionError('fallback')))
                guard.enter_context(patch.object(
                    commit.file_store_commit, 'commit', side_effect=AssertionError('Python commit fallback')))
            else:
                guard.enter_context(patch.object(
                    writer.row_key_extractor, '_extract_buckets_batch',
                    side_effect=AssertionError('automatic routing')))
            writer.write_arrow(pa.table({'id': pa.array([17], pa.int32()), 'value': ['explicit']}), bucket=3)
            messages = writer.prepare_commit()
            assert [message.bucket for message in messages] == [3]
            commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    read = table.new_read_builder()
    assert read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist() == [{'id': 17, 'value': 'explicit'}]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('streaming', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('row_input', [False, True], ids=['arrow', 'row'])
@pytest.mark.parametrize('data_type,minimum,maximum', [
    (pa.int32(), -(2 ** 31), 2 ** 31 - 1),
    (pa.int64(), -(2 ** 63), 2 ** 63 - 1),
])
def test_mod_regular_and_precomputed_writes_share_buckets(
        rest_catalog, native, streaming, row_input, data_type, minimum, maximum):
    catalog, _ = rest_catalog
    catalog.create_table('default.t', _schema(data_type, {
        'write.native.enabled': str(native).lower(), 'commit.native.enabled': str(native).lower(),
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'}), False)
    table = catalog.get_table('default.t')
    builder = table.new_stream_write_builder() if streaming else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    values = [minimum, -17, -3, -1, 0, 5, 17, maximum]
    arrow_schema = pa.schema([('id', data_type), ('value', pa.string())])
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
                writer.write_arrow(pa.Table.from_pydict(
                    {'id': values, 'value': ['original'] * len(values)}, schema=arrow_schema))
            # This upstream bucket must agree with normal assignment. Otherwise
            # the same key is committed twice to different LSM bucket trees.
            writer.write_arrow(pa.Table.from_pydict(
                {'id': [-3], 'value': ['updated']}, schema=arrow_schema), bucket=2)
            messages = writer.prepare_commit(7) if streaming else writer.prepare_commit()
            commit.commit(messages, 7) if streaming else commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    read_builder = table.new_read_builder()
    reader = read_builder.new_read()
    rows = []
    for split in read_builder.new_scan().plan().splits():
        split_rows = reader.to_arrow([split]).to_pylist()
        assert all(row['id'] % 5 == split.bucket for row in split_rows)
        rows.extend(split_rows)
    assert sorted(rows, key=lambda row: row['id']) == [
        {'id': value, 'value': 'updated' if value == -3 else 'original'} for value in sorted(values)]
