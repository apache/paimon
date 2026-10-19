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

"""Selected append input columns and upstream buckets retain Java semantics."""

from contextlib import ExitStack
from pathlib import Path
from urllib.parse import unquote, urlparse
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.tests import native_plan_rest_test
from pypaimon.write.native_write import NativeTableWrite


rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
native = pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')
_ENGINES = [False, pytest.param(True, marks=native)]
_SCHEMA = pa.schema([('id', pa.int64()), ('pt', pa.int64()), ('value', pa.int64())])


def _files(table, pattern='*'):
    root = Path(unquote(urlparse(table.table_path).path))
    assert root.is_dir(), table.table_path
    return set(root.rglob(pattern))


def _table(catalog, use_native, partitioned=False, bucketed=False, buckets=4, schema=_SCHEMA):
    options = {'write.native.enabled': str(use_native).lower(), 'commit.native.enabled': 'false',
               'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'}
    if bucketed:
        options.update({'bucket': str(buckets), 'bucket-key': 'id'})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['pt'] if partitioned else [], options=options), False)
    return catalog.get_table('default.t')


def _rows(table):
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('value').to_pylist()


def _native_only(stack, writer, use_native):
    assert isinstance(writer, NativeTableWrite) == use_native
    if use_native:
        stack.enter_context(patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python fallback')))


def _finish(builder, writer):
    messages = writer.prepare_commit()
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    return messages


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('partitioned', [False, True])
@pytest.mark.parametrize('full_input', [False, True])
def test_upstream_bucket_subset_omits_hash_columns_without_leaking_full_input(
        rest_catalog, use_native, partitioned, full_input):
    catalog, _ = rest_catalog
    table = _table(catalog, use_native, partitioned, bucketed=True)
    columns = ['value', 'pt'] if partitioned else ['value']
    data = pa.Table.from_pylist([dict(id=1, pt=7, value=42)], schema=_SCHEMA)
    data = data if full_input else data.select(columns).to_batches()[0]
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        with ExitStack() as stack:
            _native_only(stack, writer, use_native)
            writer.with_write_type(columns)
            with pytest.raises(ValueError, match='missing.*not in table schema|Column.*missing'):
                writer.with_write_type(['missing'])
            before = _files(table)
            # Java does not route zero rows, even when the selected input
            # could only be written with an upstream bucket for nonempty data.
            writer.write_arrow(data.slice(0, 0))
            assert _files(table) == before
            if not full_input:
                with pytest.raises(ValueError, match="routing column 'id'"):
                    writer.write_arrow(data)
                assert _files(table) == before
            writer.write_arrow(data, bucket=2)
            messages = _finish(builder, writer)
        assert [message.bucket for message in messages] == [2]
        assert all(file.write_cols == columns for message in messages for file in message.new_files)
        assert _rows(table) == [dict(id=None, pt=7 if partitioned else None, value=42)]
    finally:
        writer.close()


@pytest.mark.parametrize('use_native', _ENGINES)
def test_partial_upstream_groups_reject_invalid_buckets_and_mixed_partitions(rest_catalog, use_native):
    catalog, _ = rest_catalog
    table = _table(catalog, use_native, partitioned=True, bucketed=True)
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    data = pa.table({'value': [42, 43], 'pt': [7, 8]})
    try:
        with ExitStack() as stack:
            _native_only(stack, writer, use_native)
            writer.with_write_type(['value', 'pt'])
            before = _files(table)
            for bucket in (-1, 4):
                with pytest.raises(ValueError, match='Bucket id'):
                    writer.write_arrow(data, bucket=bucket)
            with pytest.raises(ValueError, match='multiple partitions'):
                writer.write_arrow(data, bucket=2)
            assert _files(table) == before
            writer.write_arrow(data.slice(0, 1), bucket=2)
            _finish(builder, writer)
        assert _rows(table) == [dict(id=None, pt=7, value=42)]
    finally:
        writer.close()


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('partitioned', [False, True])
@pytest.mark.parametrize('full_input', [False, True])
def test_complete_reordered_append_type_keeps_field_identity_and_copies_column_names(
        rest_catalog, use_native, partitioned, full_input):
    catalog, _ = rest_catalog
    table = _table(catalog, use_native, partitioned)
    columns = ['value', 'pt', 'id']
    expected_columns = list(columns)
    data = pa.Table.from_pylist([dict(id=1, pt=7, value=42)], schema=_SCHEMA)
    data = data if full_input else data.select(columns)
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        with ExitStack() as stack:
            _native_only(stack, writer, use_native)
            writer.with_write_type(columns)
            columns.reverse()
            writer.write_arrow(data)
            messages = _finish(builder, writer)
        assert all(file.write_cols == expected_columns for message in messages for file in message.new_files)
        assert _rows(table) == [dict(id=1, pt=7, value=42)]
    finally:
        writer.close()


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('buckets', [1, 4])
@pytest.mark.parametrize('columns', [('value', 'pt'), ('value',)])
def test_full_input_routes_before_projecting_unwritten_routing_fields(
        rest_catalog, use_native, buckets, columns):
    catalog, _ = rest_catalog
    table = _table(catalog, use_native, partitioned=True, bucketed=True, buckets=buckets)
    data = pa.Table.from_pylist([
        dict(id=1, pt=7, value=42), dict(id=None, pt=None, value=43),
    ], schema=_SCHEMA)
    partitions, bucket_ids = table.create_row_key_extractor().extract_partition_bucket_batch(data.to_batches()[0])
    expected_buckets = dict(zip(partitions, bucket_ids))
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        with ExitStack() as stack:
            _native_only(stack, writer, use_native)
            writer.with_write_type(list(columns))
            before = _files(table)
            writer.write_arrow(data.slice(0, 0))
            assert _files(table) == before
            writer.write_arrow(data)
            messages = _finish(builder, writer)
        assert {tuple(message.partition): message.bucket for message in messages} == expected_buckets
        assert all(file.write_cols == list(columns) for message in messages for file in message.new_files)
        assert _rows(table) == [dict(id=None, pt=7, value=42), dict(id=None, pt=None, value=43)]
    finally:
        writer.close()


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('partial_row', [False, True])
@pytest.mark.parametrize('unwritten_blob', [False, True])
def test_rows_route_with_unwritten_columns_without_requiring_other_fields(
        rest_catalog, use_native, partial_row, unwritten_blob):
    from pypaimon.table.row.blob import Blob

    catalog, _ = rest_catalog
    schema = _SCHEMA.append(pa.field('unwritten', pa.large_binary() if unwritten_blob else pa.int64()))
    table = _table(catalog, use_native, partitioned=True, bucketed=True, schema=schema)
    fields = {field.name: field for field in table.fields}
    names = ['value', 'pt', 'id'] if partial_row else list(schema.names)
    blob = Mock(spec=Blob)
    blob.new_input_stream.side_effect = AssertionError('unwritten Blob was opened')
    values = dict(id=1, pt=7, value=42, unwritten=blob if unwritten_blob else 999)
    row = GenericRow([values[name] for name in names], [fields[name] for name in names])
    full_input = pa.Table.from_pylist([dict(values, unwritten=None)], schema=schema).to_batches()[0]
    _, buckets = table.create_row_key_extractor().extract_partition_bucket_batch(full_input)
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        with ExitStack() as stack:
            _native_only(stack, writer, use_native)
            writer.with_write_type(['value'])
            writer.write_row(row)
            messages = _finish(builder, writer)
        assert [(tuple(message.partition), message.bucket) for message in messages] == [((7,), buckets[0])]
        assert all(file.write_cols == ['value'] for message in messages for file in message.new_files)
        assert _rows(table) == [dict(id=None, pt=7, value=42, unwritten=None)]
        blob.new_input_stream.assert_not_called()
        blob.to_descriptor.assert_not_called()
    finally:
        writer.close()


@pytest.mark.parametrize('use_native', _ENGINES)
def test_upstream_bucket_partial_overwrite_preserves_other_partitions(rest_catalog, use_native):
    catalog, _ = rest_catalog
    table = _table(catalog, use_native, partitioned=True, bucketed=True)
    seed = table.new_batch_write_builder()
    writer = seed.new_write()
    try:
        for row in [dict(id=1, pt=7, value=10), dict(id=2, pt=8, value=20)]:
            writer.write_arrow(pa.Table.from_pylist([row], schema=_SCHEMA), bucket=0)
        _finish(seed, writer)
    finally:
        writer.close()
    builder = table.new_batch_write_builder().overwrite({'pt': 7})
    writer = builder.new_write()
    try:
        with ExitStack() as stack:
            _native_only(stack, writer, use_native)
            writer.with_write_type(['value', 'pt'])
            writer.write_arrow(pa.table({'value': [42], 'pt': [7]}), bucket=2)
            messages = _finish(builder, writer)
        assert [message.bucket for message in messages] == [2]
        assert _rows(table) == [dict(id=2, pt=8, value=20), dict(id=None, pt=7, value=42)]
    finally:
        writer.close()
