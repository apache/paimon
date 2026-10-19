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
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
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


def _table(catalog, use_native, partitioned=False, bucketed=False, buckets=4):
    options = {'write.native.enabled': str(use_native).lower(), 'commit.native.enabled': 'false',
               'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'}
    if bucketed:
        options.update({'bucket': str(buckets), 'bucket-key': 'id'})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, partition_keys=['pt'] if partitioned else [], options=options), False)
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


@pytest.mark.parametrize('buckets', [1, 4])
def test_python_full_input_routes_before_projecting_unwritten_hash_fields(rest_catalog, buckets):
    catalog, _ = rest_catalog
    table = _table(catalog, False, partitioned=True, bucketed=True, buckets=buckets)
    builder = table.new_batch_write_builder()
    writer = builder.new_write().with_write_type(['value', 'pt'])
    try:
        writer.write_arrow(pa.Table.from_pylist([dict(id=1, pt=7, value=42)], schema=_SCHEMA))
        messages = _finish(builder, writer)
        assert all(0 <= message.bucket < buckets for message in messages)
        assert all(file.write_cols == ['value', 'pt'] for message in messages for file in message.new_files)
        assert _rows(table) == [dict(id=None, pt=7, value=42)]
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
