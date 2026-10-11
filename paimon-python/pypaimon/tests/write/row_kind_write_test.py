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


"""InternalRow kinds survive the Arrow conversion used by both PK writers."""

import pyarrow as pa
import pytest
from unittest.mock import patch

from pypaimon import CatalogFactory, Schema
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind
from pypaimon.write.native_write import NativeTableWrite


@pytest.fixture(params=[False, pytest.param(True, marks=pytest.mark.native_plan)],
                ids=['python', 'native'])
def native_write(request):
    return request.param


def _table(tmp_path, native_write, options=None, with_op=False):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int64()), ('value', pa.int64())])
    if with_op:
        schema = schema.append(pa.field('op', pa.string()))
    config = {'bucket': '1', 'changelog-producer': 'input',
              'write.native.enabled': str(native_write).lower(),
              'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false'}
    config.update(options or {})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'], options=config), False)
    return catalog.get_table('default.t')


def _write(table, rows, native_write, stream=False):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    if native_write:
        assert isinstance(writer, NativeTableWrite)
    try:
        for values, kind in rows:
            writer.write_row(GenericRow(values, table.fields, kind))
        if native_write:
            assert writer._python_writer is None
        if stream:
            commit.commit(writer.prepare_commit(1), 1)
        else:
            commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _rows(table, changelog=False):
    if changelog:
        table = table.copy({'scan.mode': 'incremental',
                            'incremental-between-timestamp': '0,9223372036854775807'})
    builder = table.new_read_builder()
    reader = builder.new_read()
    reader.include_row_kind = changelog
    result = reader.to_arrow(builder.new_scan().plan().splits(), parallelism=1).to_pydict()
    return sorted(zip(*(result[name] for name in result)))


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('bucket', ['1', '-1'])
def test_internal_row_kinds_reach_data_and_input_changelog(tmp_path, native_write, stream, bucket):
    table = _table(tmp_path, native_write, {'bucket': bucket})
    _write(table, [
        ([1, 10], RowKind.INSERT), ([1, 20], RowKind.UPDATE_AFTER),
        ([2, 30], RowKind.DELETE), ([3, 40], RowKind.UPDATE_BEFORE),
        ([4, 50], RowKind.INSERT)], native_write, stream)
    assert _rows(table) == [(1, 20), (4, 50)]
    assert _rows(table, True) == [
        ('+I', 1, 10), ('+I', 4, 50), ('+U', 1, 20),
        ('-D', 2, 30), ('-U', 3, 40)]


@pytest.mark.parametrize('options,expected', [
    ({'ignore-delete': 'true'}, [('+I', 1, 10), ('+U', 3, 40)]),
    ({'ignore-update-before': 'true'}, [('+I', 1, 10), ('+U', 3, 40), ('-D', 1, 20)]),
])
def test_internal_row_kinds_are_filtered_before_dynamic_bucket_assignment(
        tmp_path, native_write, options, expected):
    options = dict(options, bucket='-1')
    table = _table(tmp_path, native_write, options)
    _write(table, [
        ([1, 10], RowKind.INSERT), ([1, 20], RowKind.DELETE),
        ([2, 30], RowKind.UPDATE_BEFORE), ([3, 40], RowKind.UPDATE_AFTER)], native_write)
    assert _rows(table, True) == expected


def test_rowkind_field_overrides_internal_row_kind(tmp_path, native_write):
    table = _table(tmp_path, native_write, {'rowkind.field': 'op'}, with_op=True)
    _write(table, [
        ([1, 10, '+I'], RowKind.DELETE),
        ([2, 20, '-D'], RowKind.INSERT)], native_write)
    assert _rows(table) == [(1, 10, '+I')]
    assert _rows(table, True) == [('+I', 1, 10, '+I'), ('-D', 2, 20, '-D')]


@pytest.mark.parametrize('row_first', [False, True])
@pytest.mark.parametrize('producer', ['none', 'input'])
def test_mixed_arrow_and_row_writes_preserve_kinds(tmp_path, native_write, row_first, producer):
    table = _table(tmp_path, native_write, {'changelog-producer': producer})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    schema = pa.schema([('id', pa.int64()), ('value', pa.int64())])
    try:
        if row_first:
            writer.write_row(GenericRow([1, 10], table.fields, RowKind.INSERT))
            writer.write_arrow(pa.Table.from_pydict(
                {'id': [1], 'value': [20]}, schema=schema))
        else:
            writer.write_arrow(pa.Table.from_pydict(
                {'id': [1], 'value': [10]}, schema=schema))
            writer.write_row(GenericRow([1, 20], table.fields, RowKind.DELETE))
        if native_write:
            assert isinstance(writer, NativeTableWrite)
            assert writer._python_writer is None
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert _rows(table) == ([(1, 20)] if row_first else [])
    if producer == 'input':
        assert _rows(table, True) == [('+I', 1, 10), ('+I' if row_first else '-D', 1, 20)]


@pytest.mark.parametrize('failure', ['later_changelog', 'data_after_changelogs'])
def test_input_changelog_retry_preserves_published_chunks_and_all_inputs(tmp_path, failure):
    table = _table(tmp_path, False)
    builder = table.new_stream_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pydict(
            {'id': [1, 1, 1], 'value': [10, 20, 30]},
            schema=pa.schema([('id', pa.int64()), ('value', pa.int64())])))
        # Delay rolling until prepare so a retry can preserve already published
        # log chunks. These rows fold into one data row, but remain three logs.
        bucket_writer = next(iter(writer.file_store_write.data_writers.values()))
        bucket_writer.target_file_size = 1
        original = table.file_io.write_parquet
        state = {'changelogs': 0, 'failed': False}

        def fail_once(path, data, **kwargs):
            if '/changelog-' in str(path):
                state['changelogs'] += 1
                should_fail = failure == 'later_changelog' and state['changelogs'] == 2
            else:
                should_fail = failure == 'data_after_changelogs'
            if should_fail and not state['failed']:
                state['failed'] = True
                raise IOError('transient file write failure')
            return original(path, data, **kwargs)

        with patch.object(table.file_io, 'write_parquet', side_effect=fail_once):
            with pytest.raises(IOError, match='transient file write failure'):
                writer.prepare_commit(1)
            messages = writer.prepare_commit(1)
        logs = [meta for message in messages for meta in message.changelog_files]
        assert len(logs) == 3
        assert sum(meta.row_count for meta in logs) == 3
        assert [(meta.min_sequence_number, meta.max_sequence_number) for meta in logs] == [(0, 0), (1, 1), (2, 2)]
        paths = [meta.physical_path() for meta in logs]
        assert all(table.file_io.exists(path) for path in paths)
        commit.commit(messages, 1)
        assert _rows(table) == [(1, 30)]
        assert _rows(table, True) == [('+I', 1, 10), ('+I', 1, 20), ('+I', 1, 30)]
        assert writer.prepare_commit(2) == []
    finally:
        writer.close()
        commit.close()
    # Successful prepare transfers file ownership to the messages. Closing
    # the writer must retain the logs even after an earlier failed prepare.
    assert all(table.file_io.exists(path) for path in paths)
