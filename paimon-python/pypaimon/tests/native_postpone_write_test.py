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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Native pending-bucket writes preserve Java replay order and file metadata."""

from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.write.native_write import NativeTableWrite

pytestmark = pytest.mark.native_plan
_SCHEMA = pa.schema([('id', pa.int32()), ('p', pa.string()),
                     ('v', pa.int32()), ('op', pa.string())])


def _table(tmp_path, options=None, catalog=None):
    catalog = catalog or CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    opts = {'bucket': '-2', 'file.format': 'parquet', 'rowkind.field': 'op',
            'target-file-row-num': '3', 'write.native.enabled': 'true'}
    opts.update(options or {})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, primary_keys=['id', 'p'], partition_keys=['p'], options=opts), False)
    return catalog.get_table('default.t')


def _batch(rows):
    return pa.RecordBatch.from_pylist([dict(zip(_SCHEMA.names, row)) for row in rows], schema=_SCHEMA)


def _file_rows(table, files):
    rows = []
    for file in files:
        # Inspect the unsorted physical file, before the deferred compaction.
        with table.file_io.new_input_stream(file.file_path) as source:
            values = pq.ParquetFile(source).read().to_pylist()
        assert len(values) == file.row_count
        assert sum(row['_VALUE_KIND'] in (1, 3) for row in values) == file.delete_row_count
        assert [row['_SEQUENCE_NUMBER'] for row in values] == list(range(
            file.min_sequence_number, file.max_sequence_number + 1))
        rows.extend(tuple(row[name] for name in _SCHEMA.names) for row in values)
    return rows


@pytest.mark.parametrize('native_commit', [False, True])
@pytest.mark.parametrize('stream', [False, True])
def test_postpone_native_files_roll_and_preserve_replay_order(
        tmp_path, native_rest_catalog, native_commit, stream):
    table = _table(tmp_path, {'commit.native.enabled': str(native_commit).lower()}, native_rest_catalog)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        for checkpoint in range(1, 3 if stream else 2):
            data = [(5, 'a/b', 10, '+I'), (1, 'a/b', 20, '-U'),
                    (3, 'a/b', 30, '+U'), (2, 'a/b', 40, '-D'), (1, 'a/b', 50, '+I')]
            for start in (0, 2, 4):
                writer.write_arrow_batch(_batch(data[start:start + 2]))
            messages = writer.prepare_commit(checkpoint) if stream else writer.prepare_commit()
            assert writer._python_writer is None
            assert {m.bucket for m in messages} == {-2}
            files = [f for m in messages for f in m.new_files]
            assert [f.row_count for f in files] == [4, 1]
            assert all('-u-' + builder.commit_user + '-s-' in f.file_name for f in files)
            assert [(f.min_key.values, f.max_key.values) for f in files] == [([5], [2]), ([1], [1])]
            assert _file_rows(table, files) == data
            if native_commit:
                with patch.object(commit.file_store_commit, 'commit',
                                  side_effect=AssertionError('Python commit fallback')):
                    commit.commit(messages, checkpoint) if stream else commit.commit(messages)
            else:
                commit.commit(messages, checkpoint) if stream else commit.commit(messages)
        if stream:
            assert writer.prepare_commit(3) == []
    finally:
        writer.close()
        commit.close()
    # Postpone records become visible only after assignment to real buckets.
    for native in (False, True):
        read = table.copy({'scan.native-plan.enabled': str(native).lower(),
                           'read.native.enabled': str(native).lower()}).new_read_builder()
        assert read.new_scan().plan().splits() == []


@pytest.mark.parametrize('native_commit', [False, True])
def test_postpone_native_overwrite_and_abort(tmp_path, native_rest_catalog, native_commit):
    table = _table(tmp_path, {'commit.native.enabled': str(native_commit).lower()}, native_rest_catalog)
    all_files = []
    for overwrite in (False, True):
        builder = table.new_batch_write_builder()
        if overwrite:
            builder.overwrite()
        writer, commit = builder.new_write(), builder.new_commit()
        assert isinstance(writer, NativeTableWrite)
        try:
            writer.write_arrow_batch(_batch([(1, 'a', 20 if overwrite else 10, '+I')]))
            messages = writer.prepare_commit()
            files = [f for m in messages for f in m.new_files]
            assert _file_rows(table, files) == [(1, 'a', 20 if overwrite else 10, '+I')]
            all_files.extend(files)
            if native_commit:
                with patch.object(commit.file_store_commit, 'commit',
                                  side_effect=AssertionError('Python commit fallback')):
                    commit.commit(messages)
            else:
                commit.commit(messages)
            writer.abort()
            assert all(table.file_io.exists(f.file_path) for f in files)
        finally:
            writer.close()
            commit.close()
    assert table.snapshot_manager().get_latest_snapshot().total_record_count == 1
    assert len({f.file_name.split('-w-')[0] for f in all_files}) == 2
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow_batch(_batch([(2, 'b', 30, '+I')]))
        messages = writer.prepare_commit()
        paths = [f.file_path for m in messages for f in m.new_files]
        writer.abort()
        assert paths and all(not table.file_io.exists(path) for path in paths)
    finally:
        writer.close()


@pytest.mark.parametrize('engine', ['first-row', 'partial-update', 'aggregation'])
def test_postpone_engine_retract_validation_stays_on_python(tmp_path, engine):
    table = _table(tmp_path, {'merge-engine': engine})
    writer = table.new_batch_write_builder().new_write()
    try:
        assert not isinstance(writer, NativeTableWrite)
    finally:
        writer.close()


@pytest.mark.parametrize('operation', ['truncate_table', 'truncate_partition', 'delete_partition'])
def test_pending_files_participate_in_partition_metadata_deletes(tmp_path, operation):
    table = _table(tmp_path, {'commit.native.enabled': 'false'})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow_batch(_batch([(1, 'a', 10, '+I'), (2, 'b', 20, '+I')]))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    builder = table.new_batch_write_builder()
    commit = builder.new_commit()
    try:
        if operation == 'truncate_table':
            commit.truncate_table()
        elif operation == 'truncate_partition':
            commit.truncate_partitions([{'p': 'a'}])
        else:
            predicate = table.new_read_builder().new_predicate_builder().equal('p', 'a')
            messages = builder.new_update().delete_by_predicate(predicate)
            assert len(messages) == 1
            assert messages[0].bucket == -2
            assert len(messages[0].deleted_files) == 1
            commit.commit(messages)
    finally:
        commit.close()
    expected = 0 if operation == 'truncate_table' else 1
    assert table.snapshot_manager().get_latest_snapshot().total_record_count == expected
    from pypaimon.manifest.manifest_list_manager import ManifestListManager
    from pypaimon.read.scanner.file_scanner import FileScanner
    snapshot = table.snapshot_manager().get_latest_snapshot()
    entries = FileScanner(table, lambda: ([], None)).with_all_buckets().read_manifest_entries(
        ManifestListManager(table).read_all(snapshot))
    assert [(tuple(e.partition.values), e.bucket) for e in entries] == ([] if expected == 0 else [(('b',), -2)])
