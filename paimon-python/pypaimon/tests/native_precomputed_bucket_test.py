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

"""Native writes of the partition/bucket groups produced by Daft."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.write.native_write import NativeTableWrite


pytestmark = pytest.mark.native_plan


def _table(tmp_path, bucket, partitioned=False, options=None, primary_key=True):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int64()), ('value', pa.string()), ('pt', pa.string())]),
        primary_keys=(['id', 'pt'] if partitioned else ['id']) if primary_key else [],
        partition_keys=['pt'] if partitioned else [],
        options={'bucket': str(bucket), 'file.format': 'parquet',
                 'write.native.enabled': 'true', 'read.native.enabled': 'true',
                 'dynamic-bucket.target-row-num': '1', **(options or {})}), False)
    return catalog.get_table('default.t')


def _batch(ids, values, partitions=None):
    return pa.record_batch([pa.array(ids, pa.int64()), pa.array(values, pa.string()),
                            pa.array(partitions or ['a'] * len(ids), pa.string())],
                           names=['id', 'value', 'pt'])


def _rows(table):
    builder = table.new_read_builder()
    return sorted(builder.new_read().to_arrow(
        builder.new_scan().plan().splits()).to_pylist(), key=lambda row: (row['pt'], row['id']))


def _indexes(table):
    snapshot = table.snapshot_manager().get_latest_snapshot()
    return [entry for entry in IndexFileHandler(table).scan(snapshot)
            if entry.index_file.index_type == 'HASH']


@pytest.mark.parametrize('streaming', [False, True])
def test_native_precomputed_fixed_bucket_after_regular_write(tmp_path, streaming):
    table = _table(tmp_path, 4)
    builder = (table.new_stream_write_builder() if streaming
               else table.new_batch_write_builder())
    writer = builder.new_write()
    assert isinstance(writer, NativeTableWrite)
    try:
        with patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python fallback')):
            writer.write_arrow_batch(_batch([1], ['regular']))
            writer.write_arrow_batch_to_bucket(_batch([2, 2], ['old', 'new']), 3)
            messages = writer.prepare_commit(7) if streaming else writer.prepare_commit()
        assert any(message.bucket == 3 for message in messages)
        commit = builder.new_commit()
        try:
            commit.commit(messages, 7) if streaming else commit.commit(messages)
        finally:
            commit.close()
    finally:
        writer.close()
    assert _rows(table) == [{'id': 1, 'value': 'regular', 'pt': 'a'},
                            {'id': 2, 'value': 'new', 'pt': 'a'}]


@pytest.mark.parametrize('partitioned', [False, True])
def test_native_dynamic_index_keeps_upstream_bucket_on_restart(tmp_path, partitioned):
    table = _table(tmp_path, -1, partitioned)
    for ids, values in [([1, 2], ['one', 'two']), ([2, 3], ['updated', 'three'])]:
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        assert isinstance(writer, NativeTableWrite)
        try:
            with patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python fallback')):
                assert writer.with_dynamic_bucket_index() is writer
                writer.write_arrow_batch_to_bucket(_batch(ids, values), 17)
                messages = writer.prepare_commit()
            assert {message.bucket for message in messages} == {17}
            builder.new_commit().commit(messages)
        finally:
            writer.close()
    indexes = _indexes(table)
    assert len(indexes) == 1
    assert indexes[0].bucket == 17
    assert indexes[0].index_file.row_count == 3
    assert _rows(table) == [{'id': 1, 'value': 'one', 'pt': 'a'},
                            {'id': 2, 'value': 'updated', 'pt': 'a'},
                            {'id': 3, 'value': 'three', 'pt': 'a'}]


def _hashes(table, batch):
    from pypaimon.write.row_key_extractor import DynamicBucketRowKeyExtractor

    extractor = DynamicBucketRowKeyExtractor(table.table_schema)
    return extractor.extract_hashes_batch(batch)[2]


@pytest.mark.parametrize('streaming', [False, True])
@pytest.mark.parametrize('partitioned', [False, True])
def test_supplied_java_hashes_and_new_mapping_flags(tmp_path, streaming, partitioned):
    table = _table(tmp_path, -1, partitioned)
    builder = (table.new_stream_write_builder() if streaming
               else table.new_batch_write_builder())
    writer = builder.new_write()
    first = _batch([1, 2], ['one', 'two'])
    # PyPaimon carries unsigned BinaryRow hashes through the Daft shuffle.
    first_hashes = [value & 0xffffffff for value in _hashes(table, first)]
    try:
        with patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python fallback')):
            writer.with_dynamic_bucket_index(base_snapshot_id=0)
            writer.write_arrow_batch_to_bucket(first, 12, first_hashes, [True, True])
            # A second batch can refer to this writer's uncommitted mappings.
            updated = _batch([2, 3], ['updated', 'three'])
            writer.write_arrow_batch_to_bucket(updated, 12, _hashes(table, updated), [False, True])
            messages = writer.prepare_commit(8) if streaming else writer.prepare_commit()
        assert len(messages) == 1
        assert messages[0].index_adds[0].index_file.row_count == 3
        commit = builder.new_commit()
        try:
            commit.commit(messages, 8) if streaming else commit.commit(messages)
        finally:
            commit.close()
    finally:
        writer.close()
    assert _rows(table) == [{'id': 1, 'value': 'one', 'pt': 'a'},
                            {'id': 2, 'value': 'updated', 'pt': 'a'},
                            {'id': 3, 'value': 'three', 'pt': 'a'}]
    # Restored hashes and indexes remain readable by Python's assigner.
    from pypaimon.write.row_key_extractor import DynamicBucketRowKeyExtractor

    extractor = DynamicBucketRowKeyExtractor(table.table_schema, table=table)
    assert extractor.extract_partition_bucket_batch(_batch([1, 2, 3], ['a', 'b', 'c']))[1] == [12, 12, 12]


@pytest.mark.parametrize('bucket', [4, -1])
@pytest.mark.parametrize('invalid', ['bucket', 'hash_count', 'flag_count', 'flags_without_hashes', 'partition'])
def test_invalid_precomputed_group_cannot_stage_half_a_partition(tmp_path, bucket, invalid):
    table = _table(tmp_path, bucket, partitioned=True)
    writer = table.new_batch_write_builder().new_write()
    batch = _batch([1, 2], ['a', 'b'])
    args = {
        'bucket': (-1, None, None),
        'hash_count': (0, [1], None),
        'flag_count': (0, [1, 2], [True]),
        'flags_without_hashes': (0, None, [True, True]),
        'partition': (0, None, None),
    }
    if invalid == 'partition':
        batch = _batch([1, 2], ['a', 'b'], ['a', 'b'])
    try:
        with patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python fallback')):
            with pytest.raises(ValueError):
                writer.write_arrow_batch_to_bucket(batch, *args[invalid])
            assert writer.prepare_commit() == []
    finally:
        writer.close()
    assert table.snapshot_manager().get_latest_snapshot() is None


def test_precomputed_append_projects_full_or_selected_schema(tmp_path):
    table = _table(tmp_path, 4, partitioned=True, primary_key=False,
                   options={'bucket-key': 'id'})
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        with patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python fallback')):
            writer.with_write_type(['pt', 'id'])
            writer.write_arrow_batch_to_bucket(_batch([1], ['omitted']), 3)
            writer.write_arrow_batch_to_bucket(_batch([2], ['omitted']).select(['pt', 'id']), 3)
            messages = writer.prepare_commit()
        assert {message.bucket for message in messages} == {3}
        assert all(file.write_cols == ['pt', 'id'] for message in messages for file in message.new_files)
        builder.new_commit().commit(messages)
    finally:
        writer.close()
    assert _rows(table) == [{'id': 1, 'value': None, 'pt': 'a'},
                            {'id': 2, 'value': None, 'pt': 'a'}]


def test_pinned_dynamic_configuration_survives_early_python_selection(tmp_path):
    table = _table(tmp_path, -1)
    initial = table.new_batch_write_builder()
    first = initial.new_write()
    first.write_arrow_batch(_batch([1], ['old']))
    initial.new_commit().commit(first.prepare_commit())
    first.close()
    pinned_id = table.snapshot_manager().get_latest_snapshot().id
    writer = table.new_batch_write_builder().new_write()
    try:
        assert writer.with_dynamic_bucket_index() is writer
        later = table.new_batch_write_builder()
        second = later.new_write()
        second.write_arrow_batch(_batch([2], ['later']))
        later.new_commit().commit(second.prepare_commit())
        second.close()
        # This existing advanced API still selects Python before any write.
        writer.with_blob_consumer(lambda field, blob: blob)
        assert writer._python_writer.row_key_extractor.base_snapshot_id == pinned_id
    finally:
        writer.close()


@pytest.mark.parametrize('ignore_existing', [False, True])
def test_dynamic_index_configuration_is_locked_after_input(tmp_path, ignore_existing):
    table = _table(tmp_path, -1)
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow_batch_to_bucket(_batch([], []), 0, [], [])
        assert writer.with_dynamic_bucket_index(ignore_existing=ignore_existing) is writer
        writer.write_arrow_batch_to_bucket(_batch([1], ['one']), 0)
        with pytest.raises(RuntimeError, match='before writing'):
            writer.with_dynamic_bucket_index()
    finally:
        writer.close()


def test_native_hash_mapping_conflict_poisoned_writer_preserves_prepared_files(tmp_path):
    table = _table(tmp_path, -1)
    builder = table.new_stream_write_builder()
    writer = builder.new_write()
    try:
        writer.write_arrow_batch_to_bucket(_batch([1], ['published']), 7)
        messages = writer.prepare_commit(1)
        builder.new_commit().commit(messages, 1)
        paths = [file.file_path for message in messages for file in message.new_files]
        writer.write_arrow_batch_to_bucket(_batch([2], ['pending']), 7)
        with pytest.raises(ValueError, match='belongs to bucket 7'):
            writer.write_arrow_batch_to_bucket(_batch([2], ['conflict']), 8)
        with pytest.raises(ValueError, match='cannot be reused'):
            writer.prepare_commit(2)
        assert all(table.file_io.exists(path) for path in paths)
    finally:
        writer.close()
    assert _rows(table) == [{'id': 1, 'value': 'published', 'pt': 'a'}]


@pytest.mark.parametrize('publish_first', [False, True])
def test_native_checkpoint_sequence_advances_with_pinned_index_base(tmp_path, publish_first):
    table = _table(tmp_path, -1)
    builder = table.new_stream_write_builder()
    writer = builder.new_write()
    commit = builder.new_commit()
    try:
        writer.with_dynamic_bucket_index(base_snapshot_id=0)
        writer.write_arrow_batch_to_bucket(_batch([2, 1], ['other', 'old']), 7)
        first = writer.prepare_commit(1)
        if publish_first:
            commit.commit(first, 1)
        writer.write_arrow_batch_to_bucket(_batch([1], ['new']), 7)
        second = writer.prepare_commit(2)
        assert second[0].new_files[0].min_sequence_number == 2
        if not publish_first:
            commit.commit(first, 1)
        commit.commit(second, 2)
    finally:
        writer.close()
        commit.close()
    assert _rows(table) == [{'id': 1, 'value': 'new', 'pt': 'a'},
                            {'id': 2, 'value': 'other', 'pt': 'a'}]


def test_first_native_sequence_uses_current_files_not_index_base(tmp_path):
    table = _table(tmp_path, -1)
    first_builder = table.new_batch_write_builder()
    first = first_builder.new_write()
    first.write_arrow_batch_to_bucket(_batch([1], ['old']), 7)
    first_builder.new_commit().commit(first.prepare_commit())
    first.close()
    builder = table.new_batch_write_builder()
    pinned = builder.new_write()
    try:
        pinned.with_dynamic_bucket_index(base_snapshot_id=1)
        later_builder = table.new_batch_write_builder()
        later = later_builder.new_write()
        later.write_arrow_batch_to_bucket(_batch([1, 1, 1], ['b', 'c', 'd']), 7)
        later_builder.new_commit().commit(later.prepare_commit())
        later.close()
        pinned.write_arrow_batch_to_bucket(_batch([1], ['new']), 7)
        messages = pinned.prepare_commit()
        assert messages[0].new_files[0].min_sequence_number == 4
        builder.new_commit().commit(messages)
    finally:
        pinned.close()
    assert _rows(table) == [{'id': 1, 'value': 'new', 'pt': 'a'}]
