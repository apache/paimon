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

from unittest.mock import patch
from urllib.parse import urlparse

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.blob import Blob, VideoFrameDescriptor
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.video_keyframe_index import VideoKeyframeIndex

pytestmark = pytest.mark.native_plan


def _table(tmp_path, multiple, external=False, catalog=None):
    if catalog is None:
        catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    names = ['left', 'right'] if multiple else ['left']
    fields = [DataField(0, 'id', AtomicType('INT'))]
    fields.extend(DataField(index + 1, name, AtomicType('BLOB')) for index, name in enumerate(names))
    options = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
               'video-frame-field': ','.join(names), 'write.native.enabled': 'false'}
    if external:
        options['data-file.external-paths'] = (tmp_path / 'external').as_uri()
    catalog.create_table('db.t', Schema(fields=fields, options=options), False)
    table = catalog.get_table('db.t')
    mapping = VideoKeyframeIndex([(0, 1)], [(0, 0, 0)]).serialize()
    sources = []
    for ordinal in range(4 if multiple else 2):
        source = tmp_path / ('source-%d.mp4' % ordinal)
        source.write_bytes(bytes([ordinal + 1]) * 10 + mapping)
        sources.append(source.as_uri())
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        for row, (ordinal, frame) in enumerate([(0, 2), (0, 3), (None, None), (1, 7), (1, 8), (0, 10)]):
            values = [row]
            for index in range(len(names)):
                if ordinal is None:
                    values.append(None)
                else:
                    uri = sources[ordinal + index * 2]
                    descriptor = VideoFrameDescriptor(uri, 0, 10, frame, 10, len(mapping))
                    values.append(Blob.from_descriptor(table.file_io.uri_reader_factory.create(uri), descriptor))
            writer.write_row(GenericRow(values, table.fields))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table


def _read(table, native, projection=None, filtered=False, limit=None):
    table = table.copy({'read.native.enabled': str(native).lower(),
                        'scan.native-plan.enabled': str(native).lower()})
    builder = table.new_read_builder()
    if projection is not None:
        builder.with_projection(projection)
    if filtered:
        builder.with_filter(table.new_read_builder().new_predicate_builder().greater_than('id', 0))
    if limit is not None:
        builder.with_limit(limit)
    read = builder.new_read()
    splits = builder.new_scan().plan().splits()
    if native:
        with patch.object(read, '_create_split_read', side_effect=AssertionError('Python video read fallback')):
            return read.to_arrow(splits), read.to_arrow_batch_reader(splits).read_all()
    return read.to_arrow(splits), read.to_arrow_batch_reader(splits).read_all()


@pytest.mark.parametrize('multiple', [False, True])
@pytest.mark.parametrize('external', [False, True])
@pytest.mark.parametrize('descriptor_mode', [False, True])
def test_native_video_reads_match_java_lazy_frame_and_keyframe_descriptors(
        tmp_path, multiple, external, descriptor_mode):
    table = _table(tmp_path, multiple, external).copy({'blob-as-descriptor': str(descriptor_mode).lower()})
    expected = sorted(_read(table, False)[0].to_pylist(), key=lambda row: row['id'])
    for result in _read(table, True):
        rows = sorted(result.to_pylist(), key=lambda row: row['id'])
        assert rows == expected and len(rows) == 6
        for name in ('left', 'right') if multiple else ('left',):
            assert rows[2][name] is None
            frames = [VideoFrameDescriptor.deserialize(row[name]) for row in rows if row[name] is not None]
            assert [frame.frame_index for frame in frames] == [2, 3, 7, 8, 10]
            assert frames[0].payload_descriptor == frames[1].payload_descriptor == frames[4].payload_descriptor
            assert frames[2].payload_descriptor == frames[3].payload_descriptor
            for frame in frames:
                index = frame.keyframe_index_descriptor
                assert index is not None
                with open(urlparse(index.uri).path, 'rb') as source:
                    source.seek(index.offset)
                    assert VideoKeyframeIndex.deserialize(source.read(index.length)).keyframes == ((0, 0, 0),)


@pytest.mark.parametrize('projection', [[], ['id'], {'frame': 'left'}, ['left', 'left', 'id']])
@pytest.mark.parametrize('limit', [None, 3])
def test_native_video_projection_filter_and_limit_preserve_logical_rows(tmp_path, projection, limit):
    table = _table(tmp_path, True)
    expected = _read(table, False, projection, True, limit)[0]
    for result in _read(table, True, projection, True, limit):
        assert result.schema == expected.schema
        assert result.to_pylist() == expected.to_pylist()
        assert result.num_rows == expected.num_rows


def test_native_update_of_normal_columns_keeps_packed_video_frames(tmp_path, native_rest_catalog):
    table = _table(tmp_path, False, catalog=native_rest_catalog).copy(
        {'write.native.enabled': 'true', 'commit.native.enabled': 'true'})
    before = _read(table, True)[0].to_pylist()
    builder = table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['id'])
    with patch('pypaimon.write.table_update.BatchTableUpdate._update_by_arrow_batches_with_row_id',
               side_effect=AssertionError('Python update fallback')):
        messages = update.update_by_arrow_batches_with_row_id(iter([
            pa.table({'_ROW_ID': [0], 'id': [20]}, schema=pa.schema([('_ROW_ID', pa.int64()), ('id', pa.int32())]))]))
    commit = builder.new_commit()
    try:
        commit.commit(messages)
        assert commit._native_commit is not None
    finally:
        commit.close()
    after = _read(table, True)[0].to_pylist()
    assert sorted(row['id'] for row in after) == [1, 2, 3, 4, 5, 20]
    assert [row['left'] for row in after] == [row['left'] for row in before]


def test_native_video_placeholder_falls_back_but_null_stops_older_frames(tmp_path):
    from dataclasses import replace
    from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor
    from pypaimon.read.split import DataSplit
    import struct

    table = _table(tmp_path, False).copy({'read.native.enabled': 'true'})
    builder = table.new_read_builder()
    split = builder.new_scan().plan().splits()[0]
    read = builder.new_read()
    before = read.to_arrow([split]).to_pylist()
    original = next(file for file in split.files if file.file_name.endswith('.video'))
    indexes = [[], [], [1, 1, 1, 3], [-2, -1, -2, -2], [0, 0, 0, 0]]
    compressed = [DeltaVarintCompressor.compress(values) for values in indexes]
    data = b''.join(compressed) + struct.pack('<IIIIIIB', *(len(index) for index in compressed), 0x4F454449, 1)
    newer = tmp_path / 'newest.video'
    newer.write_bytes(data)
    added = replace(original, file_name=newer.name, file_path=str(newer), external_path=str(newer),
                    file_size=len(data), min_sequence_number=original.max_sequence_number + 1,
                    max_sequence_number=original.max_sequence_number + 1)
    combined = DataSplit(split.files + [added], split.partition, split.bucket,
                         snapshot_id=split.snapshot_id, bucket_path=split.bucket_path,
                         total_buckets=split.total_buckets)
    expected = [dict(row) for row in before]
    expected[1]['left'] = None
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        for result in (read.to_arrow([combined]), read.to_arrow_batch_reader([combined]).read_all()):
            assert result.to_pylist() == expected


def test_native_video_deletion_vectors_filter_both_arrow_readers(tmp_path, native_rest_catalog):
    from pypaimon.write.table_update import TableDeleteByRowId

    table = _table(tmp_path, True, catalog=native_rest_catalog).copy(
        {'deletion-vectors.enabled': 'true', 'write.native.enabled': 'true', 'commit.native.enabled': 'true'})
    builder = table.new_batch_write_builder()
    with patch.object(TableDeleteByRowId, 'delete',
                      side_effect=AssertionError('Python delete fallback')):
        messages = builder.new_update().delete_by_row_id([0, 2])
    commit = builder.new_commit()
    try:
        commit.commit(messages)
        assert commit._native_commit is not None
    finally:
        commit.close()
    for result in _read(table, True):
        rows = sorted(result.to_pylist(), key=lambda row: row['id'])
        assert [row['id'] for row in rows] == [1, 3, 4, 5]
        assert all(VideoFrameDescriptor.deserialize(row[name]).frame_index in (3, 7, 8, 10)
                   for row in rows for name in ('left', 'right'))
