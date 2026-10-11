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

from dataclasses import replace
from uuid import uuid4

import pyarrow as pa
import pytest
import pypaimon.multimodal as pm

from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.snapshot.renaming_snapshot_commit import RenamingSnapshotCommit
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId


def _publish_reassigned_partition(table, partition_value):
    """Publish the metadata-only OVERWRITE shape emitted by Java reassign.

    The real Java reassign algorithm is exercised by DataEvolutionRowIdReassignerTest.
    Preserve shared row ranges across normal/image column files in this fixture.
    """
    latest = table.snapshot_manager().get_latest_snapshot()
    manifest_file = ManifestFileManager(table)
    manifest_list = ManifestListManager(table)
    entries = manifest_file.read_entries_parallel(manifest_list.read_all(latest), drop_stats=False)
    next_row_id = latest.next_row_id
    mapping = {}
    reassigned = []
    for entry in entries:
        if entry.partition.values[0] == partition_value:
            key = (entry.file.first_row_id, entry.file.row_count)
            if key not in mapping:
                mapping[key] = next_row_id
                next_row_id += entry.file.row_count
            entry = entry.assign_first_row_id(mapping[key])
        reassigned.append(entry)
    token = str(uuid4())
    manifests = manifest_file.rolling_write(reassigned, 1 << 30, 'manifest-' + token)
    base, delta = 'manifest-list-' + token + '-0', 'manifest-list-' + token + '-1'
    manifest_list.write(base, manifests)
    manifest_list.write(delta, [])
    snapshot = replace(latest, id=latest.id + 1, base_manifest_list=base,
                       delta_manifest_list=delta, delta_record_count=0,
                       commit_user='video-reassign-test', commit_identifier=0,
                       commit_kind='OVERWRITE', time_millis=latest.time_millis + 1,
                       next_row_id=next_row_id, uuid=str(uuid4()))
    assert RenamingSnapshotCommit(table.snapshot_manager()).commit(latest.uuid, snapshot, [])


@pytest.mark.parametrize('phase', ['after_mapping', 'commit_cas'])
@pytest.mark.parametrize('concurrent_change', ['same_partition', 'other_partition', 'append'])
def test_video_backfill_reassign_interleavings(tmp_path, monkeypatch, phase, concurrent_change):
    conn = pm.connect(options={'warehouse': str(tmp_path / 'warehouse')})
    table = conn.create_table('images', schema=pa.schema([
        ('pt', pa.string()), ('id', pa.int32()), ('image', pa.large_binary())]),
        partitioned=['pt'], options={'file.format': 'parquet'})
    for group in range(4):
        table.add([{'pt': 'a' if group % 2 == 0 else 'b', 'id': i, 'image': bytes([i])}
                   for i in range(group * 2, group * 2 + 2)])
    conn.catalog.alter_table('default.images', [
        SchemaChange.add_column('video', AtomicType('BLOB')),
        SchemaChange.set_option('video-frame-field', 'video')])
    table = conn.get_table('images')
    raw = table.raw_table
    source = tmp_path / 'source.mp4'
    source.write_bytes(b'complete-video-payload')
    video = pm.Blob.from_local(str(source))
    injected = []

    def concurrent_commit():
        injected.append(True)
        if concurrent_change == 'append':
            table.add([{'pt': 'b', 'id': 8, 'image': bytes([8]), 'video': None}])
        else:
            _publish_reassigned_partition(raw, 'a' if concurrent_change == 'same_partition' else 'b')
        injected.append(raw.snapshot_manager().get_latest_snapshot().id)

    real_init = TableUpdateByRowId.__init__
    real_commit = RenamingSnapshotCommit.commit

    def init_after_reassign(updater, *args, **kwargs):
        if not injected:
            concurrent_commit()
        real_init(updater, *args, **kwargs)

    def commit_after_reassign(committer, base_uuid, snapshot, statistics):
        if not injected and snapshot.commit_kind == 'APPEND':
            concurrent_commit()
            # Let the real compare-and-swap lose to the concurrent snapshot,
            # then exercise the updater's retry validation.
        return real_commit(committer, base_uuid, snapshot, statistics)

    with monkeypatch.context() as patch:
        if phase == 'after_mapping':
            patch.setattr(TableUpdateByRowId, '__init__', init_after_reassign)
        else:
            patch.setattr(RenamingSnapshotCommit, 'commit', commit_after_reassign)
        if concurrent_change == 'same_partition':
            with pytest.raises((ValueError, RuntimeError), match='valid range|Row ID existence conflict'):
                table.replace_video('id < 2', video, first_frame=7)
        else:
            table.replace_video('id < 2', video, first_frame=7)
    assert injected

    if concurrent_change == 'same_partition':
        assert raw.snapshot_manager().get_latest_snapshot().id == injected[1]
        assert all(row['video'] is None for row in table.scan().to_list())
        # A fresh read rebuilds row-ID -> frame mapping; never reuse stale commit messages.
        table.replace_video('id < 2', video, first_frame=7)

    rows = sorted(table.scan().select(['id', 'video']).to_list(), key=lambda row: row['id'])
    assert [r['id'] for r in rows] == list(range(9 if concurrent_change == 'append' else 8))
    for row in rows:
        if row['id'] < 2:
            descriptor = pm.VideoFrameDescriptor.deserialize(row['video'])
            assert descriptor.frame_index == row['id'] + 7
            assert descriptor.uri.endswith('.video')
        else:
            assert row['video'] is None
    scalar, images = table.scan().read_blobs('image')
    assert dict(zip(scalar['id'].to_pylist(), images['image'])) == {
        row['id']: bytes([row['id']]) for row in rows}
    _, videos = table.scan().where('id < 2').read_blobs('video')
    assert videos['video'] == [b'complete-video-payload'] * 2
