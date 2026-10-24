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

"""Initial streaming levels follow Java DataTableStreamScan and Rust Core."""

from dataclasses import replace

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.python.read.streaming_table_scan import AsyncStreamingTableScan
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager


def _initial_level_table(tmp_path, levels):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'], options={
            'bucket': '1', 'write.native.enabled': 'false',
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
        }), False)
    table = catalog.get_table('db.t')
    for level in levels:
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist([
                {'id': level, 'value': level}], schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    snapshot = table.snapshot_manager().get_latest_snapshot()
    lists = ManifestListManager(table)
    files = ManifestFileManager(table)
    entries = files.read_entries_parallel(lists.read_all(snapshot), drop_stats=False)
    for entry in entries:
        entry.file.level = entry.file.min_key.values[0]
    manifests = files.rolling_write(entries, 1 << 30, 'stream-initial-levels')
    base_size = lists.write('stream-initial-levels-base', manifests)
    delta_size = lists.write('stream-initial-levels-delta', [])
    return table, replace(
        snapshot, base_manifest_list='stream-initial-levels-base',
        base_manifest_list_size=base_size,
        delta_manifest_list='stream-initial-levels-delta',
        delta_manifest_list_size=delta_size)


@pytest.mark.parametrize('producer,options,expected', [
    ('none', {}, [0, 1, 2, 5, 2147483646]),
    ('input', {}, [0, 1, 2, 5, 2147483646]),
    ('lookup', {}, [1, 2, 5, 2147483646]),
    ('full-compaction', {}, [5]),
    ('full-compaction', {'num-levels': '3'}, [2]),
    ('full-compaction', {'num-sorted-run.compaction-trigger': '1'}, [1]),
    ('full-compaction', {'num-levels': '3', 'num-sorted-run.compaction-trigger': '99'}, [2]),
    ('full-compaction', {'num-sorted-run.compaction-trigger': '2147483647'}, [2147483646]),
])
def test_initial_frame_selects_only_published_changelog_levels(tmp_path, producer, options, expected):
    table, snapshot = _initial_level_table(tmp_path, [0, 1, 2, 5, 2147483646])
    table = table.copy(dict(options, **{'changelog-producer': producer}))
    builder = table.new_stream_read_builder()
    scan = AsyncStreamingTableScan(table)
    try:
        plan = scan._create_initial_plan(snapshot)
        assert plan.snapshot_id == snapshot.id
        assert sorted(file.level for split in plan.splits() for file in split.files) == expected
        assert sorted(row['id'] for row in builder.new_read().to_arrow(plan.splits()).to_pylist()) == expected
        assert all(not split.is_streaming for split in plan.splits())
    finally:
        scan.close()


@pytest.mark.parametrize('producer', ['lookup', 'full-compaction'])
def test_initial_frame_with_only_level_zero_is_empty(tmp_path, producer):
    table, snapshot = _initial_level_table(tmp_path, [0])
    table = table.copy({'changelog-producer': producer})
    builder = table.new_stream_read_builder()
    scan = AsyncStreamingTableScan(table)
    try:
        plan = scan._create_initial_plan(snapshot)
        assert plan.snapshot_id == snapshot.id
        assert plan.splits() == []
        assert builder.new_read().to_arrow(plan.splits()).num_rows == 0
    finally:
        scan.close()
