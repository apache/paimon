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

"""Deletion metadata must describe physical files, not incoming requests."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import orc

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.read.split import DataSplit

pytestmark = [pytest.mark.python_write, pytest.mark.python_plan,
              pytest.mark.python_read, pytest.mark.python_commit]


def make_table(tmp_path, fmt, options=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    settings = {'bucket': '1', 'file.format': fmt, 'rowkind.field': 'op',
                'changelog-producer': 'input', 'changelog-file.format': fmt,
                'write.native.enabled': 'false', 'read.native.enabled': 'false',
                'scan.native-plan.enabled': 'false', 'commit.native.enabled': 'false'}
    settings.update(options or {})
    schema = pa.schema([pa.field('id', pa.int32(), nullable=False),
                        pa.field('v', pa.int32()), pa.field('op', pa.string())])
    catalog.create_table('default.events', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'], options=settings), False)
    return catalog.get_table('default.events'), schema


def commit(table, schema, rows):
    builder = table.new_batch_write_builder()
    writer, committer = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        messages = writer.prepare_commit()
        committer.commit(messages)
        return messages
    finally:
        writer.close()
        committer.close()


def check_metadata(table, messages):
    snapshot = table.snapshot_manager().get_latest_snapshot()
    lists = ManifestListManager(table)
    manifests = ManifestFileManager(table)
    for attr, stored in [('new_files', lists.read_delta(snapshot)),
                         ('changelog_files', lists.read_changelog(snapshot))]:
        prepared = {f.file_name: f for m in messages for f in getattr(m, attr)}
        restored = {entry.file.file_name: entry.file for manifest in stored
                    for entry in manifests.read(manifest.file_name)}
        assert restored.keys() == prepared.keys()
        for name, meta in prepared.items():
            with table.file_io.new_input_stream(meta.file_path) as source:
                physical = (pq.ParquetFile(source).read() if name.endswith('.parquet')
                            else orc.ORCFile(source).read())
            deletes = sum(kind in (1, 3) for kind in physical['_VALUE_KIND'].to_pylist())
            assert meta.row_count == restored[name].row_count == physical.num_rows
            assert meta.delete_row_count == restored[name].delete_row_count == deletes


def read_rows(table):
    builder = table.new_read_builder()
    splits = builder.new_scan().plan().splits()
    return builder.new_read().to_arrow(splits).to_pylist(), splits


@pytest.mark.parametrize('fmt', ['parquet', 'orc'])
@pytest.mark.parametrize('op,deletes', [('+I', 0), ('+U', 0), ('-U', 1), ('-D', 1)])
def test_single_file_retractions_are_not_visible(tmp_path, fmt, op, deletes):
    table, schema = make_table(tmp_path, fmt)
    rows = [{'id': 1, 'v': 10, 'op': op}]
    messages = commit(table, schema, rows)
    table = CatalogFactory.create({'warehouse': str(tmp_path)}).get_table('default.events')
    actual, splits = read_rows(table)
    assert actual == ([] if deletes else rows)
    check_metadata(table, messages)
    assert [f.delete_row_count for m in messages for f in m.new_files] == [deletes]
    assert len(splits) == 1
    assert isinstance(splits[0], DataSplit)
    assert splits[0].raw_convertible == (deletes == 0)


@pytest.mark.parametrize('fmt', ['parquet', 'orc'])
def test_counts_follow_folded_and_rolled_files(tmp_path, fmt):
    table, schema = make_table(tmp_path, fmt, {'target-file-size': '40 b'})
    # Three retract requests become two physical retracts after the id=1 update.
    rows = [{'id': key, 'v': value, 'op': op} for key, value, op in [
        (1, 10, '-D'), (1, 20, '+U'), (2, 30, '+I'),
        (2, 30, '-U'), (3, 40, '-D'), (4, 50, '+I')]]
    messages = commit(table, schema, rows)
    table = CatalogFactory.create({'warehouse': str(tmp_path)}).get_table('default.events')
    actual, _ = read_rows(table)
    assert sorted(actual, key=lambda row: row['id']) == [rows[1], rows[-1]]
    # A point lookup can prune the other rolled files and expose a retract-only file.
    reader = table.new_read_builder()
    reader.with_filter(reader.new_predicate_builder().equal('id', 3))
    splits = reader.new_scan().plan().splits()
    result = reader.new_read().to_arrow(splits)
    assert result is not None
    assert result.num_rows == 0
    files = [f for m in messages for f in m.new_files]
    assert len(files) > 1
    assert sum(f.row_count for f in files) == 4
    delete_counts = []
    for file in files:
        assert file.delete_row_count is not None
        delete_counts.append(file.delete_row_count)
    assert sum(delete_counts) == 2
    assert any(f.delete_row_count == 0 for f in files)
    check_metadata(table, messages)


@pytest.mark.parametrize('fmt', ['parquet', 'orc'])
def test_delete_survives_reopening_table(tmp_path, fmt):
    table, schema = make_table(tmp_path, fmt)
    rows = [{'id': 1, 'v': 10, 'op': '+I'}, {'id': 2, 'v': 20, 'op': '+I'}]
    added = commit(table, schema, rows)
    check_metadata(table, added)
    assert sorted(read_rows(table)[0], key=lambda row: row['id']) == rows
    deleted = commit(table, schema, [{'id': 1, 'v': 10, 'op': '-D'}])

    # Open a fresh catalog/table: the delete must survive through persisted
    # snapshots and manifests, without affecting the other primary key.
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    table = catalog.get_table('default.events')
    actual, splits = read_rows(table)
    assert actual == [rows[1]]
    check_metadata(table, deleted)
    # The earlier add file remains in the latest snapshot; its physical count
    # stays zero even though the row no longer exists in the logical table.
    counts = {f.file_name: f.delete_row_count for split in splits for f in split.files}
    assert counts == {added[0].new_files[0].file_name: 0,
                      deleted[0].new_files[0].file_name: 1}
