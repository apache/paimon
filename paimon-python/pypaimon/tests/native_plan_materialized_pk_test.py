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
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.deletionvectors.bitmap_deletion_vector import BitmapDeletionVector
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_delete import TableDeleteByRowId


pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust planner required')]


@pytest.mark.parametrize('engine', ['deduplicate', 'first-row'])
@pytest.mark.parametrize('merge_on_read', ['false', 'true'])
@pytest.mark.parametrize('target_size', ['1b', '1mb'])
def test_clustered_materialized_dv_files_use_native_raw_splits(tmp_path, engine, merge_on_read, target_size):
    schema = pa.schema([('id', pa.int64()), ('value', pa.string())])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'], options={
            'bucket': '1', 'merge-engine': engine, 'file.format': 'parquet',
            'deletion-vectors.enabled': 'true', 'deletion-vectors.merge-on-read': merge_on_read,
            'pk-clustering-override': 'true', 'clustering.columns': 'value',
            'source.split.target-size': target_size, 'source.split.open-file-cost': '1b',
        }), False)
    table = catalog.get_table('default.t')
    expected = [{'id': i, 'value': 'v%d' % (10 - i)} for i in range(1, 5)]
    files = []
    for level, keys in ((1, (1, 3)), (2, (2, 4))):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist([expected[key - 1] for key in keys], schema=schema))
            messages = writer.prepare_commit()
            for message in messages:
                rewritten = []
                for file in message.new_files:
                    path = table.path_factory().bucket_path((), 0) + '/' + file.file_name
                    # Simulate Java clustering compaction: physical order is by
                    # value, opposite to PK order, while min/max PK ranges overlap.
                    data = pq.read_table(path).sort_by([('value', 'ascending')])
                    pq.write_table(data, path)
                    rewritten.append(replace(file, level=level,
                                             file_size=Path(path).stat().st_size))
                message.new_files = rewritten
                files.extend(rewritten)
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()

    vector = BitmapDeletionVector()
    vector.delete(0)  # clustered first file starts with id=3
    entry = TableDeleteByRowId(table)._write_deletion_vector_index(
        GenericRow([], []), 0, {files[0].file_name: vector})
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.commit([CommitMessage(partition=(), bucket=0, new_files=[], index_adds=[entry])])
    finally:
        commit.close()
    expected = [row for row in expected if row['id'] != 3]
    pb = table.new_read_builder().new_predicate_builder()
    for predicate in (None, pb.equal('id', 3), pb.equal('value', 'v8')):
        for native in (False, True):
            builder = table.copy({'scan.native-plan.enabled': str(native).lower()}).new_read_builder()
            if predicate is not None:
                builder.with_filter(predicate)
            scan = builder.new_scan()
            if native:
                with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('native fallback')):
                    plan = scan.plan()
            else:
                plan = scan.plan()
            assert all(split.raw_convertible for split in plan.splits())
            rows = builder.new_read().to_arrow(plan.splits()).to_pylist()
            wanted = expected if predicate is None else ([] if predicate.field == 'id' else [expected[1]])
            assert sorted(rows, key=lambda row: row['id']) == wanted
            assert plan.snapshot_id == 3
            if predicate is None:
                assert len(plan.splits()) == (2 if target_size == '1b' else 1)

    if engine == 'first-row' and merge_on_read == 'true':
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist([{'id': 5, 'value': 'pending'}], schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
        scan = table.copy({'scan.native-plan.enabled': 'true'}).new_read_builder().new_scan()
        with patch.object(scan.file_scanner, 'scan', wraps=scan.file_scanner.scan) as fallback:
            plan = scan.plan()
        fallback.assert_called_once_with()
        assert any(file.level == 0 for split in plan.splits() for file in split.files)
