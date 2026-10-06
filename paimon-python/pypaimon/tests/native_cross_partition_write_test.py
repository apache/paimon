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

"""Native cross-partition writes use the Java global-key routing contract."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.native_write import NativeTableWrite

pytestmark = pytest.mark.native_plan
_SCHEMA = pa.schema([('tenant', pa.string()), ('p', pa.string()),
                     ('id', pa.int32()), ('v', pa.int32())])


def _table(tmp_path, options=None, catalog=None, rowkind=False):
    catalog = catalog or CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    opts = {'bucket': '-1', 'file.format': 'parquet',
            'dynamic-bucket.target-row-num': '2', 'write.native.enabled': 'true'}
    opts.update(options or {})
    schema = _SCHEMA.append(pa.field('op', pa.string())) if rowkind else _SCHEMA
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['tenant', 'id'], partition_keys=['tenant', 'p'], options=opts), False)
    return catalog.get_table('default.t')


def _batch(rows, rowkind=False):
    schema = _SCHEMA.append(pa.field('op', pa.string())) if rowkind else _SCHEMA
    return pa.RecordBatch.from_pylist([dict(zip(schema.names, row)) for row in rows], schema=schema)


def _rows(table, native):
    builder = table.copy({'scan.native-plan.enabled': str(native).lower(),
                          'read.native.enabled': str(native).lower()}).new_read_builder()
    # Java first-row scans hide un-compacted L0 files; a write scan includes them.
    splits = builder.new_scan().plan_for_write().splits()
    reader = builder.new_read()
    if native:
        with patch.object(reader, '_create_split_read', side_effect=AssertionError('fallback')):
            rows = reader.to_arrow(splits).to_pylist()
    else:
        rows = reader.to_arrow(splits).to_pylist()
    return sorted(tuple(row[name] for name in _SCHEMA.names) for row in rows)


def _commit(commit, messages, native_commit=False, checkpoint=None):
    def publish():
        if checkpoint is None:
            commit.commit(messages)
        else:
            commit.commit(messages, checkpoint)
    if native_commit:
        with patch.object(commit.file_store_commit, 'commit',
                          side_effect=AssertionError('Python commit fallback')):
            publish()
    else:
        publish()


@pytest.mark.parametrize('native_commit', [False, True])
@pytest.mark.parametrize('engine,expected', [
    ('deduplicate', [('x', 'b', 1, 20), ('y', 'b', 1, 200)]),
    ('first-row', [('x', 'a', 1, 10), ('y', 'a', 1, 100)]),
    ('partial-update', [('x', 'a', 1, 20), ('y', 'a', 1, 200)]),
    ('aggregation', [('x', 'a', 1, 30), ('y', 'a', 1, 300)]),
])
def test_cross_partition_engines_rebuild_full_primary_keys(
        tmp_path, native_rest_catalog, native_commit, engine, expected):
    opts = {'merge-engine': engine, 'commit.native.enabled': str(native_commit).lower()}
    if engine == 'aggregation':
        opts['fields.v.aggregate-function'] = 'sum'
    table = _table(tmp_path, opts, native_rest_catalog)
    for data in ([('x', 'a', 1, 10), ('y', 'a', 1, 100)],
                 [('x', 'b', 1, 20), ('y', 'b', 1, 200)]):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        assert isinstance(writer, NativeTableWrite)
        try:
            writer.write_arrow_batch(_batch(data))
            assert writer._python_writer is None
            messages = writer.prepare_commit()
            # Global index is rebuilt from data, not the HASH index of local dynamic buckets.
            assert all(not m.index_adds and not m.index_deletes for m in messages)
            _commit(commit, messages, native_commit)
        finally:
            writer.close()
            commit.close()
    for native in (False, True):
        assert _rows(table, native) == expected


@pytest.mark.parametrize('chunk_size', [1, 2, 3, 4])
@pytest.mark.parametrize('stream', [False, True])
def test_cross_partition_migration_event_order(tmp_path, chunk_size, stream):
    table = _table(tmp_path)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    data = [('x', 'a/b', 1, 10), ('x', 'c=d', 1, 20),
            ('x', 'a/b', 1, 30), ('x', 'a/b', 1, 40)]
    try:
        for start in range(0, len(data), chunk_size):
            writer.write_arrow_batch(_batch(data[start:start + chunk_size]))
        _commit(commit, writer.prepare_commit(1) if stream else writer.prepare_commit(),
                checkpoint=1 if stream else None)
        if stream:
            # The same assigner must retain locations after preparing a checkpoint.
            writer.write_arrow_batch(_batch([('x', 'c=d', 1, 50)]))
            _commit(commit, writer.prepare_commit(2), checkpoint=2)
            assert writer.prepare_commit(3) == []
    finally:
        writer.close()
        commit.close()
    expected = [('x', 'c=d', 1, 50)] if stream else [data[-1]]
    for native in (False, True):
        assert _rows(table, native) == expected


def test_cross_partition_rowkind_and_row_input(tmp_path):
    table = _table(tmp_path, {'rowkind.field': 'op'}, rowkind=True)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_row(GenericRow(['x', 'a', 1, 10, '+I'], table.fields))
        writer.write_arrow_batch(_batch([
            ('x', 'b', 1, 20, '+U'), ('x', 'a', 1, 30, '+U'),
            ('x', 'a', 2, 40, '+I'), ('x', 'b', 2, 50, '-D')], True))
        assert writer._python_writer is None
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    for native in (False, True):
        assert _rows(table, native) == [('x', 'a', 1, 30)]


@pytest.mark.parametrize('committed', [False, True])
def test_cross_partition_abort_preserves_prepared_and_committed_files(tmp_path, committed):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow_batch(_batch([('x', 'a', 1, 10), ('x', 'b', 1, 20)]))
        messages = writer.prepare_commit()
        paths = [f.file_path for m in messages for f in m.new_files]
        assert paths and all(table.file_io.exists(p) for p in paths)
        if committed:
            commit.commit(messages)
        writer.abort()
        assert all(table.file_io.exists(p) for p in paths)
    finally:
        writer.close()
        commit.close()
    assert _rows(table, True) == ([('x', 'b', 1, 20)] if committed else [])


def test_cross_partition_dynamic_overwrite_keeps_migration_deletes(tmp_path):
    table = _table(tmp_path, {'dynamic-partition-overwrite': 'true'})
    for data, overwrite in [([('x', 'a', 1, 10), ('y', 'c', 2, 20)], False),
                            ([('x', 'b', 1, 30)], True)]:
        builder = table.new_batch_write_builder()
        if overwrite:
            builder.overwrite()
        writer, commit = builder.new_write(), builder.new_commit()
        assert isinstance(writer, NativeTableWrite)
        try:
            writer.write_arrow_batch(_batch(data))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    for native in (False, True):
        assert _rows(table, native) == [('x', 'b', 1, 30), ('y', 'c', 2, 20)]
