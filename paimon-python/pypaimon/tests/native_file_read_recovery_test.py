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

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema

pytestmark = pytest.mark.native_plan


def _table(tmp_path, mode, native, options):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
    settings = {'file.format': 'parquet', 'write.native.enabled': 'true',
                'read.native.enabled': str(native).lower()}
    if mode == 'pk':
        settings['bucket'] = '1'
    else:
        settings['row-tracking.enabled'] = 'true'
    if mode == 'evolution':
        settings['data-evolution.enabled'] = 'true'
    settings.update(options)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'pk' else [], options=settings), False)
    table = catalog.get_table('default.t')
    files = []
    for start in (0, 10):
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        try:
            writer.write_arrow(pa.table({'id': list(range(start, start + 10)),
                                        'value': ['v%d' % i for i in range(start, start + 10)]}, schema=schema))
            messages = writer.prepare_commit()
            files.extend(file for message in messages for file in message.new_files)
            commit = builder.new_commit()
            try:
                commit.commit(messages)
            finally:
                commit.close()
        finally:
            writer.close()
    return table, files


def _read(table, native, limit=None):
    builder = table.new_read_builder()
    if limit is not None:
        builder.with_limit(limit)
    read = builder.new_read()
    splits = builder.new_scan().plan().splits()
    if native:
        with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
            return read.to_arrow(splits).sort_by('id').to_pydict()
    return read.to_arrow(splits).sort_by('id').to_pydict()


@pytest.mark.parametrize('mode', ['append', 'evolution', 'pk'])
@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('failure,option', [
    ('missing', 'scan.ignore-lost-files'), ('corrupt', 'scan.ignore-corrupt-files')])
def test_read_skips_only_requested_file_failure(tmp_path, mode, native, failure, option):
    table, files = _table(tmp_path, mode, native, {})
    damaged = files[0].file_path
    if failure == 'missing':
        table.file_io.delete(damaged)
    else:
        with table.file_io.new_output_stream(damaged) as output:
            output.write(b'corrupt parquet')
    tolerant = table.copy({option: 'true'})
    assert _read(tolerant, native) == {
        'id': list(range(10, 20)), 'value': ['v%d' % i for i in range(10, 20)]}
    assert _read(tolerant, native, limit=2) == {'id': [10, 11], 'value': ['v10', 'v11']}
    for strict in (table, table.copy({option: 'false'})):
        with pytest.raises((OSError, ValueError, pa.ArrowException)):
            _read(strict, native)
    other = 'scan.ignore-corrupt-files' if failure == 'missing' else 'scan.ignore-lost-files'
    with pytest.raises((OSError, ValueError, pa.ArrowException)):
        _read(table.copy({option: 'false', other: 'true'}), native)
    # A scan policy never deletes corrupt files or healthy outputs.
    assert table.file_io.exists(files[1].file_path)
    assert table.file_io.exists(damaged) == (failure == 'corrupt')


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('corrupt', [False, True])
def test_missing_update_column_stops_group_without_null_filling(tmp_path, native, corrupt):
    table, _ = _table(tmp_path, 'evolution', native, {})
    builder = table.new_batch_write_builder()
    updates = builder.new_update().update_by_arrow_with_row_id(pa.table({
        '_ROW_ID': list(range(10)), 'value': ['changed'] * 10}))
    commit = builder.new_commit()
    try:
        commit.commit(updates)
    finally:
        commit.close()
    file = updates[0].new_files[0]
    if corrupt:
        with table.file_io.new_output_stream(file.file_path) as output:
            output.write(b'corrupt')
    else:
        table.file_io.delete(file.file_path)
    option = 'scan.ignore-corrupt-files' if corrupt else 'scan.ignore-lost-files'
    assert _read(table.copy({option: 'true'}), native) == {
        'id': list(range(10, 20)), 'value': ['v%d' % i for i in range(10, 20)]}
    builder = table.new_read_builder().with_projection(['id'])
    assert builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pydict() == {
        'id': list(range(20))}


@pytest.mark.parametrize('native', [False, True])
def test_invalid_batch_size_is_not_hidden_by_corrupt_files_option(tmp_path, native):
    table, _ = _table(tmp_path, 'append', native, {})
    invalid = table.copy({'read.batch-size': '0', 'scan.ignore-corrupt-files': 'true'})
    with pytest.raises(ValueError, match='read.batch-size'):
        _read(invalid, native)


@pytest.mark.parametrize('stage', ['open', 'read'])
@pytest.mark.parametrize('error_type', [
    pa.ArrowMemoryError, pa.ArrowCapacityError, pa.ArrowNotImplementedError,
    MemoryError, RuntimeError, ValueError])
def test_file_recovery_does_not_hide_non_file_errors(tmp_path, stage, error_type):
    from pypaimon.read.reader.file_read_recovery import FileReadRecovery
    from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
    table, _ = _table(tmp_path, 'append', False, {})
    table = table.copy({'scan.ignore-lost-files': 'true', 'scan.ignore-corrupt-files': 'true'})
    error = error_type('protected failure')

    class Reader(RecordBatchReader):
        def read_arrow_batch(self):
            raise error

        def close(self):
            pass

    recovery = FileReadRecovery(table.file_io, 'unused', table.options)
    if stage == 'open':
        def create():
            raise error
        operation = lambda: recovery.create_reader(create)
    else:
        operation = recovery.create_reader(Reader).read_arrow_batch
    with pytest.raises(error_type, match='protected failure'):
        operation()


def test_late_missing_file_requires_corrupt_policy_and_keeps_emitted_batch(tmp_path):
    from pypaimon.read.reader.file_read_recovery import FileReadRecovery
    from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
    from pypaimon.read.reader.concat_batch_reader import ConcatBatchReader
    table, _ = _table(tmp_path, 'append', False, {})
    first = pa.record_batch([pa.array([1])], names=['id'])
    second = pa.record_batch([pa.array([2])], names=['id'])

    class Reader(RecordBatchReader):
        def __init__(self):
            self.first = True
            self.closed = False

        def read_arrow_batch(self):
            if self.first:
                self.first = False
                return first
            raise FileNotFoundError('deleted after reader creation')

        def close(self):
            self.closed = True

    lost = FileReadRecovery(table.file_io, 'unused', table.copy({'scan.ignore-lost-files': 'true'}).options)
    reader = lost.create_reader(Reader)
    assert reader.read_arrow_batch().to_pydict() == {'id': [1]}
    with pytest.raises(FileNotFoundError, match='after reader creation'):
        reader.read_arrow_batch()
    original = Reader()
    corrupt = FileReadRecovery(table.file_io, 'unused', table.copy({'scan.ignore-corrupt-files': 'true'}).options)
    reader = corrupt.create_reader(lambda: original)
    # Concat continues to the next reader after the damaged one ends.

    class Healthy(RecordBatchReader):
        def __init__(self):
            self.first = True

        def read_arrow_batch(self):
            if self.first:
                self.first = False
                return second
            return None

        def close(self):
            pass
    concat = ConcatBatchReader([lambda: reader, Healthy])
    assert concat.read_arrow_batch().to_pydict() == {'id': [1]}
    assert concat.read_arrow_batch().to_pydict() == {'id': [2]}
    assert concat.read_arrow_batch() is None
    assert original.closed
    concat.close()


def test_corrupt_row_payload_ends_only_current_file(tmp_path):
    from pypaimon.read.reader.file_read_recovery import FileReadRecovery
    from pypaimon.read.reader.format_row_reader import FormatRowReader
    from pypaimon.write.writer.format_row_writer import FormatRowWriter
    from pypaimon.schema.data_types import PyarrowFieldParser
    table, _ = _table(tmp_path, 'append', False, {})
    data = pa.table({'id': pa.array([1, 2], pa.int32())})
    fields = PyarrowFieldParser.to_paimon_schema(data.schema)
    path = str(tmp_path / 'data.parquet.row')
    with table.file_io.new_output_stream(path) as output:
        writer = FormatRowWriter(output, fields)
        writer.write_table(data)
        writer.close()
    with open(path, 'r+b') as output:
        output.write(b'bad!')
    recovery = FileReadRecovery(table.file_io, path, table.copy({'scan.ignore-corrupt-files': 'true'}).options)
    reader = recovery.create_reader(lambda: FormatRowReader(table.file_io, path, ['id'], fields, None))
    assert reader.read_arrow_batch() is None
    reader.close()
    assert table.file_io.exists(path)
