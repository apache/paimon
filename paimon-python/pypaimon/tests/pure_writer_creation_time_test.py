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

"""Prepared data and dedicated files use Java DataFileMeta's TIMESTAMP(3)."""

from datetime import datetime, timedelta
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.write.write_dispatch import NativeTableWrite


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]


@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('mode', ['append', 'pk-input', 'postpone', 'blob', 'vector'])
def test_prepared_file_creation_times_have_millisecond_precision(tmp_path, stream, mode):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
    rows = [dict(id=1, value=10), dict(id=2, value=20)]
    options = {'file.format': 'parquet', 'write.native.enabled': 'false',
               'commit.native.enabled': 'false', 'read.native.enabled': 'false',
               'scan.native-plan.enabled': 'false'}
    if mode in ('pk-input', 'postpone'):
        options.update({'bucket': '-2' if mode == 'postpone' else '1',
                        'changelog-producer': 'input'})
    if mode in ('blob', 'vector'):
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
        if mode == 'blob':
            schema = schema.append(pa.field('payload', pa.large_binary()))
            for row in rows:
                row['payload'] = ('blob-%d' % row['id']).encode()
        else:
            options['vector.file.format'] = 'parquet'
            schema = schema.append(pa.field('embedding', pa.list_(pa.float32(), 2)))
            for row in rows:
                row['embedding'] = [float(row['id']), 2.0]
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode in ('pk-input', 'postpone') else [], options=options), False)
    table = catalog.get_table('default.t')
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert not isinstance(writer, NativeTableWrite)
    now = Timestamp.from_local_date_time(datetime(2024, 6, 1, 12, 30, 40, 123456))
    expected = now.to_millis_timestamp()
    try:
        with patch.object(Timestamp, 'now', return_value=now):
            writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
            messages = writer.prepare_commit(1) if stream else writer.prepare_commit()
        files = [file for message in messages for file in message.new_files + message.changelog_files]
        assert files
        assert all(file.creation_time.get_nano_of_millisecond() == 0 for file in files)
        assert all(file.creation_time == expected for file in files)
        assert now.get_nano_of_millisecond() == 456000
        if mode == 'blob':
            assert any(file.file_name.endswith('.blob') for file in files)
            assert any(file.file_name.endswith('.parquet') for file in files)
        if mode == 'vector':
            assert any('.vector.parquet' in file.file_name for file in files)
        if mode == 'pk-input':
            assert any(message.changelog_files for message in messages)
        if mode == 'postpone':
            assert all(message.bucket == -2 and not message.changelog_files for message in messages)
            # Pending events are invisible to normal scans until assigned real
            # buckets; verify their physical Parquet rows before publication.
            with table.file_io.new_input_stream(files[0].file_path) as stream_:
                physical = pq.ParquetFile(stream_).read().select(schema.names).to_pylist()
            assert physical == rows
        commit.commit(messages, 1) if stream else commit.commit(messages)
    finally:
        writer.close()
        commit.close()

    snapshot = table.snapshot_manager().get_latest_snapshot()
    entries = ManifestFileManager(table).read_entries_parallel(ManifestListManager(table).read_all(snapshot))
    assert entries and all(entry.file.creation_time == expected for entry in entries)
    read = table.new_read_builder()
    actual = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id').to_pylist()
    assert actual == ([] if mode == 'postpone' else rows)


@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
def test_input_changelog_creation_time_is_taken_after_its_write(tmp_path, stream):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'], options={
            'bucket': '1', 'changelog-producer': 'input',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'}), False)
    table = catalog.get_table('default.t')
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    clock = [datetime(2024, 6, 1, 12, 30, 40, 123456)]
    completed = {}
    write_parquet = table.file_io.write_parquet

    def write_and_advance(path, *args, **kwargs):
        result = write_parquet(path, *args, **kwargs)
        # Advance a deterministic clock only after a real file was written.
        # Each file's result must use its own completion time, including the
        # changelog whose statistics were collected before the I/O.
        clock[0] += timedelta(seconds=1)
        completed[path] = Timestamp.from_local_date_time(clock[0]).to_millis_timestamp()
        return result

    try:
        with patch.object(table.file_io, 'write_parquet', side_effect=write_and_advance), \
                patch.object(Timestamp, 'now', side_effect=lambda: Timestamp.from_local_date_time(clock[0])):
            writer.write_arrow(pa.Table.from_pylist([
                {'id': 1, 'value': 10}, {'id': 1, 'value': 20}], schema=schema))
            messages = writer.prepare_commit(1) if stream else writer.prepare_commit()
        data_files = [file for message in messages for file in message.new_files]
        changelogs = [file for message in messages for file in message.changelog_files]
        assert data_files and changelogs
        assert sum(file.row_count for file in data_files) == 1
        assert sum(file.row_count for file in changelogs) == 2
        for file in data_files + changelogs:
            assert file.creation_time == completed[file.file_path]
        commit.commit(messages, 1) if stream else commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    read = table.new_read_builder()
    assert read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist() == [{'id': 1, 'value': 20}]
