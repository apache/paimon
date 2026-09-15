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

"""Deletion-vector locations must survive writes, further deletes and time travel."""

from pathlib import Path
from dataclasses import replace
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.read.native_plan import native_plan, native_version_at_least
from pypaimon.write.file_store_commit import _abort_commit_messages
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_delete import TableDeleteByRowId


_PLANNERS = ['python', pytest.param('native', marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_version_at_least(0, 4, 0),
    reason='pypaimon-rust>=0.4.0 required for native DV paths')])]


def _table(tmp_path, layout, first_partition='a', partition_type=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    options = {
        'bucket': '-1', 'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'deletion-vectors.enabled': 'true', 'scan.native-plan.enabled': 'false',
        'index-file-in-data-file-dir': str(layout.startswith('bucket')).lower(),
    }
    if layout == 'bucket-external':
        options.update({
            'data-file.external-paths': (tmp_path / 'external-data').as_uri(),
            'data-file.external-paths.strategy': 'round-robin',
            'global-index.external-path': (tmp_path / 'unused-global-index').as_uri(),
        })
    elif layout == 'global-external':
        options['global-index.external-path'] = (tmp_path / 'external-index').as_uri()
    second_partition = 'b' if partition_type is None else False if pa.types.is_boolean(partition_type) else 2.0
    schema = pa.schema([('p', pa.string() if partition_type is None else partition_type), ('k', pa.int64())])
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['p'], options=options), False)
    table = catalog.get_table('db.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pydict({
            'p': [first_partition, first_partition, second_partition, second_partition], 'k': [0, 1, 2, 3]}, schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table


def _delete(table, row_ids):
    builder = table.new_batch_write_builder()
    messages = builder.new_update().delete_by_row_id(row_ids)
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    return messages


def _entries(table, snapshot_id):
    snapshot = table.snapshot_manager().get_snapshot_by_id(snapshot_id)
    return IndexManifestFile(table).read(snapshot.index_manifest)


def _read(table, planner, snapshot_id, expected):
    table = table.copy({'scan.snapshot-id': str(snapshot_id)})
    builder = table.new_read_builder()
    # Direct invocation makes a native planning error fail rather than fall back.
    plan = native_plan(table) if planner == 'native' else builder.new_scan().plan()
    assert plan.snapshot_id == snapshot_id
    assert sorted(builder.new_read().to_arrow(plan.splits()).column('k').to_pylist()) == expected
    return plan


@pytest.mark.parametrize('planner', _PLANNERS)
@pytest.mark.parametrize('layout', ['table', 'bucket', 'bucket-external', 'global-external'])
def test_delete_paths_preserve_repeated_deletes_and_historical_reads(tmp_path, planner, layout):
    table = _table(tmp_path, layout)
    _delete(table, [0, 2])
    entries = _entries(table, 2)
    assert len(entries) == 2
    for entry in entries:
        file = entry.index_file
        bucket = table.path_factory().relative_bucket_path(tuple(entry.partition.values), entry.bucket)
        if layout == 'bucket':
            expected = Path(table.table_path) / bucket / file.file_name
        elif layout == 'bucket-external':
            expected = tmp_path / 'external-data' / bucket / file.file_name
        elif layout == 'global-external':
            expected = tmp_path / 'external-index' / file.file_name
        else:
            expected = Path(table.table_path) / 'index' / file.file_name
        assert expected.is_file()
        assert file.external_path == ('file://' + str(expected) if 'external' in layout else None)
        # Obsolete locations with the same name must never shadow canonical or
        # explicit paths. Invalid bytes make a wrong-path read fail observably.
        if layout != 'table':
            decoy = Path(table.table_path) / 'index' / file.file_name
            decoy.parent.mkdir(parents=True, exist_ok=True)
            decoy.write_bytes(b'not a deletion vector')
        if layout == 'bucket-external':
            decoy = Path(table.table_path) / bucket / file.file_name
            decoy.parent.mkdir(parents=True, exist_ok=True)
            decoy.write_bytes(b'not an external deletion vector')
    plan = _read(table, planner, 2, [1, 3])
    assert all(table.file_io.exists(dv.dv_index_path) for split in plan.splits()
               for dv in split.data_deletion_files if dv is not None)
    _delete(table, [1])
    _read(table, planner, 3, [3])
    _read(table, planner, 2, [1, 3])
    _read(table, planner, 1, [0, 1, 2, 3])


@pytest.mark.parametrize('planner', _PLANNERS)
def test_legacy_python_index_directory_remains_readable_and_new_deletes_use_bucket(tmp_path, planner):
    table = _table(tmp_path, 'bucket')
    # Old Python writers ignored the option and placed the index under table/index.
    legacy_factory = table.path_factory()
    legacy_factory.index_file_in_data_file_dir = False
    with patch.object(table, 'path_factory', return_value=legacy_factory):
        _delete(table, [0, 2])
    old_paths = [Path(table.table_path) / 'index' / entry.index_file.file_name for entry in _entries(table, 2)]
    assert all(path.is_file() for path in old_paths)
    _read(table, planner, 2, [1, 3])
    _delete(table, [1])
    _read(table, planner, 3, [3])
    _read(table, planner, 2, [1, 3])
    for entry in _entries(table, 3):
        if entry.partition.values == ['a']:
            assert (Path(table.path_factory().bucket_path(('a',), entry.bucket)) /
                    entry.index_file.file_name).is_file()
    assert all(path.is_file() for path in old_paths)


@pytest.mark.parametrize('layout', ['bucket', 'bucket-external', 'global-external'])
def test_abort_removes_uncommitted_dv_from_its_actual_directory(tmp_path, layout):
    table = _table(tmp_path, layout)
    _delete(table, [0])
    builder = table.new_batch_write_builder()
    messages = builder.new_update().delete_by_row_id([1])
    uncommitted = [entry for message in messages for entry in message.index_adds]
    assert uncommitted
    files_before = set(tmp_path.rglob('index-*'))
    _abort_commit_messages(table, messages)
    files_after = set(tmp_path.rglob('index-*'))
    removed = {path.name for path in files_before - files_after}
    assert removed == {entry.index_file.file_name for entry in uncommitted}
    _read(table, 'python', 2, [1, 2, 3])


@pytest.mark.parametrize('planner', _PLANNERS)
def test_missing_explicit_dv_is_not_replaced_by_a_local_copy(tmp_path, planner):
    table = _table(tmp_path, 'bucket-external')
    _delete(table, [0])
    entry = _entries(table, 2)[0]
    file = entry.index_file
    with table.file_io.new_input_stream(file.external_path) as stream:
        data = stream.read()
    factory = table.path_factory()
    for directory in [factory.index_path(), factory.bucket_path(tuple(entry.partition.values), entry.bucket)]:
        with table.file_io.new_output_stream(directory + '/' + file.file_name) as stream:
            stream.write(data)
    table.file_io.delete_quietly(file.external_path)
    with pytest.raises(FileNotFoundError):
        _read(table, planner, 2, [1, 2, 3])


@pytest.mark.parametrize('planner', _PLANNERS)
@pytest.mark.parametrize('partition', ['a/b', 'a%2Fb', 'a=b', 'a#b', 'a b', '中文'])
def test_bucket_dv_in_partition_requiring_path_escaping(tmp_path, planner, partition):
    table = _table(tmp_path, 'bucket', partition)
    _delete(table, [0])
    file = _entries(table, 2)[0].index_file
    if partition in ['a b', '中文']:
        assert file.external_path is None
    else:
        assert file.external_path is not None
        assert table.file_io.exists(file.external_path)
    _read(table, planner, 2, [1, 2, 3])


@pytest.mark.parametrize('planner', _PLANNERS)
@pytest.mark.parametrize('partition_type,value', [(pa.bool_(), True), (pa.float32(), 0.1), (pa.float64(), 0.1)],
                         ids=['BOOLEAN', 'FLOAT', 'DOUBLE'])
def test_bucket_dv_preserves_python_typed_partition_directory(tmp_path, planner, partition_type, value):
    table = _table(tmp_path, 'bucket', value, partition_type)
    _delete(table, [0])
    file = _entries(table, 2)[0].index_file
    assert file.external_path is not None
    assert table.file_io.exists(file.external_path)
    effective_planner = planner
    if planner == 'native' and pa.types.is_floating(partition_type):
        # Rust intentionally rejects floating partition formatting until it can
        # reproduce Java Float/Double.toString, including boundary values.
        with pytest.raises(NotImplementedError, match='type is not supported as partition key'):
            native_plan(table)
        native_table = table.copy({'scan.native-plan.enabled': 'true'})
        scan = native_table.new_read_builder().new_scan()
        with patch('pypaimon.read.native_plan.native_plan', wraps=native_plan) as native_call:
            with patch.object(scan.file_scanner, 'scan', wraps=scan.file_scanner.scan) as fallback:
                plan = scan.plan()
        assert native_call.call_count == 1
        assert fallback.call_count == 1
        result = native_table.new_read_builder().new_read().to_arrow(plan.splits())
        assert sorted(result.column('k').to_pylist()) == [1, 2, 3]
        effective_planner = 'python'
    _read(table, effective_planner, 2, [1, 2, 3])
    _delete(table, [1])
    _read(table, effective_planner, 3, [2, 3])
    _read(table, effective_planner, 2, [1, 2, 3])


@pytest.mark.parametrize('planner', _PLANNERS)
@pytest.mark.parametrize('value,partition_type,canonical_name', [('a/b', None, 'a%2Fb'), (True, pa.bool_(), 'true')])
def test_java_canonical_bucket_dv_without_external_path(tmp_path, planner, value, partition_type, canonical_name):
    table = _table(tmp_path, 'bucket', value, partition_type)
    _delete(table, [0])
    old = _entries(table, 2)[0]
    canonical_path = str(Path(table.table_path) / ('p=' + canonical_name) / ('bucket-' + str(old.bucket)) /
                         old.index_file.file_name)
    # Model the persisted layout produced by Java: the bucket path is canonical
    # and no explicit index location is needed in its manifest metadata.
    with table.file_io.new_input_stream(old.index_file.external_path) as stream:
        data = stream.read()
    with table.file_io.new_output_stream(canonical_path) as stream:
        stream.write(data)
    canonical = replace(old, index_file=replace(old.index_file, external_path=None))
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.commit([CommitMessage(
            partition=tuple(old.partition.values), bucket=old.bucket, new_files=[],
            check_from_snapshot=2, index_adds=[canonical], index_deletes=[replace(old, kind=1)])])
    finally:
        commit.close()
    # Case-insensitive local filesystems consider p=True and p=true identical.
    if not Path(old.index_file.external_path).samefile(canonical_path):
        table.file_io.delete_quietly(old.index_file.external_path)
    plan = _read(table, planner, 3, [1, 2, 3])
    paths = [dv.dv_index_path for split in plan.splits()
             for dv in split.data_deletion_files or [] if dv is not None]
    assert paths == [canonical_path]
    _delete(table, [1])
    _read(table, planner, 4, [2, 3])
    _read(table, planner, 3, [1, 2, 3])


@pytest.mark.parametrize('planner', _PLANNERS)
def test_multiple_dv_index_files_in_one_bucket_keep_all_deletions(tmp_path, planner):
    table = _table(tmp_path, 'bucket')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'p': ['a', 'a'], 'k': [4, 5]}))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    _delete(table, [0, 4])
    old = _entries(table, 3)[0]
    deleter = TableDeleteByRowId(table)
    _, vectors = deleter._read_existing_deletion_vectors(old.partition, old.bucket, 3)
    assert len(vectors) == 2
    # Java rolls DV index files by target size, so one bucket can legitimately
    # have more than one live DV index entry for different data files.
    indexes = [deleter._write_deletion_vector_index(old.partition, old.bucket, {name: vector})
               for name, vector in vectors.items()]
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.commit([CommitMessage(
            partition=tuple(old.partition.values), bucket=old.bucket, new_files=[],
            check_from_snapshot=3, index_adds=indexes, index_deletes=[replace(old, kind=1)])])
    finally:
        commit.close()
    _read(table, planner, 4, [1, 2, 3, 5])
    _delete(table, [1])
    _read(table, planner, 5, [2, 3, 5])
    _read(table, planner, 4, [1, 2, 3, 5])
