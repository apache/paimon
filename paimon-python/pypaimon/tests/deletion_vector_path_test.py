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

import os
import subprocess
import sys
import time as system_time
from pathlib import Path
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.read.native_plan import native_plan, native_version_at_least
from pypaimon.schema.data_types import AtomicType
from pypaimon.write.file_store_commit import _abort_commit_messages
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_delete import TableDeleteByRowId


_PLANNERS = ['python', pytest.param('native', marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_version_at_least(0, 4, 0),
    reason='pypaimon-rust>=0.4.0 required for native DV paths')])]


def _table(tmp_path, layout, first_partition='a', partition_type=None, legacy_partition_name=True):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    options = {
        'bucket': '-1', 'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'deletion-vectors.enabled': 'true', 'scan.native-plan.enabled': 'false',
        'index-file-in-data-file-dir': str(layout.startswith('bucket')).lower(),
        'partition.legacy-name': str(legacy_partition_name).lower(),
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
    if partition_type is not None and pa.types.is_timestamp(partition_type):
        second_partition = first_partition + timedelta(days=1)
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
@pytest.mark.parametrize('value,partition_type,canonical_name', [
    ('a/b', None, 'a%2Fb'), (True, pa.bool_(), 'true'),
    (0.1, pa.float32(), '0.1'),
    (0.0001, pa.float32(), '1.0E-4'),
    (1e7, pa.float32(), '1.0E7'),
    (-0.0, pa.float32(), '-0.0'),
    (1.4e-45, pa.float32(), '1.4E-45'),
    (3.4028234663852886e38, pa.float32(), '3.4028235E38'),
    (1.17549435e-38, pa.float32(), '1.17549435E-38'),
    (1.17549435e-38, pa.float32(), '1.1754944E-38'),
    (2.68873286e11, pa.float32(), '2.68873286E11'),
    (2.68873286e11, pa.float32(), '2.6887329E11'),
    (9.64991956e24, pa.float32(), '9.6499195E24'),
    (1e23, pa.float64(), '9.999999999999999E22'),
    (1e23, pa.float64(), '1.0E23'),
    (-2.3345394554987242e17, pa.float64(), '-2.33453945549872416E17'),
    (5e-324, pa.float64(), '4.9E-324'),
    (datetime(2026, 9, 15, 12), pa.timestamp('s'), '2026-09-15 12%3A00%3A00'),
    (datetime(2026, 9, 15, 12), pa.timestamp('ms'), '2026-09-15 12%3A00%3A00.000'),
    (datetime(2026, 9, 15, 12, 0, 0, 120000), pa.timestamp('ms'), '2026-09-15 12%3A00%3A00.120'),
    (datetime(2026, 9, 15, 12), pa.timestamp('us'), '2026-09-15 12%3A00%3A00.000000'),
    (datetime(2026, 9, 15, 12, 0, 0, 123456), pa.timestamp('us'), '2026-09-15 12%3A00%3A00.123456'),
    (datetime(2026, 9, 15, 12), pa.timestamp('ns'), '2026-09-15 12%3A00%3A00.000000000'),
])
def test_java_canonical_bucket_dv_without_external_path(tmp_path, planner, value, partition_type, canonical_name):
    _check_java_bucket_dv(tmp_path, planner, value, partition_type, canonical_name, False)


@pytest.mark.parametrize('planner', _PLANNERS)
@pytest.mark.parametrize('value,partition_type,canonical_name', [
    (datetime(2026, 9, 15, 12), pa.timestamp('ms'), '2026-09-15T12%3A00'),
    (datetime(2026, 9, 15, 12, 0, 1), pa.timestamp('ms'), '2026-09-15T12%3A00%3A01'),
    (datetime(2026, 9, 15, 12, 0, 0, 120000), pa.timestamp('ms'), '2026-09-15T12%3A00%3A00.120'),
    (datetime(2026, 9, 15, 12, 0, 0, 120100), pa.timestamp('us'), '2026-09-15T12%3A00%3A00.120100'),
])
def test_java_legacy_bucket_dv_without_external_path(tmp_path, planner, value, partition_type, canonical_name):
    _check_java_bucket_dv(tmp_path, planner, value, partition_type, canonical_name, True)


@pytest.mark.skipif(not hasattr(system_time, 'tzset'), reason='requires POSIX timezone support')
@pytest.mark.parametrize('local_zone', ['UTC', 'Asia/Shanghai'])
@pytest.mark.parametrize('type_name', ['TIMESTAMP_LTZ(3)', 'TIMESTAMP(3) WITH LOCAL TIME ZONE'])
@pytest.mark.parametrize('value', [
    datetime(2026, 9, 15, 20, 0, 0, 120000),
    datetime(2026, 9, 15, 20, 0, 0, 120000, tzinfo=timezone.utc),
    datetime(2026, 9, 16, 4, 0, 0, 120000, tzinfo=timezone(timedelta(hours=8))),
])
@pytest.mark.parametrize('legacy', [False, True])
def test_ltz_canonical_partition_normalizes_timezone(tmp_path, monkeypatch, local_zone, type_name, value, legacy):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        pa.schema([('p', pa.timestamp('ms', tz='UTC'))]), partition_keys=['p'],
        options={'partition.legacy-name': str(legacy).lower()}), False)
    factory = catalog.get_table('db.t').path_factory()
    factory.partition_types = [AtomicType(type_name)]
    # Naive LTZ values represent UTC, just as GenericRow serialization does.
    with monkeypatch.context() as context:
        context.setenv('TZ', local_zone)
        system_time.tzset()
        try:
            if legacy:
                expected = '2026-09-15T20%3A00%3A00.120'
            elif local_zone == 'UTC':
                expected = '2026-09-15 20%3A00%3A00.120'
            else:
                expected = '2026-09-16 04%3A00%3A00.120'
            assert factory.relative_bucket_path((value,), 2, True) == 'p=' + expected + '/bucket-2'
        finally:
            context.undo()
            system_time.tzset()


@pytest.mark.skipif(not hasattr(system_time, 'tzset'), reason='requires POSIX timezone support')
@pytest.mark.parametrize('planner', _PLANNERS)
@pytest.mark.parametrize('local_zone', ['UTC', 'Asia/Shanghai'])
@pytest.mark.parametrize('legacy,unit,micros,fraction', [
    (False, 'ms', 0, '.000'), (False, 'us', 120100, '.120100'),
    (True, 'ms', 120000, '.120'), (True, 'us', 120100, '.120100'),
])
def test_java_ltz_bucket_dv_without_external_path(tmp_path, planner, local_zone, legacy, unit, micros, fraction):
    if legacy:
        canonical_name = '2026-09-15T20%3A00%3A00' + fraction
    elif local_zone == 'UTC':
        canonical_name = '2026-09-15 20%3A00%3A00' + fraction
    else:
        canonical_name = '2026-09-16 04%3A00%3A00' + fraction
    # A fresh process gives Python and Rust the same default timezone without
    # changing Rust's process-global timezone cache between test cases.
    script = '''
import sys
from datetime import datetime, timezone
from pathlib import Path
import pyarrow as pa
from pypaimon.tests.deletion_vector_path_test import _check_java_bucket_dv
_check_java_bucket_dv(
    Path(sys.argv[1]), sys.argv[2],
    datetime(2026, 9, 15, 20, 0, 0, int(sys.argv[4]), tzinfo=timezone.utc),
    pa.timestamp(sys.argv[3], tz='UTC'), sys.argv[5], sys.argv[6] == 'True')
'''
    result = subprocess.run(
        [sys.executable, '-c', script, str(tmp_path), planner, unit, str(micros), canonical_name, str(legacy)],
        env=dict(os.environ, TZ=local_zone), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    assert result.returncode == 0, result.stdout


def _check_java_bucket_dv(tmp_path, planner, value, partition_type, canonical_name, legacy_partition_name):
    table = _table(tmp_path, 'bucket', value, partition_type, legacy_partition_name)
    _delete(table, [0])
    old = _entries(table, 2)[0]
    canonical_path = str(Path(table.table_path) / ('p=' + canonical_name) / ('bucket-' + str(old.bucket)) /
                         old.index_file.file_name)
    # Model the persisted layout produced by Java: the bucket path is canonical
    # and no explicit index location is needed in its manifest metadata.
    old_path = table.path_factory().bucket_index_path(
        tuple(old.partition.values), old.bucket, old.index_file, table.file_io)
    with table.file_io.new_input_stream(old_path) as stream:
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
    if not Path(old_path).samefile(canonical_path):
        table.file_io.delete_quietly(old_path)
        # Both previous Python locations must lose to every Java spelling.
        with table.file_io.new_output_stream(old_path) as stream:
            stream.write(b'not the canonical deletion vector')
    with table.file_io.new_output_stream(table.path_factory().index_path() + '/' + old.index_file.file_name) as stream:
        stream.write(b'not a bucket deletion vector')
    if partition_type is not None and pa.types.is_floating(partition_type):
        other_partition = str(Path(table.table_path) / 'p=2.0' / ('bucket-' + str(old.bucket)) /
                              old.index_file.file_name)
        with table.file_io.new_output_stream(other_partition) as stream:
            stream.write(b'not the requested floating partition')
    if planner == 'native' and partition_type is not None and pa.types.is_floating(partition_type):
        # Rust does not support floating partitions; exercise the real adapter
        # fallback against the same Java-produced DV layout.
        table = table.copy({'scan.native-plan.enabled': 'true'})
        planner = 'python'
        with patch('pypaimon.read.native_plan.native_plan', wraps=native_plan) as native_call:
            plan = _read(table, planner, 3, [1, 2, 3])
        assert native_call.call_count == 1
    else:
        plan = _read(table, planner, 3, [1, 2, 3])
    paths = [dv.dv_index_path for split in plan.splits()
             for dv in split.data_deletion_files or [] if dv is not None]
    assert paths == [canonical_path]
    _delete(table, [1])
    _read(table, planner, 4, [2, 3])
    _read(table, planner, 3, [1, 2, 3])


@pytest.mark.parametrize('legacy_partition_name,expected', [
    (False, 'ts=2026-09-15 12%3A00%3A00.000/day=1970-01-02/tm=12%3A34%3A56'),
    (True, 'ts=2026-09-15T12%3A00/day=1/tm=45296120'),
])
def test_typed_partition_paths_follow_partition_key_order(tmp_path, legacy_partition_name, expected):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    schema = pa.schema([
        ('id', pa.int64()), ('ts', pa.timestamp('ms')), ('p/q', pa.float32()),
        ('day', pa.date32()), ('tm', pa.time32('ms')), ('d', pa.decimal128(10, 9)),
    ])
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['p/q', 'ts', 'day', 'tm', 'd'],
        options={'partition.legacy-name': str(legacy_partition_name).lower()}), False)
    factory = catalog.get_table('db.t').path_factory()
    partition = (0.1, datetime(2026, 9, 15, 12), date(1970, 1, 2), time(12, 34, 56, 120000), Decimal('0E-9'))
    assert factory.relative_bucket_path(partition, 2, True) == (
        'p%2Fq=0.1/' + expected + '/d=0.000000000/bucket-2')


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
