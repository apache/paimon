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

"""Parquet tables retain compressed manifest history across Pure/Native commits."""

from contextlib import ExitStack
from io import BytesIO
from unittest.mock import Mock, patch

import fastavro
import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.tests import native_plan_rest_test
from pypaimon.tests.native_commit_semantics_test import _publication


rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [False, pytest.param(True, marks=pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed'))]
_SCHEMA = pa.schema([('id', pa.int64()), ('pt', pa.string()), ('value', pa.int64())])
_SEED = [dict(id=1, pt='a', value=10), dict(id=2, pt='b', value=20)]


def _prepare(builder, rows, identifier=None):
    writer = builder.new_write()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=_SCHEMA))
        return writer.prepare_commit() if identifier is None else writer.prepare_commit(identifier)
    finally:
        writer.close()


def _read(table, native, snapshot_id=None):
    options = {'scan.native-plan.enabled': str(native).lower(), 'read.native.enabled': str(native).lower()}
    if snapshot_id is not None:
        options['scan.snapshot-id'] = str(snapshot_id)
    builder = table.copy(options).new_read_builder()
    scan = builder.new_scan()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(scan.file_scanner, 'scan',
                                             side_effect=AssertionError('Python plan fallback')))
        plan = scan.plan()
        if native:
            assert all(getattr(split, '_native_split', None) is not None for split in plan.splits())
            stack.enter_context(patch('pypaimon.read.table_read.TableRead._create_split_read',
                                      side_effect=AssertionError('Python read fallback')))
        return builder.new_read().to_arrow(plan.splits()).sort_by('id').to_pylist()


def _assert_codec(table, snapshot, codec, primary_key):
    def records(name):
        with table.file_io.new_input_stream(table.table_path.rstrip('/') + '/manifest/' + name) as stream:
            reader = fastavro.reader(BytesIO(stream.read()))
        assert reader.metadata['avro.codec'] == codec
        return list(reader)

    lists = ManifestListManager(table)
    for name in [snapshot.base_manifest_list, snapshot.delta_manifest_list, snapshot.changelog_manifest_list]:
        if name is not None:
            records(name)
            for manifest in lists.read(name):
                entries = records(manifest.file_name)
                assert all(entry['_FILE']['_FILE_NAME'].endswith('.parquet') for entry in entries)
    if primary_key and snapshot.total_record_count:
        assert snapshot.index_manifest
        assert records(snapshot.index_manifest)


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('codec', ['bzip2', 'xz'])
@pytest.mark.parametrize('primary_key', [False, True], ids=['append', 'dynamic-pk'])
def test_compressed_manifest_history_survives_native_publication_and_reads(
        rest_catalog, use_native, codec, primary_key):
    catalog, server = rest_catalog
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, partition_keys=['pt'], primary_keys=['id', 'pt'] if primary_key else [], options={
            'manifest.compression': codec, 'bucket': '-1', 'dynamic-bucket.target-row-num': '100',
            'changelog-producer': 'input' if primary_key else 'none',
            'dynamic-partition-overwrite': 'false',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
            'commit.min-retry-wait': '0 ms', 'commit.max-retry-wait': '0 ms',
        }), False)
    table = catalog.get_table('default.t')
    seed = table.new_batch_write_builder()
    commit = seed.new_commit()
    try:
        commit.commit(_prepare(seed, _SEED))
    finally:
        commit.close()
    first = table.snapshot_manager().get_latest_snapshot()
    _assert_codec(table, first, codec, primary_key)
    assert _read(table, use_native) == _SEED

    table = table.copy({'commit.native.enabled': str(use_native).lower()})
    builder = table.new_stream_write_builder()
    commit = builder.new_commit()
    callback = Mock()
    commit.add_commit_callback(callback)
    expected = list(_SEED)
    try:
        for identifier, row in [(11, dict(id=3, pt='a', value=30)),
                                (12, dict(id=4, pt='c' if primary_key else None, value=40))]:
            messages = _prepare(builder, [row], identifier)
            with _publication(server, commit, use_native, lose_response=identifier == 12) as attempts:
                commit.commit(messages, identifier, snapshot_properties={'codec': codec})
            assert attempts == [200]
            expected.append(row)
            snapshot = table.snapshot_manager().get_latest_snapshot()
            assert snapshot.commit_identifier == identifier
            assert snapshot.properties == {'codec': codec}
            _assert_codec(table, snapshot, codec, primary_key)
            assert _read(table, use_native) == expected
        assert callback.call.call_count == 2
    finally:
        commit.close()
    callback.close.assert_called_once()
    history = table.snapshot_manager().get_latest_snapshot()

    replacement = dict(id=9, pt='a', value=90)
    overwrite = table.new_batch_write_builder().overwrite({'pt': 'a'})
    commit = overwrite.new_commit()
    try:
        with _publication(server, commit, use_native):
            commit.commit(_prepare(overwrite, [replacement]))
    finally:
        commit.close()
    snapshot = table.snapshot_manager().get_latest_snapshot()
    assert snapshot.commit_kind == 'OVERWRITE'
    _assert_codec(table, snapshot, codec, primary_key)
    assert _read(table, use_native) == [expected[1], expected[3], replacement]

    commit = table.new_batch_write_builder().new_commit()
    try:
        with _publication(server, commit, use_native):
            commit.truncate_partitions([{'pt': 'b'}])
    finally:
        commit.close()
    assert _read(table, use_native) == [expected[3], replacement]
    _assert_codec(table, table.snapshot_manager().get_latest_snapshot(), codec, primary_key)

    commit = table.new_batch_write_builder().new_commit()
    try:
        with _publication(server, commit, use_native):
            commit.truncate_table()
    finally:
        commit.close()
    assert _read(table, use_native) == []
    assert _read(table, use_native, history.id) == expected
    assert _read(table, use_native, first.id) == _SEED
