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

"""REST commits preserve Java's empty-overwrite and changelog-only semantics."""

from contextlib import contextmanager
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.tests import native_plan_rest_test
from pypaimon.write.commit_message import CommitMessage


rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [False, pytest.param(True, marks=pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed'))]
_SCHEMA = pa.schema([('id', pa.int64()), ('pt', pa.string()), ('region', pa.string())])
_ROWS = [dict(id=1, pt='a', region='us'), dict(id=2, pt='b', region='eu')]


def _table(catalog, use_native, ignore_empty, primary_key=False):
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, primary_keys=['id'] if primary_key else [],
        partition_keys=[] if primary_key else ['pt', 'region'], options={
            'query-auth.enabled': 'true', 'bucket': '1' if primary_key else '-1',
            'changelog-producer': 'input' if primary_key else 'none',
            'write.native.enabled': 'false', 'read.native.enabled': 'false',
            'scan.native-plan.enabled': 'false',
            'commit.native.enabled': str(use_native).lower(),
            'snapshot.ignore-empty-commit': str(ignore_empty).lower(),
            'commit.min-retry-wait': '0 ms', 'commit.max-retry-wait': '0 ms',
        }), False)
    return catalog.get_table('default.t')


def _prepare(builder, rows, identifier=None):
    writer = builder.new_write()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=_SCHEMA))
        return writer.prepare_commit() if identifier is None else writer.prepare_commit(identifier)
    finally:
        writer.close()


@contextmanager
def _publication(server, commit, use_native, lose_response=False):
    original = server._table_commit_handle
    attempts = []

    def publish(*args, **kwargs):
        response = original(*args, **kwargs)
        attempts.append(response[1])
        assert response[1] == 200
        # The snapshot is published but the client cannot decode its response.
        return ('not-json', 200) if lose_response else response

    with patch.object(server, '_table_commit_handle', side_effect=publish), \
            patch.object(server, '_table_query_auth_handle',
                         side_effect=AssertionError('publication must not request SELECT')), \
            patch.object(commit.file_store_commit, 'abort', side_effect=AssertionError('implicit abort')):
        if use_native:
            with patch.object(commit.file_store_commit, '_try_commit',
                              side_effect=AssertionError('native commit fell back')):
                yield attempts
        else:
            yield attempts


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('ignore_empty', [False, True])
@pytest.mark.parametrize('target', ['empty-table', 'empty-partition', 'missing-partition', 'partial-spec'])
def test_truncate_records_empty_overwrite_and_notifies(rest_catalog, use_native, ignore_empty, target):
    catalog, server = rest_catalog
    table = _table(catalog, use_native, ignore_empty)
    seeded = target in ('missing-partition', 'partial-spec')
    if seeded:
        builder = table.copy({'commit.native.enabled': 'false'}).new_batch_write_builder()
        seed_commit = builder.new_commit()
        try:
            seed_commit.commit(_prepare(builder, _ROWS))
        finally:
            seed_commit.close()
    commit = table.new_batch_write_builder().new_commit()
    callback = Mock()
    commit.add_commit_callback(callback)
    try:
        with _publication(server, commit, use_native) as attempts:
            if target == 'empty-table':
                commit.truncate_table()
            else:
                spec = {'pt': 'absent'}
                if target != 'partial-spec':
                    spec['region'] = 'us'
                commit.truncate_partitions([spec])
        assert attempts == [200]
        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.id == (2 if seeded else 1)
        assert snapshot.commit_kind == 'OVERWRITE'
        assert snapshot.commit_identifier == BATCH_COMMIT_IDENTIFIER
        assert snapshot.total_record_count == (2 if seeded else 0)
        assert snapshot.delta_record_count == 0
        callback.call.assert_called_once()
        context = callback.call.call_args[0][0]
        assert context.snapshot == snapshot
        assert context.identifier == BATCH_COMMIT_IDENTIFIER
        assert context.commit_entries == []
    finally:
        commit.close()
    callback.close.assert_called_once()


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('ignore_empty', [False, True])
@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('lose_response', [False, True])
def test_changelog_only_commit_keeps_snapshot_and_recovers_once(
        rest_catalog, use_native, ignore_empty, stream, lose_response):
    catalog, server = rest_catalog
    table = _table(catalog, use_native, ignore_empty, primary_key=True)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    prepared = _prepare(builder, _ROWS[:1], 7 if stream else None)
    assert sum(len(message.changelog_files) for message in prepared) == 1
    changes = [CommitMessage(message.partition, message.bucket, [],
                             changelog_files=message.changelog_files) for message in prepared]
    paths = [file.file_path for message in prepared for file in message.new_files + message.changelog_files]
    commit = builder.new_commit()
    callback = Mock()
    commit.add_commit_callback(callback)
    try:
        with _publication(server, commit, use_native, lose_response) as attempts:
            args = (changes, 7) if stream else (changes,)
            commit.commit(*args, snapshot_properties={'source': 'changelog-only'})
        assert attempts == [200], 'lost response recovery must not publish a second snapshot'
        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.id == 1
        assert snapshot.commit_kind == 'APPEND'
        assert snapshot.commit_identifier == (7 if stream else BATCH_COMMIT_IDENTIFIER)
        assert snapshot.total_record_count == snapshot.delta_record_count == 0
        assert snapshot.changelog_record_count == 1
        assert snapshot.changelog_manifest_list
        assert snapshot.properties == {'source': 'changelog-only'}
        manifests = ManifestListManager(table).read_changelog(snapshot)
        entries = [entry for manifest in manifests for entry in ManifestFileManager(table).read(
            manifest.file_name, drop_stats=False, file_size=manifest.file_size)]
        assert [entry.file.file_name for entry in entries] == [
            file.file_name for message in changes for file in message.changelog_files]
        callback.call.assert_called_once()
        context = callback.call.call_args[0][0]
        assert context.snapshot == snapshot
        assert context.identifier == snapshot.commit_identifier
        assert context.commit_entries == [], 'changelogs are not data-file deltas'
        assert all(table.file_io.exists(path) for path in paths)

        if stream:
            with _publication(server, commit, use_native) as empty_attempts:
                commit.commit([], 8)
            latest = table.snapshot_manager().get_latest_snapshot()
            if ignore_empty:
                assert empty_attempts == []
                assert latest.id == 1
                callback.call.assert_called_once()
            else:
                assert empty_attempts == [200]
                assert latest.id == 2
                assert latest.commit_identifier == 8
                assert latest.properties is None
                assert latest.changelog_manifest_list is None
                assert latest.changelog_record_count is None
                assert callback.call.call_count == 2
        else:
            with pytest.raises(RuntimeError, match='one-time'):
                commit.commit([])
            assert table.snapshot_manager().get_latest_snapshot().id == 1
    finally:
        commit.close()
    callback.close.assert_called_once()


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('operation', ['truncate-table', 'truncate-partitions', 'changelog-only'])
def test_zero_data_snapshot_callback_error_preserves_confirmed_publication(rest_catalog, use_native, operation):
    catalog, server = rest_catalog
    table = _table(catalog, use_native, True, primary_key=operation == 'changelog-only')
    builder = table.new_batch_write_builder()
    paths = []
    changes = []
    if operation == 'changelog-only':
        prepared = _prepare(builder, _ROWS[:1])
        changes = [CommitMessage(message.partition, message.bucket, [],
                                 changelog_files=message.changelog_files) for message in prepared]
        assert sum(len(message.changelog_files) for message in changes) == 1
        paths = [file.file_path for message in prepared for file in message.new_files + message.changelog_files]
    commit = builder.new_commit()
    failure = LookupError('notification failed after publication')
    callback = Mock()
    callback.call.side_effect = failure
    follower = Mock()
    commit.add_commit_callback(callback)
    commit.add_commit_callback(follower)
    try:
        with _publication(server, commit, use_native, lose_response=True) as attempts:
            with pytest.raises(LookupError) as error:
                if operation == 'truncate-table':
                    commit.truncate_table()
                elif operation == 'truncate-partitions':
                    commit.truncate_partitions([{'pt': 'absent'}])
                else:
                    commit.commit(changes)
        assert error.value is failure
        assert attempts == [200]
        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.id == 1
        assert snapshot.commit_kind == ('APPEND' if operation == 'changelog-only' else 'OVERWRITE')
        assert snapshot.total_record_count == snapshot.delta_record_count == 0
        callback.call.assert_called_once()
        assert callback.call.call_args[0][0].snapshot == snapshot
        follower.call.assert_not_called()
        assert all(table.file_io.exists(path) for path in paths)
    finally:
        commit.close()
    callback.close.assert_called_once()
    follower.close.assert_called_once()
