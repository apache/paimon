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

from unittest.mock import Mock, patch

import pytest

from pypaimon.tests import native_commit_test

pytestmark = pytest.mark.native_plan


@native_commit_test.requires_native
@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
@pytest.mark.parametrize('properties', [None, {}, {'source': 'capture', 'label': '中文'}])
@pytest.mark.parametrize('overwrite', [False, True])
def test_rest_native_snapshot_properties(tmp_path, native_rest_catalog, mode, properties, overwrite):
    table = native_commit_test._table(tmp_path, mode, catalog=native_rest_catalog)
    native_commit_test._seed(table)
    builder = table.new_batch_write_builder()
    if overwrite:
        builder.overwrite()
    messages = native_commit_test._prepare(builder, [{'id': 10, 'pt': 'a'}])
    commit = builder.new_commit()
    try:
        with native_commit_test._must_not_fallback(commit, 'overwrite' if overwrite else 'commit'):
            commit.commit(messages, snapshot_properties=properties)
        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.properties == (properties or None)
        expected = ([{'id': 10, 'pt': 'a'}] if mode == 'pk' else [
            {'id': 2, 'pt': 'b'}, {'id': 3, 'pt': None}, {'id': 10, 'pt': 'a'}]) if overwrite else [
            {'id': 1, 'pt': 'a'}, {'id': 2, 'pt': 'b'}, {'id': 3, 'pt': None}, {'id': 10, 'pt': 'a'}]
        assert native_commit_test._rows(table) == expected
    finally:
        commit.close()


@native_commit_test.requires_native
@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
def test_stream_properties_do_not_leak_between_checkpoints(tmp_path, native_rest_catalog, mode):
    table = native_commit_test._table(tmp_path, mode, catalog=native_rest_catalog)
    builder = table.new_stream_write_builder()
    commit = builder.new_commit()
    properties = {'checkpoint': 'first'}
    try:
        with native_commit_test._must_not_fallback(commit):
            for identifier, metadata in enumerate([properties, None, {}, {'checkpoint': 'fourth'}], 1):
                messages = native_commit_test._prepare(builder, [{'id': identifier, 'pt': 'a'}], identifier)
                commit.commit(messages, identifier, snapshot_properties=metadata)
                snapshot = table.snapshot_manager().get_latest_snapshot()
                assert snapshot.commit_identifier == identifier
                assert snapshot.properties == (metadata or None)
        properties['checkpoint'] = 'changed'
        assert table.snapshot_manager().get_snapshot_by_id(1).properties == {'checkpoint': 'first'}
        assert native_commit_test._rows(table) == [{'id': i, 'pt': 'a'} for i in range(1, 5)]
    finally:
        commit.close()


@native_commit_test.requires_native
@pytest.mark.parametrize('ignore_empty', [False, True])
def test_empty_append_properties_follow_empty_commit_option(tmp_path, native_rest_catalog, ignore_empty):
    table = native_commit_test._table(tmp_path, catalog=native_rest_catalog).copy(
        {'snapshot.ignore-empty-commit': str(ignore_empty).lower()})
    commit = table.new_batch_write_builder().new_commit()
    try:
        with native_commit_test._must_not_fallback(commit):
            commit.commit([], snapshot_properties={'source': 'empty'})
        snapshot = table.snapshot_manager().get_latest_snapshot()
        if ignore_empty:
            assert snapshot is None
        else:
            assert snapshot.properties == {'source': 'empty'}
            assert snapshot.total_record_count == 0
            assert snapshot.delta_record_count == 0
    finally:
        commit.close()


@native_commit_test.requires_native
@pytest.mark.parametrize('mode', ['append', 'de'])
def test_empty_static_overwrite_records_properties(tmp_path, native_rest_catalog, mode):
    table = native_commit_test._table(tmp_path, mode, catalog=native_rest_catalog).copy(
        {'dynamic-partition-overwrite': 'false'})
    native_commit_test._seed(table)
    builder = table.new_batch_write_builder().overwrite({'pt': 'a'})
    commit = builder.new_commit()
    try:
        with native_commit_test._must_not_fallback(commit, 'overwrite'):
            commit.commit([], snapshot_properties={'reason': 'empty overwrite'})
        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.properties == {'reason': 'empty overwrite'}
        assert snapshot.commit_kind == 'OVERWRITE'
        assert snapshot.delta_record_count == -1
        assert native_commit_test._rows(table) == [{'id': 2, 'pt': 'b'}, {'id': 3, 'pt': None}]
    finally:
        commit.close()


@native_commit_test.requires_native
def test_empty_dynamic_overwrite_does_not_publish_properties(tmp_path, native_rest_catalog):
    table = native_commit_test._table(tmp_path, catalog=native_rest_catalog).copy(
        {'dynamic-partition-overwrite': 'true'})
    native_commit_test._seed(table)
    commit = table.new_batch_write_builder().overwrite().new_commit()
    try:
        with native_commit_test._must_not_fallback(commit, 'overwrite'):
            commit.commit([], snapshot_properties={'source': 'unused'})
        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.id == 1
        assert snapshot.properties is None
        assert len(native_commit_test._rows(table)) == 3
    finally:
        commit.close()


@native_commit_test.requires_native
@pytest.mark.parametrize('overwrite', [False, True])
def test_properties_survive_publication_response_loss(tmp_path, native_rest_catalog, overwrite):
    table = native_commit_test._table(tmp_path, catalog=native_rest_catalog)
    builder = table.new_batch_write_builder()
    if overwrite:
        builder.overwrite({'pt': 'a'})
    messages = native_commit_test._prepare(builder, [{'id': 1, 'pt': 'a'}])
    native = native_commit_test.create_native_commit(table, builder.commit_user, builder.static_partition)
    proxy = Mock(wraps=native)

    def publish_then_fail(*args, **kwargs):
        native.commit(*args, **kwargs)
        raise OSError('response lost')

    proxy.commit.side_effect = publish_then_fail
    commit = builder.new_commit()
    try:
        with patch('pypaimon.write.native_commit.create_native_commit', return_value=proxy), \
                native_commit_test._must_not_fallback(commit, 'overwrite' if overwrite else 'commit'), \
                patch.object(commit.file_store_commit, 'abort', side_effect=AssertionError('abort')):
            with pytest.raises(OSError, match='response lost'):
                commit.commit(messages, snapshot_properties={'source': 'published'})
        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.id == 1
        assert snapshot.properties == {'source': 'published'}
        assert native_commit_test._rows(table) == [{'id': 1, 'pt': 'a'}]
        proxy.abort.assert_not_called()
    finally:
        commit.close()
