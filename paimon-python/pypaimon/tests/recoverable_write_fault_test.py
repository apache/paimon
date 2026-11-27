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

import base64
import json
import multiprocessing
from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.utils.file_store_path_factory import canonical_data_file_path
from pypaimon.write.commit_message_serializer import deserialize_commit_message
from pypaimon.write.recoverable_write import RecoverableWriteSession


@pytest.fixture
def table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', False)
    catalog.create_table('default.frames', Schema.from_pyarrow_schema(
        pa.schema([('episode', pa.string())]),
        options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}), False)
    return catalog.get_table('default.frames')


def session(table, **kwargs):
    kwargs.setdefault('checkpoint_location', table.table_path.rsplit('/', 2)[0] + '/checkpoints')
    return RecoverableWriteSession(
        table, write_id='fault-run', episodes=[{'episode_id': 'ep-0'}, {'episode_id': 'ep-1'}],
        input_manifest_id='input-v1', processing_version='model-v1',
        episodes_per_task=1, commit_every_tasks=1, **kwargs)


def produce(task):
    return pa.table({'episode': task['episode_ids']})


def read_episodes(table):
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).column('episode').to_pylist()


def test_missing_prepared_data_file_never_publishes_dangling_manifest(table):
    writer = session(table)
    task_id = writer.plan[0]['task_id']
    prepared = writer.prepare_task(task_id, produce)
    message = deserialize_commit_message(
        base64.b64decode(prepared['messages'][0]), table.partition_keys_fields,
        version=prepared['wire_version'])
    data_file = message.new_files[0]
    data_path = canonical_data_file_path(table, message.partition, message.bucket, data_file.file_name)
    assert table.file_io.exists(data_path)
    table.file_io.delete(data_path)

    resumed = session(table, resume=True)
    with pytest.raises((FileNotFoundError, RuntimeError), match='(?i)(file|missing)'):
        resumed.commit_ready()
    assert table.snapshot_manager().get_latest_snapshot() is None
    assert resumed.progress()['committed_episodes'] == []


def test_crash_after_commit_before_receipt_recovers_without_recomputation(table, monkeypatch):
    writer = session(table)
    task_id = writer.plan[0]['task_id']
    writer.prepare_task(task_id, produce)
    original_put = writer._put

    def lose_receipt(name, value):
        if name.startswith('receipts/'):
            raise ConnectionError('driver disconnected after publication')
        return original_put(name, value)

    monkeypatch.setattr(writer, '_put', lose_receipt)
    with pytest.raises(ConnectionError):
        writer.commit_ready()
    assert read_episodes(table) == ['ep-0']

    resumed = session(table, resume=True)
    resumed.recover()
    processor = Mock(side_effect=AssertionError('committed input recomputed'))
    resumed.prepare_task(task_id, processor)
    processor.assert_not_called()
    assert read_episodes(table) == ['ep-0']
    assert table.snapshot_manager().get_latest_snapshot().id == 1
    assert resumed.progress()['committed_episodes'] == ['ep-0']


def test_without_checkpoint_new_session_rewrites_same_input(table):
    for _ in range(2):
        writer = session(table, checkpoint_location=None)
        writer.write_task(writer.plan[0]['task_id'], produce)
        writer.commit_ready()
    assert read_episodes(table) == ['ep-0', 'ep-0']


def test_corrupt_batch_members_fail_closed(table, monkeypatch):
    writer = session(table)
    writer.prepare_task(writer.plan[0]['task_id'], produce)
    monkeypatch.setattr(writer, '_publish', Mock(side_effect=ConnectionError('before commit')))
    with pytest.raises(ConnectionError):
        writer.commit_ready()
    path = writer.path + '/batches/1.json'
    batch = json.loads(writer.io.read_file_utf8(path))
    batch['task_ids'] = [writer.plan[1]['task_id']]
    writer.io.overwrite_file_utf8(path, json.dumps(batch))

    resumed = session(table, resume=True)
    with pytest.raises(ValueError, match='(?i)(batch|checkpoint)'):
        resumed.recover()
    assert table.snapshot_manager().get_latest_snapshot() is None


def test_corrupt_receipt_task_members_fail_closed(table):
    writer = session(table)
    writer.prepare_task(writer.plan[0]['task_id'], produce)
    writer.commit_ready()
    path = writer.path + '/receipts/1.json'
    receipt = json.loads(writer.io.read_file_utf8(path))
    receipt['task_ids'] = [writer.plan[1]['task_id']]
    writer.io.overwrite_file_utf8(path, json.dumps(receipt))
    resumed = session(table, resume=True)
    with pytest.raises(ValueError, match='(?i)(receipt|checkpoint)'):
        resumed.progress()


def _commit_in_process(warehouse, barrier, outcomes):
    try:
        table = CatalogFactory.create({'warehouse': warehouse}).get_table('default.frames')
        writer = session(table, resume=True)
        barrier.wait(timeout=15)
        writer.commit_ready()
        outcomes.put(('ok', writer.progress()['committed_episodes']))
    except Exception as error:
        outcomes.put(('error', repr(error)))


def test_two_processes_publish_same_prepared_batch_once(table):
    writer = session(table)
    writer.prepare_task(writer.plan[0]['task_id'], produce)
    warehouse = str(Path(table.table_path).parent.parent)
    context = multiprocessing.get_context('spawn')
    barrier = context.Barrier(2)
    outcomes = context.Queue()
    processes = [context.Process(target=_commit_in_process, args=(warehouse, barrier, outcomes))
                 for _ in range(2)]
    try:
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=25)
            assert not process.is_alive(), 'concurrent coordinator did not finish'
            assert process.exitcode == 0
        results = [outcomes.get(timeout=5) for _ in processes]
        assert results == [('ok', ['ep-0']), ('ok', ['ep-0'])]
        assert read_episodes(table) == ['ep-0']
        assert table.snapshot_manager().get_latest_snapshot().id == 1
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
        outcomes.close()


def test_task_checkpoint_cannot_impersonate_another_planned_task(table):
    writer = session(table)
    first, second = [task['task_id'] for task in writer.plan]
    writer.prepare_task(first, produce)
    writer.prepare_task(second, produce)
    # Both results have a valid session identity; the storage key must also bind
    # the result to its own task, or batch selection can repeatedly pick second.
    writer.io.overwrite_file_utf8(
        writer.path + '/' + writer._task_path(first),
        writer.io.read_file_utf8(writer.path + '/' + writer._task_path(second)))
    resumed = session(table, resume=True)
    with pytest.raises(ValueError, match='(?i)(task|checkpoint|result)'):
        resumed.recover()
        resumed.commit_ready()
    assert table.snapshot_manager().get_latest_snapshot() is None
