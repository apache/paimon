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

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.write import recoverable_write as write_api


@pytest.fixture
def table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', False)
    schema = Schema.from_pyarrow_schema(
        pa.schema([('episode', pa.string()), ('value', pa.int64())]),
        options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'})
    catalog.create_table('default.frames', schema, False)
    return catalog.get_table('default.frames')


def episodes():
    return [{'episode_id': 'ep-' + str(i), 'input': i} for i in range(3)]


def session(table, **kwargs):
    cls = getattr(write_api, 'RecoverableWriteSession', None)
    assert cls is not None, 'public recoverable write API is missing'
    kwargs.setdefault('checkpoint_location', table.table_path.rsplit('/', 2)[0] + '/checkpoints')
    options = dict(write_id='run-1', episodes=episodes(), episodes_per_task=1,
                   input_manifest_id='input-v1', processing_version='model-v1',
                   commit_every_tasks=2)
    options.update(kwargs)
    return cls(table, **options)


def rows(table):
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist()


def produce(task):
    return pa.table({'episode': task['episode_ids'],
                     'value': [e['input'] for e in task['episodes']]})


def task_id(writer, n):
    return writer.plan[n]['task_id']


def test_threshold_tail_and_out_of_order_episode_progress(table):
    writer = session(table)
    writer.write_task(task_id(writer, 2), produce)
    assert writer.commit_ready() == []
    assert writer.progress()['prepared_episodes'] == ['ep-2']
    writer.write_task(task_id(writer, 0), produce)
    assert len(writer.commit_ready()) == 1
    assert sorted(r['episode'] for r in rows(table)) == ['ep-0', 'ep-2']
    progress = writer.progress()
    assert progress['committed_episodes'] == ['ep-0', 'ep-2']
    assert progress['pending_episodes'] == ['ep-1']
    assert progress['resume_position'] == 1
    writer.write_task(task_id(writer, 1), produce)
    writer.finish()
    assert len(rows(table)) == 3
    assert writer.progress()['resume_position'] == 3
    reopened = session(table, resume=True)
    reopened.finish()
    assert len(rows(table)) == 3


def test_success_keeps_checkpoint_for_caller_managed_cleanup(table):
    writer = session(table)
    writer.write_task(task_id(writer, 0), produce)
    writer.commit_ready(flush=True)
    batch = writer._read('batches/1.json')
    assert all('messages' not in result for result in batch['results'])
    assert writer._read(writer._task_path(task_id(writer, 0))) is not None
    resumed = session(table, resume=True)
    assert resumed.progress()['committed_episodes'] == ['ep-0']


def test_coordinator_resolves_newly_sealed_predecessor_before_next_batch(table, monkeypatch):
    first = session(table)
    first.write_task(task_id(first, 0), produce)
    first.write_task(task_id(first, 1), produce)

    def disconnected(batch):
        raise ConnectionError('driver stopped before commit')
    monkeypatch.setattr(first, '_publish', disconnected)
    with pytest.raises(ConnectionError):
        first.commit_ready()
    second = session(table, resume=True)
    # A competing driver sealed the predecessor after this driver's initial scan.
    monkeypatch.setattr(second, 'recover', lambda: {})
    second.commit_ready()
    assert len(list(second._batches())) == 1
    assert len(rows(table)) == 2


def test_commit_poll_does_not_probe_every_unfinished_task(table, monkeypatch):
    writer = session(table)
    writer.write_task(task_id(writer, 0), produce)
    calls = []
    original = writer.io.exists

    def count(path):
        if '/tasks/' in path:
            calls.append(path)
        return original(path)
    monkeypatch.setattr(writer.io, 'exists', count)
    writer.commit_ready()
    writer.commit_ready()
    assert calls == []


@pytest.mark.parametrize('changed', [
    {'episodes_per_task': 2},
    {'processing_version': 'model-v2'},
    {'input_manifest_id': 'input-v2'},
    {'episodes': list(reversed(episodes()))},
    {'episodes': [{'episode_id': 'ep-0', 'input': 999}] + episodes()[1:]},
])
def test_resume_rejects_changed_work_in_same_write_id(table, changed):
    writer = session(table)
    writer.write_task(task_id(writer, 0), produce)
    with pytest.raises(ValueError, match='identity or input plan'):
        session(table, resume=True, **changed)
    assert table.snapshot_manager().get_latest_snapshot() is None


def test_resume_can_change_commit_threshold_without_regrouping(table):
    writer = session(table)
    writer.write_task(task_id(writer, 0), produce)
    assert writer.commit_ready() == []
    resumed = session(table, resume=True, commit_every_tasks=1)
    assert resumed.plan == writer.plan
    resumed.commit_ready()
    assert rows(table) == [{'episode': 'ep-0', 'value': 0}]


@pytest.mark.parametrize('suffix', ['', '/checkpoints', '/data/../checkpoints'])
def test_checkpoint_cannot_live_inside_table(table, suffix):
    with pytest.raises(ValueError, match='outside the table'):
        session(table, checkpoint_location=table.table_path + suffix)


def test_missing_or_existing_session_requires_explicit_intent(table):
    with pytest.raises(FileNotFoundError, match='checkpoint session is missing'):
        session(table, resume=True)
    session(table)
    with pytest.raises(FileExistsError, match='resume=True'):
        session(table)


def test_zero_row_task_is_committed_and_not_reprocessed(table):
    writer = session(table, episodes=episodes()[:1], commit_every_tasks=1)

    def no_rows(task):
        return []
    writer.write_task(task_id(writer, 0), no_rows)
    assert writer.finish()['committed_episodes'] == ['ep-0']
    assert table.snapshot_manager().get_latest_snapshot().id == 1
    resumed = session(table, episodes=episodes()[:1], resume=True)

    def never(task):
        raise AssertionError('completed zero-row task was processed again')
    resumed.write_task(task_id(resumed, 0), never)
    assert resumed.finish()['pending_episodes'] == []
    assert table.snapshot_manager().get_latest_snapshot().id == 1
