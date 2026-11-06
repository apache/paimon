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

"""Durable task checkpoints and recoverable micro-batches for append tables."""

import base64
import hashlib
import json
import os
import posixpath
from urllib.parse import unquote, urlparse
import uuid

from pypaimon.common.file_io import FileIO
from pypaimon.common.json_util import JSON
from pypaimon.snapshot.renaming_snapshot_commit import RenamingSnapshotCommit
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.write.commit_callback import CommitCallback
from pypaimon.write.commit_message_serializer import (
    VERSION, deserialize_commit_message, serialize_commit_message,
)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False)


def _digest(value):
    return hashlib.sha256(_json(value).encode('utf-8')).hexdigest()


def make_task_id(*, target_table_id, input_manifest_id, start, end,
                 processing_version):
    """Identify a fixed [start, end) range in an immutable ordered input manifest."""
    if (isinstance(start, bool) or isinstance(end, bool)
            or not isinstance(start, int) or not isinstance(end, int)
            or not 0 <= start < end):
        raise ValueError('Expected integer range 0 <= start < end')
    values = [target_table_id, input_manifest_id, processing_version]
    if any(not isinstance(value, str) or not value for value in values):
        raise ValueError('Table, input manifest and processing version must be nonempty strings')
    return 'task-' + _digest([1, values, start, end])


def _require_external_checkpoint(table_path, checkpoint_location):
    def normalize(value):
        parsed = urlparse(value)
        if parsed.scheme in ('', 'file'):
            return ('file', '', os.path.realpath(unquote(parsed.path)))
        return (parsed.scheme.lower(), parsed.netloc, posixpath.normpath(parsed.path))
    table = normalize(table_path)
    checkpoint = normalize(checkpoint_location)
    if table[:2] == checkpoint[:2] and (
            checkpoint[2] == table[2] or checkpoint[2].startswith(table[2].rstrip('/') + '/')):
        raise ValueError('Checkpoint must be outside the table directory')


class _SnapshotRecorder(CommitCallback):
    def __init__(self):
        self.snapshot = None

    def call(self, context):
        self.snapshot = context.snapshot


class _ConditionalSnapshotCommit(RenamingSnapshotCommit):
    def commit(self, base_snapshot_uuid, snapshot, statistics):
        # Generic rename can overwrite a winner in another process.
        success = self.file_io.create_file_if_absent(
            self.snapshot_manager.get_snapshot_path(snapshot.id),
            JSON.to_json(snapshot))
        if success:
            self._commit_latest_hint(snapshot.id)
        return success


class RecoverableWriteSession:
    """Append fixed tasks to one table with durable checkpoints.

    ``episodes`` is an ordered list of JSON objects with unique ``episode_id``
    values and optional application ``input``. Fixed groups of
    ``episodes_per_task`` entries define the tasks. Reopen with ``resume=True``
    and the same write ID, manifest, processing version, episodes and grouping
    to recover from an external checkpoint. Without a checkpoint, every new
    session writes all input again. Checkpoints are never automatically deleted.

    A processor receives a task with ``task_id``, ``episode_ids``, ``episodes``
    and a half-open input range ``[start, end)``. It returns an Arrow
    Table/RecordBatch or iterable of them. Processor side effects are
    at-least-once. ``commit_every_tasks`` controls publication independently
    of the fixed grouping and may change on resume for unsealed batches.

    Only full-row bucket-unaware appends are supported. Pause orphan cleanup
    for the recovery window, retain unresolved snapshot history, and do not
    change schemas, roll back, recreate the table or change branches. This is
    not a transaction over LeRobot companion tables. Checkpoint storage must
    implement conditional creation; an external FileIO can be supplied.
    """

    def __init__(self, table, *, write_id, episodes, input_manifest_id,
                 processing_version, episodes_per_task, commit_every_tasks=1,
                 checkpoint_location=None, checkpoint_file_io=None, resume=False):
        for name, value in [('episodes_per_task', episodes_per_task),
                            ('commit_every_tasks', commit_every_tasks)]:
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(name + ' must be a positive integer')
        if any(not isinstance(v, str) or not v
               for v in (write_id, input_manifest_id, processing_version)):
            raise ValueError('Write, input manifest and processing identities are required')
        if table.primary_keys or table.bucket_mode() != BucketMode.BUCKET_UNAWARE:
            raise ValueError('Recoverable writes require a bucket-unaware append table')
        if any(k.startswith('pypaimon.lerobot.') for k in table.table_schema.options):
            raise ValueError('LeRobot table groups require coordinated multi-table publication')
        episodes = json.loads(_json(episodes))
        if not isinstance(episodes, list):
            raise ValueError('episodes must be an ordered list')
        seen = set()
        for episode in episodes:
            eid = episode.get('episode_id') if isinstance(episode, dict) else None
            if not isinstance(eid, str) or not eid or eid in seen:
                raise ValueError('Expected unique nonempty episode IDs')
            seen.add(eid)
        self.plan = []
        for start in range(0, len(episodes), episodes_per_task):
            end = min(start + episodes_per_task, len(episodes))
            task_id = make_task_id(
                target_table_id=table.table_path, input_manifest_id=input_manifest_id,
                start=start, end=end, processing_version=processing_version)
            group = episodes[start:end]
            self.plan.append({'task_id': task_id, 'episode_ids': [e['episode_id'] for e in group],
                              'episodes': group, 'start': start, 'end': end})
        self.tasks = {t['task_id']: t for t in self.plan}
        self.table = table.copy({'snapshot.ignore-empty-commit': 'false',
                                 'write.native.enabled': 'false'})
        self.write_id = write_id
        self.commit_every_tasks = commit_every_tasks
        self.records = {}
        self.io = None
        self.path = None
        if checkpoint_location is not None:
            if not isinstance(checkpoint_location, str) or not checkpoint_location:
                raise ValueError('checkpoint_location must be a nonempty external path')
            _require_external_checkpoint(table.table_path, checkpoint_location)
            self.io = checkpoint_file_io or FileIO.get(checkpoint_location)
            self.path = checkpoint_location.rstrip('/') + '/' + _digest(write_id)
            self.io.mkdirs(self.path)
        elif checkpoint_file_io is not None or resume:
            raise ValueError('resume and checkpoint_file_io require checkpoint_location')
        self.schema_identity = _digest(JSON.to_json(table.schema_manager.latest()))
        expected = {'version': 1, 'table_path': table.table_path,
                    'branch': table.current_branch(), 'schema': self.schema_identity,
                    'write_id': write_id, 'input_manifest_id': input_manifest_id,
                    'processing_version': processing_version,
                    'episodes_per_task': episodes_per_task, 'tasks': self.plan}
        if resume:
            existing = self._read('session.json')
            if existing is None:
                raise FileNotFoundError('Cannot resume: checkpoint session is missing')
        else:
            if self._read('session.json') is not None:
                raise FileExistsError('Checkpoint already exists; use resume=True')
            existing = self._put('session.json', expected)
        if existing != expected:
            raise ValueError('Checkpoint identity or input plan does not match this session')
        self.identity = _digest(expected)
        self._discovered = not resume

    def _read(self, name):
        if name in self.records or self.io is None:
            return self.records.get(name)
        path = self.path + '/' + name
        if not self.io.exists(path):
            return None
        result = json.loads(self.io.read_file_utf8(path))
        self.records[name] = result
        return result

    def _put(self, name, value):
        if self.io is None:
            return self.records.setdefault(name, json.loads(_json(value)))
        path = self.path + '/' + name
        self.io.mkdirs(path.rsplit('/', 1)[0])
        self.io.create_file_if_absent(path, _json(value))
        result = self._read(name)
        if result is None:
            raise RuntimeError('Checkpoint was not published: ' + name)
        return result

    def _task_path(self, task_id):
        return 'tasks/' + _digest(task_id) + '.json'

    def _validate_task_result(self, task_id, result):
        if (not isinstance(result, dict) or result.get('session') != self.identity
                or result.get('task_id') != task_id or task_id not in self.tasks
                or result.get('wire_version') != VERSION
                or not isinstance(result.get('attempt_id'), str) or not result['attempt_id']
                or not isinstance(result.get('messages'), list)
                or any(not isinstance(message, str) for message in result['messages'])):
            raise ValueError('Task checkpoint does not match its session and task identity')
        return result

    def _read_task(self, task_id):
        result = self._read(self._task_path(task_id))
        return None if result is None else self._validate_task_result(task_id, result)

    def _check_schema(self):
        if _digest(JSON.to_json(self.table.schema_manager.latest())) != self.schema_identity:
            raise ValueError('Table schema changed during recoverable write')

    def _validate_batch(self, batch, number):
        if not isinstance(batch, dict):
            raise ValueError('Invalid batch checkpoint')
        body = {key: value for key, value in batch.items() if key != 'batch_id'}
        task_ids = batch.get('task_ids')
        refs = batch.get('results')
        if (batch.get('batch_id') != _digest(body)
                or batch.get('session') != self.identity or batch.get('number') != number
                or not isinstance(task_ids, list) or not task_ids
                or any(not isinstance(t, str) or t not in self.tasks for t in task_ids)
                or len(set(task_ids)) != len(task_ids)
                or not isinstance(refs, list)
                or any(not isinstance(r, dict) for r in refs)
                or [r.get('task_id') for r in refs] != task_ids):
            raise ValueError('Invalid batch checkpoint identity or members')

    def _validate_receipt(self, batch, receipt):
        if receipt is None:
            return
        if (not isinstance(receipt, dict) or receipt.get('batch_id') != batch['batch_id']
                or receipt.get('task_ids') != batch['task_ids']
                or isinstance(receipt.get('snapshot_id'), bool)
                or not isinstance(receipt.get('snapshot_id'), int)
                or receipt['snapshot_id'] <= batch['base_id']
                or not isinstance(receipt.get('snapshot_uuid'), str)
                or not receipt['snapshot_uuid']):
            raise ValueError('Receipt does not match batch checkpoint')

    def _batches(self):
        number = 1
        assigned = set()
        while True:
            batch = self._read('batches/%d.json' % number)
            if batch is None:
                return
            self._validate_batch(batch, number)
            if assigned.intersection(batch['task_ids']):
                raise ValueError('Task appears in multiple batch checkpoints')
            assigned.update(batch['task_ids'])
            receipt = self._read('receipts/%d.json' % number)
            self._validate_receipt(batch, receipt)
            yield batch, receipt
            number += 1

    def _completed(self):
        return {task_id for batch, receipt in self._batches() if receipt is not None
                for task_id in batch['task_ids']}

    def progress(self):
        """Return business progress in input order; positions never skip holes."""
        completed = self._completed()
        prepared = {task_id for task_id in self.tasks if task_id not in completed
                    and self._read_task(task_id) is not None}
        pending = set(self.tasks) - completed - prepared
        result = {}
        for status, task_ids in [('committed', completed), ('prepared', prepared),
                                 ('pending', pending)]:
            result[status + '_tasks'] = [
                t['task_id'] for t in self.plan if t['task_id'] in task_ids]
            result[status + '_episodes'] = [
                e for t in self.plan if t['task_id'] in task_ids for e in t['episode_ids']]
        position = 0
        for task in self.plan:
            if task['task_id'] not in completed:
                break
            position += len(task['episode_ids'])
        result['resume_position'] = position
        return result

    def prepare_task(self, task_id, processor):
        """Compute and prepare one task; a configured checkpoint is saved before return."""
        import pyarrow as pa

        self._check_schema()
        if task_id not in self.tasks:
            raise ValueError('Unknown task ID: ' + str(task_id))
        existing = self._read_task(task_id)
        if existing is not None:
            return existing
        writer = self.table.new_batch_write_builder().new_write()
        try:
            data = processor(json.loads(_json(self.tasks[task_id])))
            batches = [data] if isinstance(data, (pa.Table, pa.RecordBatch)) else data
            for batch in batches:
                if isinstance(batch, pa.Table):
                    writer.write_arrow(batch)
                else:
                    writer.write_arrow_batch(batch)
            messages = writer.prepare_commit()
            encoded = [base64.b64encode(serialize_commit_message(
                msg, self.table.partition_keys_fields)).decode('ascii') for msg in messages]
            return self.accept_task(
                {'session': self.identity, 'task_id': task_id,
                 'attempt_id': uuid.uuid4().hex, 'wire_version': VERSION,
                 'messages': encoded})
        finally:
            # An exception may follow successful checkpoint publication; never abort.
            writer.close()

    def accept_task(self, result):
        """Accept prepared messages returned by a worker (also without checkpoints)."""
        task_id = result.get('task_id') if isinstance(result, dict) else None
        self._validate_task_result(task_id, result)
        winner = self._put(self._task_path(task_id), result)
        return self._validate_task_result(task_id, winner)

    def accept_task_id(self, task_id):
        """Load a worker's durable result once, without scanning other tasks."""
        if task_id not in self.tasks:
            raise ValueError('Unknown task ID: ' + str(task_id))
        result = self._read_task(task_id)
        if result is None:
            raise RuntimeError('Worker did not publish its task checkpoint')
        return result

    def write_task(self, task_id, processor):
        """Prepare a task and return its ID, sending no row data to the coordinator."""
        self.prepare_task(task_id, processor)
        return task_id

    def _publish(self, batch):
        self._validate_batch(batch, batch['number'])
        receipt_name = 'receipts/%d.json' % batch['number']
        receipt = self._read(receipt_name)
        if receipt is None:
            from pypaimon.execution.python.write.file_store_commit import FileStoreCommit

            self._check_schema()
            messages = []
            for reference in batch['results']:
                result = self._read_task(reference['task_id'])
                if result is None:
                    if self._read(receipt_name) is not None:
                        return self._publish(batch)
                    raise RuntimeError('Prepared task checkpoint is missing')
                if _digest(result) != reference['digest']:
                    raise ValueError('Prepared task checkpoint changed')
                messages.extend(deserialize_commit_message(
                    base64.b64decode(raw, validate=True), self.table.partition_keys_fields,
                    version=result['wire_version']) for raw in result['messages'])
            recorder = _SnapshotRecorder()
            snapshot_commit = self.table.new_snapshot_commit()
            if self.io is not None and isinstance(snapshot_commit, RenamingSnapshotCommit):
                snapshot_commit = _ConditionalSnapshotCommit(self.table.snapshot_manager())
            committer = FileStoreCommit(snapshot_commit, self.table,
                                        'recoverable-' + batch['batch_id'], [recorder])
            try:
                committer.commit(
                    messages, batch['number'],
                    snapshot_properties={'pypaimon.write-id': self.write_id,
                                         'pypaimon.batch-id': batch['batch_id'],
                                         'pypaimon.task-plan': self.identity},
                    recovery_snapshot_id=batch['base_id'],
                    recovery_snapshot_uuid=batch['base_uuid'])
            finally:
                committer.close()
            if recorder.snapshot is None:
                raise RuntimeError('Commit returned without a snapshot receipt')
            receipt = self._put(receipt_name, {
                'batch_id': batch['batch_id'], 'snapshot_id': recorder.snapshot.id,
                'snapshot_uuid': recorder.snapshot.uuid, 'task_ids': batch['task_ids']})
        self._validate_receipt(batch, receipt)
        return receipt

    def recover(self):
        """Resolve every previously sealed batch, then report episode progress."""
        for batch, _ in self._batches():
            self._publish(batch)
        progress = self.progress()
        self._discovered = True
        return progress

    def commit_ready(self, flush=False):
        """Commit ready tasks at the threshold; ``flush`` also publishes a tail."""
        if not self._discovered:
            self.recover()
        receipts = []
        while True:
            batches = list(self._batches())
            unresolved = [batch for batch, receipt in batches if receipt is None]
            if unresolved:
                for batch in unresolved:
                    self._publish(batch)
                continue
            completed = {task_id for batch, receipt in batches if receipt is not None
                         for task_id in batch['task_ids']}
            results = []
            for task in self.plan:
                task_id = task['task_id']
                if task_id not in completed:
                    result = self.records.get(self._task_path(task_id))
                    if result is not None:
                        results.append(self._validate_task_result(task_id, result))
                if len(results) >= self.commit_every_tasks:
                    break
            if not results or (not flush and len(results) < self.commit_every_tasks):
                return receipts
            latest = self.table.snapshot_manager().get_latest_snapshot()
            number = len(batches) + 1
            batch = {'session': self.identity, 'number': number,
                     'base_id': latest.id if latest else 0,
                     'base_uuid': latest.uuid if latest else None,
                     'task_ids': [r['task_id'] for r in results],
                     'results': [{'task_id': r['task_id'], 'digest': _digest(r)}
                                 for r in results]}
            batch['batch_id'] = _digest(batch)
            batch = self._put('batches/%d.json' % number, batch)
            receipts.append(self._publish(batch))

    def finish(self):
        """Publish the tail and require every planned episode to be committed."""
        self.recover()
        self.commit_ready(flush=True)
        progress = self.progress()
        if progress['pending_tasks'] or progress['prepared_tasks']:
            raise RuntimeError('Input plan is incomplete: ' + str(progress['pending_tasks']))
        return progress
