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

"""Bounded Ray ingestion of fixed episode groups with optional recovery."""


def _open_session(target, catalog_options, session_options, checkpoint_options):
    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.common.file_io import FileIO
    from pypaimon.common.options import Options
    from pypaimon.write.recoverable_write import RecoverableWriteSession

    table = CatalogFactory.create(catalog_options).get_table(target)
    options = dict(session_options)
    checkpoint_location = options.get("checkpoint_location")
    if checkpoint_location is not None:
        # A checkpoint may be in another bucket. Do not copy table credentials.
        options["checkpoint_file_io"] = FileIO.get(
            checkpoint_location, Options(checkpoint_options or {}))
    return RecoverableWriteSession(table, **options)


def _write_episode_task(target, catalog_options, session_options,
                        checkpoint_options, task_id, processor):
    options = dict(session_options)
    options["resume"] = options["checkpoint_location"] is not None
    session = _open_session(target, catalog_options, options, checkpoint_options)
    if options["checkpoint_location"] is not None:
        session.write_task(task_id, processor)
        return task_id
    return session.prepare_task(task_id, processor)


def write_episodes(
        target, catalog_options, episodes, processor, *, write_id,
        input_manifest_id, processing_version, episodes_per_task,
        commit_every_tasks=1, checkpoint_location=None, checkpoint_options=None,
        resume=False, concurrency=4, ray_remote_args=None):
    """Append ordered episode groups on Ray and publish completed tasks in batches.

    ``episodes`` contains JSON records with a unique ``episode_id`` and optional
    application ``input``. Each worker calls ``processor(task)`` once for a fixed
    group. The task has ``task_id``, ``episode_ids`` and ``episodes``; return an
    Arrow Table/RecordBatch or an iterable of either. Only prepared metadata is
    returned to the driver, which commits every ``commit_every_tasks`` completed
    groups and then the tail. ``concurrency`` bounds outstanding Ray tasks.

    Checkpointing is opt-in and requires an external ``checkpoint_location``.
    Its ``checkpoint_options`` configure a separate FileIO on driver and workers;
    they never inherit the table's storage credentials. Reuse the write ID,
    ordered input, processing version and ``episodes_per_task`` with
    ``resume=True`` to reuse prepared files and skip published tasks. Checkpoints
    are retained for manual cleanup. Without checkpointing, every invocation
    reprocesses and appends all inputs, including previously committed batches.

    Processor side effects are at-least-once. Only bucket-unaware, single-table
    appends are supported; LeRobot companion-table publication is excluded.
    All workers need access to the catalog, table storage and optional checkpoint
    storage. Returns the session's committed/prepared/pending task and episode
    progress, including the contiguous committed ``resume_position``.
    """
    if not callable(processor):
        raise ValueError("processor must be callable")
    if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
        raise ValueError("concurrency must be a positive integer")
    if checkpoint_location is None and checkpoint_options is not None:
        raise ValueError("checkpoint_options requires checkpoint_location")
    remote_args = dict(ray_remote_args or {})
    if "num_returns" in remote_args:
        raise ValueError("num_returns is managed by write_episodes")

    import ray

    options = {
        "write_id": write_id,
        "episodes": list(episodes),
        "input_manifest_id": input_manifest_id,
        "processing_version": processing_version,
        "episodes_per_task": episodes_per_task,
        "commit_every_tasks": commit_every_tasks,
        "checkpoint_location": checkpoint_location,
        "resume": resume,
    }
    session = _open_session(target, catalog_options, options, checkpoint_options)
    progress = session.recover()
    session.commit_ready()
    if not progress["pending_tasks"]:
        return session.finish()
    pending = iter(progress["pending_tasks"])
    worker = ray.remote(_write_episode_task).options(**remote_args)
    options_ref = ray.put(options)
    processor_ref = ray.put(processor)
    outstanding = []

    def submit_next():
        task_id = next(pending, None)
        if task_id is None:
            return False
        outstanding.append(worker.remote(
            target, catalog_options, options_ref, checkpoint_options,
            task_id, processor_ref))
        return True

    try:
        for _ in range(concurrency):
            if not submit_next():
                break
        while outstanding:
            ready, outstanding = ray.wait(outstanding, num_returns=1)
            result = ray.get(ready[0])
            if checkpoint_location is None:
                session.accept_task(result)
            else:
                session.accept_task_id(result)
            session.commit_ready()
            submit_next()
        return session.finish()
    finally:
        for reference in outstanding:
            ray.cancel(reference, force=False)
