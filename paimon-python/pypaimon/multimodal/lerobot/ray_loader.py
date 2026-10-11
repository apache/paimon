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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Ray workers prepare LeRobot files; only the driver commits snapshots."""

import logging
from pathlib import Path

from pypaimon.multimodal.lerobot.loader import _commit_messages, _prepare_dataset
from pypaimon.multimodal.lerobot.source import (
    _RemoteLeRobotDataset, _resolved_source,
)


def _require_ray():
    try:
        import ray
    except ImportError as error:
        raise ImportError("Parallel import requires 'pypaimon[ray,lerobot]'.") from error
    if not ray.is_initialized():
        raise RuntimeError("Call ray.init() before using parallelism > 1.")
    return ray


def _ray_source_uri(source):
    if isinstance(source, str) and "://" in source:
        return source
    if isinstance(source, Path) or isinstance(source, str) and Path(source).is_dir():
        return Path(source).expanduser().resolve().as_uri()
    raise ValueError(
        "Parallel import requires a FileIO URI or shared directory, not a Hub repo_id.")


def _episode_groups(info, episodes, video_fields):
    # Keep Episodes sharing a data or video file in the same task.
    parents = list(range(len(episodes)))

    def root(i):
        while parents[i] != i:
            parents[i] = parents[parents[i]]
            i = parents[i]
        return i

    owners = {}
    for i in range(len(episodes)):
        episode = episodes[i]
        data_path = _RemoteLeRobotDataset._data_path(episode, info)
        previous = owners.setdefault(("data", data_path), i)
        parents[root(i)] = root(previous)
        for field in video_fields:
            prefix = "videos/%s/" % field
            path = info["video_path"].format(
                video_key=field, chunk_index=episode[prefix + "chunk_index"],
                file_index=episode[prefix + "file_index"])
            previous = owners.setdefault(("video", path), i)
            parents[root(i)] = root(previous)
    groups = {}
    for i in range(len(episodes)):
        groups.setdefault(root(i), []).append(i)
    return list(groups.values())


def _task_groups(info, episodes, video_fields, parallelism):
    groups = _episode_groups(info, episodes, video_fields)
    tasks = [[] for _ in range(min(parallelism, len(groups)))]
    for i, group in enumerate(groups):
        tasks[i % len(tasks)].extend(group)
    return [sorted(task) for task in tasks]


def _abort_messages(table, messages):
    if not messages:
        return
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.abort(messages)
    finally:
        commit.close()


def _prepare_group(table, source_path, source_options, info, schema,
                   batch_size, metadata, video_fields, episode_indices):
    with _resolved_source(source_path, source_options) as (source, current_info):
        if current_info != info:
            raise ValueError("LeRobot source metadata changed during import.")
        dataset = _RemoteLeRobotDataset(source, info)
        try:
            return _prepare_dataset(
                table, dataset, info, source, schema, batch_size,
                metadata, video_fields, episode_indices)
        finally:
            dataset.close()


def _write_dataset_ray(table, dataset, info, source, source_schema, batch_size,
                       metadata, video_fields=(), *, parallelism,
                       source_options=None):
    ray = _require_ray()
    groups = _task_groups(info, dataset._episodes, video_fields, parallelism)
    # Share only validated metadata, never the driver's dataset or open streams.
    table_ref = ray.put(table.raw_table)
    info_ref = ray.put(info)
    metadata_ref = ray.put({
        "episodes": metadata["episodes"],
        "subtask_indices": metadata["subtask_indices"],
    })
    prepare = ray.remote(num_cpus=1, max_retries=0)(_prepare_group)
    pending = []
    messages = []
    try:
        for group in groups:
            pending.append(prepare.remote(
                table_ref, source.path, source_options, info_ref,
                source_schema, batch_size, metadata_ref, video_fields, group))
        while pending:
            ready, pending = ray.wait(pending, num_returns=1)
            messages.extend(ray.get(ready[0]))
    except BaseException:
        # No snapshot was committed. Let in-flight writers finish so their
        # prepared messages can be aborted as well.
        for task in pending:
            try:
                messages.extend(ray.get(task))
            except BaseException:
                pass
        try:
            _abort_messages(table.raw_table, messages)
        except Exception:
            logging.exception("Failed to abort prepared LeRobot files.")
        raise

    return _commit_messages(table.raw_table, messages)
