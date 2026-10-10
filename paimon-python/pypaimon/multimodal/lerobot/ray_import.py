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

from pathlib import Path

from pypaimon.multimodal.hdf5 import _SnapshotRecorder
from pypaimon.multimodal.lerobot.loader import _prepare_dataset
from pypaimon.multimodal.lerobot.source import (
    _RemoteLeRobotDataset, _resolved_source,
)


def _require_ray():
    try:
        import ray
    except ImportError as error:
        raise ImportError("Ray import requires 'pypaimon[ray,lerobot]'.") from error
    if not ray.is_initialized():
        raise RuntimeError("Call ray.init() before using engine='ray'.")
    return ray


def _ray_source_uri(source):
    if isinstance(source, str) and "://" in source:
        return source
    if isinstance(source, Path) or isinstance(source, str) and Path(source).is_dir():
        return Path(source).expanduser().resolve().as_uri()
    raise ValueError(
        "Ray import requires a FileIO URI or shared directory, not a Hub repo_id.")


def _episode_groups(info, episodes, video_fields):
    # Keep Episodes sharing any camera payload in the same task.
    parents = list(range(len(episodes)))

    def root(i):
        while parents[i] != i:
            parents[i] = parents[parents[i]]
            i = parents[i]
        return i

    owners = {}
    for i in range(len(episodes)):
        episode = episodes[i]
        for field in video_fields:
            prefix = "videos/%s/" % field
            path = info["video_path"].format(
                video_key=field, chunk_index=episode[prefix + "chunk_index"],
                file_index=episode[prefix + "file_index"])
            previous = owners.setdefault(path, i)
            parents[root(i)] = root(previous)
    groups = {}
    for i in range(len(episodes)):
        groups.setdefault(root(i), []).append(i)
    return list(groups.values())


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
                       metadata, video_fields=(), *, concurrency=None,
                       source_options=None):
    ray = _require_ray()
    groups = iter(_episode_groups(info, metadata["episodes"], video_fields))
    limit = concurrency or max(1, int(ray.cluster_resources().get("CPU", 1)))
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
        exhausted = False
        while pending or not exhausted:
            while not exhausted and len(pending) < limit:
                group = next(groups, None)
                if group is None:
                    exhausted = True
                    break
                pending.append(prepare.remote(
                    table_ref, source.path, source_options, info_ref,
                    source_schema, batch_size, metadata_ref, video_fields, group))
            if pending:
                ready, pending = ray.wait(pending, num_returns=1)
                messages.extend(ray.get(ready[0]))
    except BaseException:
        for task in pending:
            ray.cancel(task)
        # Prepared files may outlive failed tasks; never delete committed data.
        raise

    recorder = _SnapshotRecorder()
    commit = table.raw_table.new_batch_write_builder().new_commit()
    try:
        commit.add_commit_callback(recorder)
        commit.commit(messages)
        if recorder.snapshot_id is None:
            raise RuntimeError("LeRobot append committed without reporting a snapshot id.")
        return recorder.snapshot_id
    finally:
        commit.close()
