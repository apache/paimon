# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""CPU benchmark of pinned CWD planning and ordered scalar/BLOB batch reads.

Prepare once, then run the same script against different source checkouts.
OS caches are uncontrolled. Diagnostic instrumentation is a separate run.
"""

import argparse
from contextlib import nullcontext
import hashlib
import json
from pathlib import Path
import platform
import random
import struct
import tempfile
import time
from unittest.mock import patch

import pyarrow as pa
import torch

import pypaimon
import pypaimon.multimodal as pmm
from pypaimon.multimodal import window_dataset
from pypaimon.read.table_scan import TableScan


_TABLE = "benchmark.cwd_frames"
_MANIFEST = "cwd-benchmark.json"
_OFFSETS = [-2] + list(range(16))


def _payload(seed, episode, step, length):
    key = "%d:%d:%d" % (seed, episode, step)
    return hashlib.shake_256(key.encode()).digest(length)


def prepare(warehouse, tasks, episodes, frames, blob_bytes, seed):
    """Write one episode per commit; never append to an existing benchmark."""
    if warehouse.exists():
        raise ValueError("prepare requires a new warehouse directory")
    if min(tasks, episodes, blob_bytes) < 1 or frames <= 17:
        raise ValueError("positive dimensions and at least 18 frames required")
    table = pmm.connect(options={"warehouse": str(warehouse)}).create_table(
        _TABLE,
        schema=pa.schema([
            pa.field("task", pa.int32(), nullable=False),
            pa.field("episode", pa.int64(), nullable=False),
            pa.field("step", pa.int32(), nullable=False),
            pa.field("value", pa.int64(), nullable=False),
            pa.field("payload", pa.large_binary(), nullable=False),
        ]),
        partitioned=["task"],
        options={
            "row-tracking.enabled": "true",
            "data-evolution.enabled": "true",
            "deletion-vectors.enabled": "true",
            "file.format": "parquet",
            "vector.file.format": "parquet",
            "blob-as-descriptor": "true",
        },
    )
    for task in range(tasks):
        for local_episode in range(episodes):
            episode = task * episodes + local_episode
            table.add([
                {"task": task, "episode": episode, "step": step,
                 "value": episode * frames + step,
                 "payload": _payload(seed, episode, step, blob_bytes)}
                for step in range(frames)
            ])
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    manifest = dict(version=1, table=_TABLE, tasks=tasks, episodes=episodes,
                    frames=frames, blob_bytes=blob_bytes, seed=seed,
                    snapshot_id=snapshot.id)
    (warehouse / _MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def _indices(length, count, seed):
    order = list(range(length))
    random.Random(seed).shuffle(order)
    return [order[index % length] for index in range(count)]


def _check_and_hash(samples, indices, manifest, first_episode, digest):
    windows_per_episode = manifest["frames"] - 17
    for sample, index in zip(samples, indices):
        episode = first_episode + index // windows_per_episode
        step = index % windows_per_episode + 2
        values = [episode * manifest["frames"] + step + offset
                  for offset in _OFFSETS]
        payload = _payload(manifest["seed"], episode, step, manifest["blob_bytes"])
        if ((sample["episode"], sample["step"]) != (episode, step)
                or sample["value"] != values
                or sample["payload"] != [payload]
                or any(sample["value_is_pad"])
                or any(sample["payload_is_pad"])):
            raise AssertionError("CWD output differs from generated sample oracle")
        digest.update(struct.pack("<qq", episode, step))
        digest.update(struct.pack("<%dq" % len(values), *values))
        digest.update(payload)
    if len(samples) != len(indices):
        raise AssertionError("CWD returned a different batch size")


def run(warehouse, scope, task, batch_size, batches, seed, diagnostic=False):
    """Measure one fresh dataset; verify every sample outside fetch timers."""
    manifest_bytes = (warehouse / _MANIFEST).read_bytes()
    manifest = json.loads(manifest_bytes)
    if manifest["version"] != 1:
        raise ValueError("unsupported benchmark manifest version")
    if not 0 <= task < manifest["tasks"] or min(batch_size, batches) < 1:
        raise ValueError("invalid task, batch size or steady batch count")
    events = []
    phase = ["build"]
    original = TableScan.plan

    def recording_plan(scan):
        plan = original(scan)
        splits = plan.splits()
        files = {file.file_name for split in splits for file in split.files}
        events.append(dict(phase=phase[0], splits=len(splits), files=len(files)))
        return plan

    instrumentation = (patch.object(TableScan, "plan", recording_plan)
                       if diagnostic else nullcontext())
    with instrumentation:
        started = time.perf_counter()
        table = pmm.connect(options={"warehouse": str(warehouse)}).get_table(manifest["table"])
        query = table.scan(snapshot_id=manifest["snapshot_id"])
        if scope == "task":
            query = query.where("task = %d" % task)
        dataset = query.to_contiguous_window_dataset(
            columns=["value", "payload"], frame_offsets={"value": _OFFSETS},
            group_key="episode", order_key="step", boundary="drop")
        build_s = time.perf_counter() - started
        expected = (manifest["frames"] - 17) * manifest["episodes"]
        if scope == "all":
            expected *= manifest["tasks"]
        if len(dataset) != expected or dataset.snapshot_id != manifest["snapshot_id"]:
            raise AssertionError("CWD cardinality or snapshot changed")
        indices = _indices(len(dataset), (batches + 1) * batch_size, seed)
        digest = hashlib.sha256()
        first_episode = task * manifest["episodes"] if scope == "task" else 0
        elapsed = []
        for batch in range(batches + 1):
            phase[0] = "first_batch" if batch == 0 else "steady"
            selected = indices[batch * batch_size:(batch + 1) * batch_size]
            started = time.perf_counter()
            samples = dataset.__getitems__(selected)
            elapsed.append(time.perf_counter() - started)
            _check_and_hash(samples, selected, manifest, first_episode, digest)
    steady_s = sum(elapsed[1:])
    result = dict(
        scope=scope, task=task if scope == "task" else None,
        snapshot_id=manifest["snapshot_id"], dataset_samples=len(dataset),
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        seed=seed, batch_size=batch_size, steady_batches=batches,
        build_s=build_s, first_batch_s=elapsed[0], steady_s=steady_s,
        steady_samples=batches * batch_size,
        samples_per_s=batches * batch_size / steady_s,
        build_and_read_s=build_s + sum(elapsed),
        sample_digest=digest.hexdigest(), verified_samples=len(indices),
        cache_control="uncontrolled", device="cpu", num_workers=0,
        diagnostic=diagnostic, plan_events=events,
        source=str(Path(pypaimon.__file__).resolve()),
        window_dataset_sha256=hashlib.sha256(Path(window_dataset.__file__).read_bytes()).hexdigest(),
        python=platform.python_version(), platform=platform.platform(),
        pyarrow=pa.__version__, torch=torch.__version__,
    )
    return result


def selfcheck():
    """Exercise a real partitioned table and independently check every value."""
    assert _indices(3, 8, 7) == _indices(3, 8, 7)
    assert sorted(_indices(3, 3, 7)) == [0, 1, 2]
    with tempfile.TemporaryDirectory(prefix="cwd_benchmark_") as directory:
        warehouse = Path(directory) / "warehouse"
        prepare(warehouse, 2, 2, 24, 32, 7)
        for scope in ("task", "all"):
            plain = run(warehouse, scope, 1, 4, 2, 9)
            traced = run(warehouse, scope, 1, 4, 2, 9, True)
            assert plain["sample_digest"] == traced["sample_digest"]
            assert traced["plan_events"]
    return {"selfcheck": "passed"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("selfcheck")
    create = commands.add_parser("prepare")
    create.add_argument("--warehouse", type=Path, required=True)
    create.add_argument("--tasks", type=int, default=16)
    create.add_argument("--episodes", type=int, default=8, help="episodes per task")
    create.add_argument("--frames", type=int, default=128, help="frames per episode")
    create.add_argument("--blob-bytes", type=int, default=1024)
    create.add_argument("--seed", type=int, default=20261013)
    read = commands.add_parser("run")
    read.add_argument("--warehouse", type=Path, required=True)
    read.add_argument("--scope", choices=("task", "all"), required=True)
    read.add_argument("--task", type=int, default=0)
    read.add_argument("--batch-size", type=int, default=16)
    read.add_argument("--batches", type=int, default=32, help="steady batches after first batch")
    read.add_argument("--seed", type=int, default=20261013)
    read.add_argument("--diagnostic", action="store_true")
    args = vars(parser.parse_args())
    command = args.pop("command")
    if "warehouse" in args:
        args["warehouse"] = args["warehouse"].expanduser().resolve()
    result = {"prepare": prepare, "run": run, "selfcheck": selfcheck}[command](**args)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
