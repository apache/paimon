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

"""Benchmark cold indexed and unindexed random video reads.

The run command expects short-lived object URLs in environment variables so
credentials never appear in arguments or results. Each sample uses a fresh
decoder, HTTP session, and byte cache. Object-store service caches cannot be
evicted by a client and remain outside this benchmark contract.
"""

import argparse
import hashlib
import io
import json
import os
import platform
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

import numpy as np
import requests

from pypaimon.multimodal.lerobot.dataset import _PyAVVideoDecoder
from pypaimon.table.row.video_keyframe_index import VideoKeyframeIndex


VIDEO_URL_ENV = "PYPAIMON_BENCHMARK_VIDEO_URL"
INDEX_URL_ENV = "PYPAIMON_BENCHMARK_INDEX_URL"


def _percentile(values, fraction):
    ordered = sorted(values)
    return ordered[
        min(round((len(ordered) - 1) * fraction), len(ordered) - 1)
    ]


def _summary(runs):
    latencies = [run["seconds"] * 1000 for run in runs]
    gets = [run["gets"] for run in runs]
    transferred = [run["bytes"] for run in runs]
    return {
        "runs": len(runs),
        "latency_ms_p50": statistics.median(latencies),
        "latency_ms_p95": _percentile(latencies, 0.95),
        "gets_p50": statistics.median(gets),
        "gets_p95": _percentile(gets, 0.95),
        "gets_total": sum(gets),
        "bytes_p50": statistics.median(transferred),
        "bytes_p95": _percentile(transferred, 0.95),
        "bytes_total": sum(transferred),
    }


def _runtime(source_commit):
    import av
    import pyarrow
    model = ""
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                model = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    return {
        "source_commit": source_commit,
        "python": platform.python_version(),
        "pyav": av.__version__,
        "pyarrow": pyarrow.__version__,
        "numpy": np.__version__,
        "requests": requests.__version__,
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "cpu_model": model,
    }


def _file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                return digest.hexdigest()
            digest.update(chunk)


class _HttpObjects:

    def __init__(self, video_url, index_url):
        self.video_url = video_url
        self.index_url = index_url
        self._local = threading.local()
        self._sessions = []
        self._lock = threading.Lock()
        self.gets = 0
        self.bytes = 0
        self.index_gets = 0
        self.index_bytes = 0

    def _session(self):
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.trust_env = False
            self._local.session = session
            with self._lock:
                self._sessions.append(session)
        return session

    def read_index(self):
        response = self._session().get(self.index_url, timeout=120)
        response.raise_for_status()
        data = response.content
        self._record(len(data), True)
        return data

    def read_video(self, offset, length):
        if length <= 0:
            return b""
        response = self._session().get(
            self.video_url,
            headers={"Range": "bytes=%d-%d" % (
                offset, offset + length - 1)},
            timeout=120,
        )
        response.raise_for_status()
        data = response.content
        if response.status_code != 206 or len(data) != length:
            raise IOError(
                "Object store did not return the requested video range."
            )
        self._record(len(data), False)
        return data

    def _record(self, length, index):
        with self._lock:
            self.gets += 1
            self.bytes += length
            if index:
                self.index_gets += 1
                self.index_bytes += length

    def close(self):
        for session in self._sessions:
            session.close()


class _HttpRangeStream(io.RawIOBase):

    def __init__(self, objects, length, parallelism):
        self._objects = objects
        self._length = length
        self._parallelism = parallelism
        self._position = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._position

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            position = offset
        elif whence == io.SEEK_CUR:
            position = self._position + offset
        elif whence == io.SEEK_END:
            position = self._length + offset
        else:
            raise ValueError("Invalid whence: %s" % whence)
        if position < 0:
            raise ValueError("Negative seek position: %s" % position)
        self._position = position
        return position

    def read(self, size=-1):
        length = (
            self._length - self._position
            if size is None or size < 0
            else min(size, self._length - self._position)
        )
        if length <= 0:
            return b""
        data = self._objects.read_video(self._position, length)
        self._position += len(data)
        return data

    def readinto(self, value):
        data = self.read(len(value))
        value[:len(data)] = data
        return len(data)

    def video_read_ranges(self, ranges):
        if len(ranges) < 2 or self._parallelism == 1:
            return [
                self._objects.read_video(offset, length)
                for offset, length in ranges
            ]
        with ThreadPoolExecutor(
                max_workers=min(self._parallelism, len(ranges))) as pool:
            return list(pool.map(
                lambda value: self._objects.read_video(*value), ranges))


def _frame_count(path):
    import av
    with av.open(str(path)) as container:
        stream = container.streams.video[0]
        if stream.frames:
            return stream.frames
        return sum(1 for unused in container.decode(stream))


def _generate_index(video, repetitions):
    payload_length = video.stat().st_size
    values = []
    serialized = None
    for unused in range(repetitions):
        started = time.perf_counter()
        with video.open("rb") as source:
            current = VideoKeyframeIndex.inspect(source, payload_length)
        values.append(time.perf_counter() - started)
        current_bytes = current.serialize()
        if serialized is not None and current_bytes != serialized:
            raise RuntimeError("Video index generation is not deterministic.")
        serialized = current_bytes
    return serialized, values


def prepare(args):
    video = args.video.resolve()
    serialized, durations = _generate_index(video, args.repetitions)
    args.index.write_bytes(serialized)
    result = {
        "contract": "pypaimon-video-index-ingestion-v1",
        "runtime": _runtime(args.source_commit),
        "video_bytes": video.stat().st_size,
        "video_sha256": _file_sha256(video),
        "frames": _frame_count(video),
        "index_bytes": len(serialized),
        "index_ratio": len(serialized) / video.stat().st_size,
        "generation_runs": len(durations),
        "generation_ms_p50": statistics.median(durations) * 1000,
        "generation_ms_p95": _percentile(durations, 0.95) * 1000,
        "generation_seconds": durations,
    }
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))


def _decode(mode, targets, video_length, expected_index, parallelism):
    objects = _HttpObjects(
        os.environ[VIDEO_URL_ENV], os.environ[INDEX_URL_ENV])
    stream = _HttpRangeStream(objects, video_length, parallelism)
    started = time.perf_counter()
    decoder = None
    try:
        mapping = None
        if mode == "indexed":
            index_bytes = objects.read_index()
            if index_bytes != expected_index:
                raise RuntimeError("Remote video index differs from local index.")
            mapping = VideoKeyframeIndex.deserialize(
                index_bytes, video_length)
            stream.video_length = video_length
        decoder = _PyAVVideoDecoder(stream, mapping)
        frames = np.stack(
            decoder._read_indexed(targets)
            if mapping is not None else [decoder[index] for index in targets]
        )
        digest = hashlib.sha256(frames.tobytes()).hexdigest()
        elapsed = time.perf_counter() - started
        return {
            "targets": targets,
            "seconds": elapsed,
            "gets": objects.gets,
            "bytes": objects.bytes,
            "index_gets": objects.index_gets,
            "index_bytes": objects.index_bytes,
            "video_gets": objects.gets - objects.index_gets,
            "video_bytes": objects.bytes - objects.index_bytes,
            "sha256": digest,
        }
    finally:
        if decoder is not None:
            decoder.close()
        stream.close()
        objects.close()


def _paired_runs(cases, video_length, expected_index, parallelism):
    runs = {"indexed": [], "unindexed": []}
    for position, targets in enumerate(cases):
        order = (
            ("unindexed", "indexed")
            if position % 2 == 0 else ("indexed", "unindexed")
        )
        pair = {}
        for mode in order:
            pair[mode] = _decode(
                mode, targets, video_length, expected_index, parallelism)
            runs[mode].append(pair[mode])
        if pair["indexed"]["sha256"] != pair["unindexed"]["sha256"]:
            raise RuntimeError("Indexed and unindexed frames differ.")
    return runs


def run(args):
    video_url = os.environ.get(VIDEO_URL_ENV)
    index_url = os.environ.get(INDEX_URL_ENV)
    if not video_url or not index_url:
        raise ValueError(
            "%s and %s must contain object URLs."
            % (VIDEO_URL_ENV, INDEX_URL_ENV)
        )
    expected_index = args.index.read_bytes()
    video_length = args.video.stat().st_size
    frames = _frame_count(args.video)
    if frames < 3:
        raise ValueError("Video must contain at least three frames.")
    if args.batch_size > frames - 2:
        raise ValueError("--batch-size exceeds the available target frames.")
    randomizer = random.Random(args.seed)
    first_cases = [
        [randomizer.randrange(1, frames - 1)]
        for unused in range(args.first_frame_runs)
    ]
    batch_cases = [
        randomizer.sample(range(1, frames - 1), args.batch_size)
        for unused in range(args.batch_runs)
    ]

    # Keep decoder and RGB conversion costs while avoiding a Torch dependency.
    _PyAVVideoDecoder._tensor = staticmethod(
        lambda frame: np.array(
            frame.to_ndarray(format="rgb24"), copy=True
        ).transpose(2, 0, 1)
    )
    first = _paired_runs(
        first_cases, video_length, expected_index, args.parallelism)
    batches = _paired_runs(
        batch_cases, video_length, expected_index, args.parallelism)
    result = {
        "contract": "pypaimon-cold-object-video-random-read-v1",
        "runtime": _runtime(args.source_commit),
        "storage_host": urlsplit(video_url).hostname,
        "service_cache": "not controlled",
        "fresh_state": "decoder, HTTP sessions, and client byte cache",
        "video_bytes": video_length,
        "video_sha256": _file_sha256(args.video),
        "frames": frames,
        "index_bytes": len(expected_index),
        "seed": args.seed,
        "parallelism": args.parallelism,
        "first_frame": {
            "indexed": _summary(first["indexed"]),
            "unindexed": _summary(first["unindexed"]),
            "raw": first,
        },
        "random_batch": {
            "batch_size": args.batch_size,
            "indexed": _summary(batches["indexed"]),
            "unindexed": _summary(batches["unindexed"]),
            "raw": batches,
        },
        "decoded_frames_equal": True,
    }
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command")

    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--video", type=Path, required=True)
    prepare_parser.add_argument("--index", type=Path, required=True)
    prepare_parser.add_argument("--output", type=Path, required=True)
    prepare_parser.add_argument("--source-commit", required=True)
    prepare_parser.add_argument("--repetitions", type=int, default=3)
    prepare_parser.set_defaults(function=prepare)

    run_parser = commands.add_parser("run")
    run_parser.add_argument("--video", type=Path, required=True)
    run_parser.add_argument("--index", type=Path, required=True)
    run_parser.add_argument("--output", type=Path, required=True)
    run_parser.add_argument("--source-commit", required=True)
    run_parser.add_argument("--first-frame-runs", type=int, default=20)
    run_parser.add_argument("--batch-runs", type=int, default=10)
    run_parser.add_argument("--batch-size", type=int, default=16)
    run_parser.add_argument("--parallelism", type=int, default=8)
    run_parser.add_argument("--seed", type=int, default=9831)
    run_parser.set_defaults(function=run)

    args = parser.parse_args()
    if not hasattr(args, "function"):
        parser.error("a command is required")
    for name in (
            "repetitions", "first_frame_runs", "batch_runs", "batch_size",
            "parallelism"):
        if hasattr(args, name) and getattr(args, name) <= 0:
            parser.error("--%s must be positive" % name.replace("_", "-"))
    args.function(args)


if __name__ == "__main__":
    main()
