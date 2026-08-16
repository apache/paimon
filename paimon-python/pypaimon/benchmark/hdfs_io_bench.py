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

"""HDFS FileIO benchmark: native (hdfs-native) vs pyarrow (libhdfs/JVM).

Compares throughput of common FileIO operations between the two backends
against the same HDFS cluster. Each backend is exercised via the FileIO
factory by toggling the `hdfs.client.impl` option.

Usage:
    python -m pypaimon.benchmark.hdfs_io_bench \\
        --warehouse hdfs://localhost:8020/bench \\
        [--backend native|pyarrow|both] \\
        [--write-size-mb 256] \\
        [--list-files 1000] \\
        [--read-iters 3]

Notes:
- `pyarrow` backend requires HADOOP_HOME + HADOOP_CONF_DIR + libhdfs.
- `native` backend requires `pip install pypaimon[hdfs]`.
- The benchmark writes/reads scratch files under <warehouse>/bench_<uuid>/
  and removes them on exit.
"""

import argparse
import os
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pypaimon.common.file_io import FileIO  # noqa: E402
from pypaimon.common.options import Options  # noqa: E402


def _build_file_io(warehouse: str, backend: str) -> FileIO:
    opts = Options({"hdfs.client.impl": backend})
    return FileIO.get(warehouse, opts)


def _human(seconds: float) -> str:
    if seconds < 1e-3:
        return f"{seconds * 1e6:.0f}us"
    if seconds < 1:
        return f"{seconds * 1e3:.1f}ms"
    return f"{seconds:.2f}s"


def _bench_write(file_io: FileIO, root: str, size_mb: int) -> float:
    payload = os.urandom(min(size_mb, 16) * 1024 * 1024)
    path = f"{root}/write-{uuid.uuid4().hex[:8]}.bin"
    t0 = time.perf_counter()
    with file_io.new_output_stream(path) as stream:
        written = 0
        target = size_mb * 1024 * 1024
        while written < target:
            chunk = payload[: min(len(payload), target - written)]
            n = stream.write(chunk)
            written += n if isinstance(n, int) and n > 0 else len(chunk)
    return time.perf_counter() - t0


def _bench_read(file_io: FileIO, path: str) -> float:
    t0 = time.perf_counter()
    with file_io.new_input_stream(path) as stream:
        while True:
            chunk = stream.read(8 * 1024 * 1024)
            if not chunk:
                break
    return time.perf_counter() - t0


def _bench_list(file_io: FileIO, root: str, num_files: int) -> float:
    scratch = f"{root}/list-{uuid.uuid4().hex[:8]}"
    file_io.mkdirs(scratch)
    try:
        for i in range(num_files):
            with file_io.new_output_stream(f"{scratch}/f-{i:06d}.txt") as s:
                s.write(b"x")
        t0 = time.perf_counter()
        results = file_io.list_status(scratch)
        _ = list(results)
        return time.perf_counter() - t0
    finally:
        file_io.delete(scratch, recursive=True)


def run_one(backend: str, args) -> None:
    print(f"\n=== backend={backend} ===")
    try:
        file_io = _build_file_io(args.warehouse, backend)
    except Exception as e:
        print(f"  init failed: {e}")
        return

    bench_root = f"{args.warehouse.rstrip('/')}/bench_{uuid.uuid4().hex[:8]}"
    file_io.mkdirs(bench_root)
    try:
        # Write
        sample_path = f"{bench_root}/write-sample.bin"
        with file_io.new_output_stream(sample_path) as stream:
            payload = os.urandom(min(args.write_size_mb, 16) * 1024 * 1024)
            written = 0
            target = args.write_size_mb * 1024 * 1024
            t0 = time.perf_counter()
            while written < target:
                chunk = payload[: min(len(payload), target - written)]
                n = stream.write(chunk)
                written += n if isinstance(n, int) and n > 0 else len(chunk)
            write_elapsed = time.perf_counter() - t0
        mb_per_s = args.write_size_mb / write_elapsed if write_elapsed else 0
        print(f"  write {args.write_size_mb}MB:  "
              f"{_human(write_elapsed)}  ({mb_per_s:.1f} MB/s)")

        # Read (warm)
        read_times = []
        for _ in range(args.read_iters):
            read_times.append(_bench_read(file_io, sample_path))
        avg_read = sum(read_times) / len(read_times)
        rmb_per_s = args.write_size_mb / avg_read if avg_read else 0
        print(f"  read  {args.write_size_mb}MB (avg of {args.read_iters}): "
              f"{_human(avg_read)}  ({rmb_per_s:.1f} MB/s)")

        # List
        list_elapsed = _bench_list(file_io, bench_root, args.list_files)
        print(f"  list  {args.list_files} files: {_human(list_elapsed)}")

    finally:
        try:
            file_io.delete(bench_root, recursive=True)
        except Exception as e:
            print(f"  cleanup failed: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", required=True,
                        help="HDFS URI under which scratch files are created")
    parser.add_argument("--backend", default="both",
                        choices=["native", "pyarrow", "both"])
    parser.add_argument("--write-size-mb", type=int, default=128)
    parser.add_argument("--list-files", type=int, default=1000)
    parser.add_argument("--read-iters", type=int, default=3)
    args = parser.parse_args()

    backends = ["native", "pyarrow"] if args.backend == "both" else [args.backend]
    for backend in backends:
        run_one(backend, args)


if __name__ == "__main__":
    main()
