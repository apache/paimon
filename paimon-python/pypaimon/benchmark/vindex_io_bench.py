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

"""Compare serial and concurrent vindex I/O with local or injected read latency.

Run with pypaimon[vindex] installed:
    python -m pypaimon.benchmark.vindex_io_bench --output /tmp/vindex-io.json

Latency is simulated per positional read; this is not an S3 benchmark.
The native search measurement includes reader open, initialization, search and close.
Index construction and correctness assertions are outside the timed region.
"""

import argparse
from importlib.metadata import version
import json
import os
import platform
import tempfile
import threading
import time
from unittest import mock

import numpy as np

from pypaimon.common.file_io import pread
from pypaimon.globalindex.batch_vector_search import BatchVectorSearch
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.vindex import vindex_vector_global_index_reader as adapter


class MeasuredStream:
    def __init__(self, stream, delay):
        self.stream = stream
        self.delay = delay
        self.lock = threading.Lock()
        self.calls = self.bytes = self.active = self.peak = 0

    def read_at(self, length, offset):
        with self.lock:
            self.calls += 1
            self.active += 1
            self.peak = max(self.peak, self.active)
        try:
            if self.delay:
                time.sleep(self.delay)
            data = pread(self.stream, length, offset)
            with self.lock:
                self.bytes += len(data)
            return data
        finally:
            with self.lock:
                self.active -= 1

    def close(self):
        self.stream.close()


class SerialInput:
    """Original position-read path, used as the baseline."""
    def __init__(self, stream, parallelism=1):
        self.stream = stream

    def pread_many(self, ranges):
        return [pread(self.stream, length, offset) for offset, length in ranges]

    def close(self):
        pass


class MeasuredFileIO:
    def __init__(self, delay):
        self.delay = delay
        self.streams = []

    def new_input_stream(self, path):
        stream = MeasuredStream(open(path, "rb"), self.delay)
        self.streams.append(stream)
        return stream


def summary(times, streams):
    return {
        "p50_ms": float(np.percentile(times, 50) * 1000),
        "p95_ms": float(np.percentile(times, 95) * 1000),
        "reads_per_iteration": sum(s.calls for s in streams) / len(times),
        "bytes_per_iteration": sum(s.bytes for s in streams) / len(times),
        "peak_concurrent_reads": max(s.peak for s in streams),
    }


def signature(results):
    return [sorted((row_id, result.score_getter()(row_id))
                   for row_id in result.results().to_list()) for result in results]


def run(args, directory):
    from paimon_vindex import VectorIndexTrainer, VectorIndexWriter

    rng = np.random.default_rng(42)
    vectors = rng.standard_normal((args.rows, args.dimension)).astype(np.float32)
    queries = rng.standard_normal((args.batch_size, args.dimension)).astype(np.float32)
    path = os.path.join(directory, "index")
    options = {"index.type": args.index_type, "metric": "l2"}
    if args.index_type.startswith("ivf_"):
        options["nlist"] = "64"
    with VectorIndexTrainer.train(options, vectors) as training:
        with VectorIndexWriter(training) as writer:
            writer.add_vectors(np.arange(args.rows, dtype=np.int64), vectors)
            with open(path, "wb") as output:
                writer.write(output)
    with open(path, "rb") as stream:
        payload = stream.read()
    ranges = [(i * 4096, 4096) for i in range(args.range_count)]
    if len(payload) < args.range_count * 4096:
        raise ValueError("Index too small for requested microbenchmark ranges")
    expected_chunks = [payload[o:o + n] for o, n in ranges]
    query = BatchVectorSearch(vectors=queries.tolist(), limit=10, field_name="embedding",
                              options=({"diskann.l_search": "100"} if args.index_type == "diskann"
                                       else {"ivf.nprobe": "16"}))
    records = []
    input_class = adapter.PaimonVindexInput
    for delay_ms in args.latency_ms:
        # DiskANN can choose a different read plan from the header-read latency.
        expected = None
        for parallelism in [0] + args.parallelism:
            cls = SerialInput if parallelism == 0 else input_class
            label = "baseline" if parallelism == 0 else str(parallelism)
            delay = delay_ms / 1000
            stream = MeasuredStream(open(path, "rb"), delay)
            input_ = cls(stream, max(1, parallelism))
            try:
                # Warm the reusable executor; native timings below include cold startup.
                assert input_.pread_many(ranges) == expected_chunks
                stream.calls = stream.bytes = stream.peak = 0
                times = []
                for _ in range(args.iterations):
                    start = time.perf_counter()
                    chunks = input_.pread_many(ranges)
                    times.append(time.perf_counter() - start)
                    assert chunks == expected_chunks
                micro = summary(times, [stream])
            finally:
                input_.close()
                stream.close()
            file_io = MeasuredFileIO(delay)
            times = []
            with mock.patch.object(adapter, "PaimonVindexInput", cls):
                for _ in range(args.iterations):
                    reader = adapter.VindexVectorGlobalIndexReader(
                        file_io, directory,
                        [GlobalIndexIOMeta(file_name="index", file_size=len(payload))],
                        options={"vindex.read.parallelism": str(max(1, parallelism))})
                    start = time.perf_counter()
                    try:
                        results = reader.visit_batch_vector_search(query).result()
                    finally:
                        reader.close()
                    times.append(time.perf_counter() - start)
                    actual = signature(results)
                    if expected is None:
                        expected = actual
                    assert actual == expected, "Native row IDs or scores changed"
            record = {"latency_ms": delay_ms, "parallelism": label,
                      "ranges": micro, "native_search": summary(times, file_io.streams)}
            records.append(record)
            print(json.dumps(record), flush=True)
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-type", default="ivf_flat",
                        choices=["ivf_flat", "ivf_pq", "ivf_sq", "ivf_rq", "diskann"])
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--latency-ms", type=float, nargs="+", default=[0, 2, 10])
    parser.add_argument("--parallelism", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--range-count", type=int, default=32)
    parser.add_argument("--rows", type=int, default=16384)
    parser.add_argument("--dimension", type=int, default=64)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="paimon-vindex-io-") as directory:
        records = run(args, directory)
    report = {"platform": platform.platform(), "python": platform.python_version(),
              "paimon_vindex": version("paimon-vindex"), "cpu_count": os.cpu_count(),
              "rayon_num_threads": os.environ.get("RAYON_NUM_THREADS"),
              "parameters": vars(args), "records": records}
    with open(args.output, "w") as output:
        json.dump(report, output, indent=2)


if __name__ == "__main__":
    main()
