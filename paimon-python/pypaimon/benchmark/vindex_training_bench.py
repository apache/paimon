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

"""Benchmark complete vector writer builds in isolated processes.

python -m pypaimon.benchmark.vindex_training_bench --output /tmp/training.json
Requires pypaimon[vindex]. Compares full-file one-shot training, bounded reads
into a complete sample matrix, and streaming native training. Source reads
and ingestion are bounded and identical in all variants. Reports process
peak RSS, ingestion/finish time and an index digest for result equivalence.
"""

import argparse
import hashlib
import json
import math
import os
import platform
import resource
import subprocess
import sys
import tempfile
import time
from importlib.metadata import version

import numpy as np

from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.globalindex.vindex.vindex_vector_index_writer import (
    VindexVectorIndexWriter, _iter_training_batches,
)
from pypaimon.schema.data_types import ArrayType, AtomicType


def one_shot(writer, np, trainer_type, bounded):
    count = max(1, math.ceil(writer._vector_count * writer._train_sample_ratio))
    if bounded:
        sample = np.empty((count, writer._dimension), dtype=np.float32)
        position = 0
        with open(writer._vector_temp_path, "rb") as stream:
            for batch in _iter_training_batches(
                np, stream, writer._vector_count, writer._dimension,
                writer._train_sample_ratio,
            ):
                sample[position:position + len(batch)] = batch
                position += len(batch)
    else:
        vectors = np.fromfile(writer._vector_temp_path, dtype=np.float32).reshape(
            writer._vector_count, writer._dimension)
        sample = (vectors if count == len(vectors) else
                  vectors[np.arange(count) * len(vectors) // count])
    return trainer_type.train(writer._training_options(), sample)


def worker(args):
    with tempfile.TemporaryDirectory(prefix="paimon-stream-train-") as directory:
        options = {args.index_type + ".dimension": str(args.dimension),
                   args.index_type + ".train.sample-ratio": str(args.ratio)}
        if args.index_type != "diskann":
            options[args.index_type + ".nlist"] = "64"
        writer = VindexVectorIndexWriter(
            LocalFileIO(), directory, ArrayType(True, AtomicType("FLOAT")),
            args.index_type, options, "embedding")
        if args.mode != "streaming":
            writer._train = lambda np, trainer: one_shot(
                writer, np, trainer, args.mode == "sample-matrix")
        start = time.perf_counter()
        try:
            row_id = 0
            with open(args.source, "rb") as stream:
                while True:
                    batch = np.fromfile(stream, dtype=np.float32, count=10000 * args.dimension)
                    if not batch.size:
                        break
                    for vector in batch.reshape(-1, args.dimension):
                        writer.write(vector, row_id)
                        row_id += 1
            ingestion_s = time.perf_counter() - start
            start_finish = time.perf_counter()
            writer.finish()
            finish_s = time.perf_counter() - start_finish
            peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            peak_mib = peak / (1024 ** 2 if sys.platform == "darwin" else 1024)
            digest = hashlib.sha256()
            with open(writer._file_path(), "rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
        finally:
            writer.close()
        return {"mode": args.mode, "ratio": args.ratio, "ingestion_s": ingestion_s,
                "finish_s": finish_s, "total_s": ingestion_s + finish_s,
                "peak_rss_mib": peak_mib, "sha256": digest.hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200000)
    parser.add_argument("--dimension", type=int, default=128)
    parser.add_argument("--index-type", default="ivf-flat")
    parser.add_argument("--ratios", type=float, nargs="+", default=[1.0, 0.1])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output")
    parser.add_argument("--source", help=argparse.SUPPRESS)
    parser.add_argument("--mode", help=argparse.SUPPRESS)
    parser.add_argument("--ratio", type=float, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.mode:
        print(json.dumps(worker(args)))
        return
    if not args.output:
        parser.error("--output is required")
    records = []
    with tempfile.TemporaryDirectory(prefix="paimon-train-source-") as directory:
        source = os.path.join(directory, "vectors")
        rng = np.random.default_rng(42)
        with open(source, "wb") as stream:
            for start in range(0, args.rows, 10000):
                rng.standard_normal((min(10000, args.rows - start), args.dimension)).astype(
                    np.float32).tofile(stream)
        for ratio in args.ratios:
            expected = None
            for _ in range(args.repeats):
                for mode in ("baseline", "sample-matrix", "streaming"):
                    process = subprocess.run(
                        [sys.executable, "-m", "pypaimon.benchmark.vindex_training_bench",
                         "--source", source, "--mode", mode, "--ratio", str(ratio),
                         "--dimension", str(args.dimension), "--index-type", args.index_type],
                        check=True, capture_output=True, text=True)
                    record = json.loads(process.stdout)
                    if expected is None:
                        expected = record["sha256"]
                    assert record["sha256"] == expected, "Index bytes changed"
                    records.append(record)
                    print(json.dumps(record), flush=True)
    with open(args.output, "w") as output:
        json.dump({"platform": platform.platform(), "python": platform.python_version(),
                   "paimon_vindex": version("paimon-vindex"), "parameters": vars(args),
                   "records": records}, output, indent=2)


if __name__ == "__main__":
    main()
