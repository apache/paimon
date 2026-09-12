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

"""Compare Parquet vector fallback reads, conversion, scoring and Top-K.

python -m pypaimon.benchmark.vector_scoring_bench --output /tmp/scoring.json
Each variant runs in a fresh process. The blocked-scalar ablation keeps the
new bounded conversion but disables vectorized arithmetic. Timings include
Parquet reading and Top-K, but exclude Paimon manifest planning and ANN search.
"""

import argparse
import gc
import hashlib
import json
import os
import platform
import resource
import struct
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon.table.source import vector_search_read as scoring
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range


def baseline(reader, ranges, query):
    table = reader._read_raw_arrow(ranges, True, None)
    ids = table.column(SpecialFields.ROW_ID.name).to_pylist()
    vectors = table.column("embedding").to_pylist()
    heap = []
    for row_id, vector in zip(ids, vectors):
        if vector is None:
            continue
        vector = scoring._to_vector_list(vector)
        scoring._check_vector_dimension(query, vector)
        scoring._offer_score(heap, reader._limit, row_id,
                             scoring._compute_score(query, vector, reader._options["metric"]))
    return scoring._scored_result(heap)


def worker(args):
    field = SimpleNamespace(name="embedding")
    table = SimpleNamespace(table_schema=SimpleNamespace(options={}), fields=[field])
    reader = scoring.DataEvolutionVectorRead(table, 10, field, [], options={"metric": args.metric})
    reader._read_raw_arrow = lambda *a: pq.read_table(args.source)
    queries = np.random.default_rng(123).standard_normal((args.queries, args.dimension)).tolist()
    # Initialize Arrow's read machinery before timing; data remains file-backed input.
    warmup = pq.read_table(args.source)
    del warmup
    gc.collect()
    digest = hashlib.sha256()
    times = []
    ranges = [Range(0, args.rows - 1)]
    if args.mode == "blocked-scalar":
        patch = mock.patch.object(scoring, "_compute_scores", lambda *args: None)
    else:
        patch = mock.patch.object(scoring, "_compute_scores", scoring._compute_scores)
    with patch:
        for query in queries:
            start = time.perf_counter()
            if args.mode == "baseline":
                result = baseline(reader, ranges, query)
            else:
                result = reader._read_raw_search(ranges, None, query)
            times.append(time.perf_counter() - start)
            getter = result.score_getter()
            for row_id in result.results():
                digest.update(struct.pack(">qd", row_id, getter(row_id)))
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {"mode": args.mode, "metric": args.metric, "seconds": sum(times),
            "per_query_ms": [t * 1000 for t in times],
            "peak_rss_mib": peak / (1024 ** 2 if sys.platform == "darwin" else 1024),
            "result_sha256": digest.hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=8192)
    parser.add_argument("--dimension", type=int, default=384)
    parser.add_argument("--queries", type=int, default=4)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--metrics", nargs="+", default=["l2", "cosine", "inner_product"])
    parser.add_argument("--output")
    parser.add_argument("--source", help=argparse.SUPPRESS)
    parser.add_argument("--mode", help=argparse.SUPPRESS)
    parser.add_argument("--metric", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.mode:
        print(json.dumps(worker(args)))
        return
    if not args.output:
        parser.error("--output is required")
    records = []
    with tempfile.TemporaryDirectory(prefix="paimon-scoring-") as directory:
        path = os.path.join(directory, "vectors.parquet")
        vectors = np.random.default_rng(42).standard_normal(
            (args.rows, args.dimension)).astype(np.float32)
        table = pa.table({SpecialFields.ROW_ID.name: np.arange(args.rows, dtype=np.int64),
                          "embedding": pa.FixedSizeListArray.from_arrays(
                              pa.array(vectors.ravel()), args.dimension)})
        pq.write_table(table, path, compression="zstd", row_group_size=1024)
        del table, vectors
        for metric in args.metrics:
            expected = None
            for _ in range(args.repeats):
                for mode in ("baseline", "blocked-scalar", "vectorized"):
                    process = subprocess.run(
                        [sys.executable, "-m", "pypaimon.benchmark.vector_scoring_bench",
                         "--source", path, "--mode", mode, "--metric", metric,
                         "--rows", str(args.rows), "--dimension", str(args.dimension),
                         "--queries", str(args.queries)], check=True, stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE, universal_newlines=True)
                    record = json.loads(process.stdout)
                    if expected is None:
                        expected = record["result_sha256"]
                    assert record["result_sha256"] == expected, "Top-K IDs or score bits changed"
                    records.append(record)
                    print(json.dumps(record), flush=True)
    with open(args.output, "w") as output:
        json.dump({"platform": platform.platform(), "python": platform.python_version(),
                   "numpy": np.__version__, "pyarrow": pa.__version__,
                   "parameters": vars(args), "records": records}, output, indent=2)


if __name__ == "__main__":
    main()
