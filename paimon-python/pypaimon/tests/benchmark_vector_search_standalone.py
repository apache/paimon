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

"""
Standalone benchmark for Phase 0 vector search optimizations.
No pypaimon imports needed — tests the algorithm directly.

Usage:
    python3 benchmark_vector_search_standalone.py
    python3 benchmark_vector_search_standalone.py --num-rows 100000 --dim 768
"""

import argparse
import time

import numpy as np


# ============================================================
# Original pure-Python implementation (copied from vector_search_read.py)
# ============================================================

def _compute_score_python(query, stored, metric):
    if metric == "l2":
        sum_sq = 0.0
        for q, s in zip(query, stored):
            diff = float(q) - float(s)
            sum_sq += diff * diff
        return 1.0 / (1.0 + sum_sq)
    if metric == "cosine":
        dot = 0.0
        norm_a = 0.0
        norm_b = 0.0
        for q, s in zip(query, stored):
            q = float(q)
            s = float(s)
            dot += q * s
            norm_a += q * q
            norm_b += s * s
        denominator = (norm_a ** 0.5) * (norm_b ** 0.5)
        return 0.0 if denominator == 0 else dot / denominator
    if metric == "inner_product":
        return sum(float(q) * float(s) for q, s in zip(query, stored))
    raise ValueError("Unknown metric: %s" % metric)


def raw_search_python(row_ids, vectors, query_vector, metric, limit):
    """Original pure-Python raw search with heap."""
    import heapq
    top_k_heap = []
    for row_id, stored in zip(row_ids, vectors):
        if stored is None:
            continue
        score = _compute_score_python(query_vector, stored, metric)
        entry = (score, -row_id, row_id)
        if len(top_k_heap) < limit:
            heapq.heappush(top_k_heap, entry)
        elif entry[:2] > top_k_heap[0][:2]:
            heapq.heapreplace(top_k_heap, entry)
    return {row_id: score for score, _, row_id in top_k_heap}


# ============================================================
# New numpy-vectorized implementation
# ============================================================

def raw_search_numpy(row_ids_list, vectors_list, query_vector, metric, limit):
    """Numpy-vectorized raw search."""
    # Filter nulls.
    filtered = [(rid, vec) for rid, vec in zip(row_ids_list, vectors_list)
                if vec is not None]
    if not filtered:
        return {}

    filtered_ids, filtered_vecs = zip(*filtered)
    row_id_array = np.array(filtered_ids, dtype=np.int64)
    stored_matrix = np.array(filtered_vecs, dtype=np.float32)
    query_np = np.asarray(query_vector, dtype=np.float32)

    return _numpy_distance_topk(row_id_array, stored_matrix, query_np, metric, limit)


def raw_search_numpy_fast(row_id_array, stored_matrix, query_np, metric, limit):
    """Numpy fast path: data already in numpy arrays (simulates Arrow zero-copy)."""
    return _numpy_distance_topk(row_id_array, stored_matrix, query_np, metric, limit)


def _numpy_distance_topk(row_id_array, stored_matrix, query_np, metric, limit):
    """Core: numpy distance computation + topK selection."""
    if metric == "l2":
        diffs = stored_matrix - query_np
        dists = np.sum(diffs * diffs, axis=1)
        scores = 1.0 / (1.0 + dists)
    elif metric == "cosine":
        dots = stored_matrix @ query_np
        norms = np.linalg.norm(stored_matrix, axis=1) * np.linalg.norm(query_np)
        norms = np.where(norms == 0, 1.0, norms)
        scores = dots / norms
    elif metric == "inner_product":
        scores = stored_matrix @ query_np
    else:
        raise ValueError("Unknown metric: %s" % metric)

    n = len(scores)
    if n <= limit:
        top_indices = np.argsort(-scores)
    else:
        top_indices = np.argpartition(-scores, limit)[:limit]
        top_indices = top_indices[np.argsort(-scores[top_indices])]

    return {int(row_id_array[i]): float(scores[i]) for i in top_indices}


# ============================================================
# ThreadPool simulation
# ============================================================

def benchmark_threadpool(num_shards, search_time_ms, num_runs=3):
    """Simulate ThreadPoolExecutor benefit."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    print(f"\n{'='*60}")
    print(f"ThreadPool Simulation: {num_shards} shards, {search_time_ms}ms/shard")
    print(f"{'='*60}")

    def fake_search(shard_id):
        time.sleep(search_time_ms / 1000.0)
        return shard_id

    # Serial.
    times_serial = []
    for _ in range(num_runs):
        t0 = time.perf_counter()
        _ = [fake_search(i) for i in range(num_shards)]
        times_serial.append(time.perf_counter() - t0)

    for max_workers in (8, 16, 32):
        times_parallel = []
        for _ in range(num_runs):
            t0 = time.perf_counter()
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = [pool.submit(fake_search, i) for i in range(num_shards)]
                _ = [f.result() for f in as_completed(futures)]
            times_parallel.append(time.perf_counter() - t0)

        avg_s = sum(times_serial) / num_runs
        avg_p = sum(times_parallel) / num_runs
        speedup = avg_s / avg_p if avg_p > 0 else float('inf')
        print(f"  Workers={max_workers}: serial={avg_s*1000:.0f}ms, "
              f"parallel={avg_p*1000:.0f}ms, speedup={speedup:.1f}x")


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark vector search optimizations (standalone)")
    parser.add_argument("--num-rows", type=int, default=10000)
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--num-runs", type=int, default=3)
    parser.add_argument("--num-shards", type=int, default=100)
    parser.add_argument("--search-time-ms", type=float, default=5.0)
    parser.add_argument("--skip-threadpool", action="store_true")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Raw Search Benchmark: {args.num_rows} rows, {args.dim}D, top-{args.limit}")
    print(f"{'='*60}")

    np.random.seed(42)
    query = np.random.randn(args.dim).astype(np.float32)
    stored = np.random.randn(args.num_rows, args.dim).astype(np.float32)
    row_ids = list(range(args.num_rows))
    vectors_as_lists = [stored[i].tolist() for i in range(args.num_rows)]

    for metric in ("l2", "cosine", "inner_product"):
        print(f"\n--- Metric: {metric} ---")

        # Pure Python.
        times_py = []
        for _ in range(args.num_runs):
            t0 = time.perf_counter()
            result_py = raw_search_python(
                row_ids, vectors_as_lists, query.tolist(), metric, args.limit)
            times_py.append(time.perf_counter() - t0)

        # Numpy (from Python lists — worst case for numpy path).
        times_np_list = []
        for _ in range(args.num_runs):
            t0 = time.perf_counter()
            result_np = raw_search_numpy(
                row_ids, vectors_as_lists, query, metric, args.limit)
            times_np_list.append(time.perf_counter() - t0)

        # Numpy (from pre-built numpy array — simulates Arrow fast path).
        row_id_array = np.arange(args.num_rows, dtype=np.int64)
        times_np_fast = []
        for _ in range(args.num_runs):
            t0 = time.perf_counter()
            result_fast = raw_search_numpy_fast(
                row_id_array, stored, query, metric, args.limit)
            times_np_fast.append(time.perf_counter() - t0)

        # Correctness.
        py_ids = set(result_py.keys())
        np_ids = set(result_np.keys())
        fast_ids = set(result_fast.keys())
        overlap_list = len(py_ids & np_ids) / max(len(py_ids), 1) * 100
        overlap_fast = len(py_ids & fast_ids) / max(len(py_ids), 1) * 100

        avg_py = sum(times_py) / args.num_runs * 1000
        avg_np_list = sum(times_np_list) / args.num_runs * 1000
        avg_np_fast = sum(times_np_fast) / args.num_runs * 1000
        speedup_list = avg_py / avg_np_list if avg_np_list > 0 else float('inf')
        speedup_fast = avg_py / avg_np_fast if avg_np_fast > 0 else float('inf')

        print(f"  Python loop:       {avg_py:.1f} ms")
        print(f"  Numpy (from list): {avg_np_list:.1f} ms  ({speedup_list:.1f}x)")
        print(f"  Numpy (fast path): {avg_np_fast:.1f} ms  ({speedup_fast:.1f}x)")
        print(f"  TopK overlap: list={overlap_list:.0f}%, fast={overlap_fast:.0f}%")

    if not args.skip_threadpool:
        benchmark_threadpool(args.num_shards, args.search_time_ms, args.num_runs)

    print(f"\n{'='*60}")
    print("Done.")


if __name__ == "__main__":
    main()
