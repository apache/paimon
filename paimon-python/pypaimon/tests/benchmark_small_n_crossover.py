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
Benchmark: small-N crossover point for _raw_search_from_arrow vs scalar loop.

Measures end-to-end latency including Arrow table construction → result,
to find the N where numpy vectorization becomes faster than a simple Python loop.
"""
import time
import numpy as np
import pyarrow as pa


def _build_arrow_table(n_rows, dim):
    """Build an Arrow table matching the real raw-search schema."""
    row_ids = list(range(n_rows))
    vectors = np.random.randn(n_rows, dim).astype(np.float32)
    vector_lists = [v.tolist() for v in vectors]

    table = pa.table({
        "__row_id__": pa.array(row_ids, type=pa.int64()),
        "vector": pa.array(vector_lists, type=pa.list_(pa.float32())),
    })
    return table


def _build_fixed_size_arrow_table(n_rows, dim):
    """Build with FixedSizeListArray (fast path in _raw_search_from_arrow)."""
    row_ids = list(range(n_rows))
    flat = np.random.randn(n_rows * dim).astype(np.float32)

    table = pa.table({
        "__row_id__": pa.array(row_ids, type=pa.int64()),
        "vector": pa.FixedSizeListArray.from_arrays(pa.array(flat), dim),
    })
    return table


def scalar_search(arrow_table, vector_column_name, query_vector, metric, limit):
    """Pure Python loop — the original approach before numpy optimization."""
    row_ids = arrow_table.column("__row_id__").to_pylist()
    vectors = arrow_table.column(vector_column_name).to_pylist()
    query = list(query_vector)

    scores = []
    for rid, stored in zip(row_ids, vectors):
        if stored is None:
            continue
        if metric == "l2":
            dist = sum((q - s) ** 2 for q, s in zip(query, stored))
            score = 1.0 / (1.0 + dist)
        elif metric == "cosine":
            dot = sum(q * s for q, s in zip(query, stored))
            norm_q = sum(q * q for q in query) ** 0.5
            norm_s = sum(s * s for s in stored) ** 0.5
            denom = norm_q * norm_s
            score = 0.0 if denom == 0 else dot / denom
        elif metric == "inner_product":
            score = sum(q * s for q, s in zip(query, stored))
        else:
            raise ValueError(metric)
        scores.append((rid, score))

    scores.sort(key=lambda x: -x[1])
    return dict(scores[:limit])


def numpy_search(arrow_table, vector_column_name, query_vector, metric, limit):
    """Numpy path — mirrors _raw_search_from_arrow end-to-end."""
    import pyarrow.compute as pc

    row_ids_col = arrow_table.column("__row_id__")
    vectors_col = arrow_table.column(vector_column_name)

    valid_mask = pc.is_valid(vectors_col)
    if not pc.all(valid_mask).as_py():
        arrow_table = arrow_table.filter(valid_mask)
        row_ids_col = arrow_table.column("__row_id__")
        vectors_col = arrow_table.column(vector_column_name)

    row_id_array = row_ids_col.to_numpy()
    try:
        if hasattr(vectors_col, 'combine_chunks'):
            vectors_arr = vectors_col.combine_chunks()
        else:
            vectors_arr = vectors_col
        flat = vectors_arr.values
        dim = vectors_arr.type.list_size
        if dim is not None and flat is not None:
            stored_matrix = flat.to_numpy(zero_copy_only=False).reshape(-1, dim).astype(
                np.float32)
        else:
            stored_matrix = np.array(vectors_col.to_pylist(), dtype=np.float32)
    except (AttributeError, TypeError, ValueError):
        stored_matrix = np.array(vectors_col.to_pylist(), dtype=np.float32)

    query_np = np.asarray(query_vector, dtype=np.float32)

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
        raise ValueError(metric)

    n = len(scores)
    if n <= limit:
        top_indices = np.argsort(-scores)
    else:
        top_indices = np.argpartition(-scores, limit)[:limit]
        top_indices = top_indices[np.argsort(-scores[top_indices])]

    return {int(row_id_array[i]): float(scores[i]) for i in top_indices}


def bench(fn, table, warmup=3, repeats=50):
    """Benchmark a search function, return median latency in microseconds."""
    query = np.random.randn(table.column("vector")[0].as_py().__len__()).astype(np.float32)

    for _ in range(warmup):
        fn(table, "vector", query, "cosine", 10)

    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn(table, "vector", query, "cosine", 10)
        times.append((time.perf_counter() - t0) * 1e6)

    times.sort()
    return times[len(times) // 2]  # median


def main():
    row_counts = [1, 8, 32, 128, 256, 512, 1024, 2048, 4096]
    dims = [128, 768]

    print(f"{'rows':<8}{'dim':<6}{'scalar(μs)':<14}{'numpy_var(μs)':<16}"
          f"{'numpy_fix(μs)':<16}{'winner':<12}{'speedup':<10}")
    print("-" * 82)

    for dim in dims:
        for n_rows in row_counts:
            table_var = _build_arrow_table(n_rows, dim)
            table_fix = _build_fixed_size_arrow_table(n_rows, dim)

            t_scalar = bench(scalar_search, table_var)
            t_numpy_var = bench(numpy_search, table_var)
            t_numpy_fix = bench(numpy_search, table_fix)

            t_numpy_best = min(t_numpy_var, t_numpy_fix)
            if t_scalar < t_numpy_best:
                winner = "scalar"
                speedup = t_numpy_best / t_scalar
            else:
                winner = "numpy"
                speedup = t_scalar / t_numpy_best

            print(f"{n_rows:<8}{dim:<6}{t_scalar:<14.1f}{t_numpy_var:<16.1f}"
                  f"{t_numpy_fix:<16.1f}{winner:<12}{speedup:<10.2f}x")
        print()


if __name__ == "__main__":
    main()
