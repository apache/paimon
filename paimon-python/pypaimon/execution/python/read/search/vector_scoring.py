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

"""Scalar and NumPy scoring for exact vector search."""

from typing import TYPE_CHECKING, List, Optional, Sequence, Union

import numpy as np

if TYPE_CHECKING:
    from numpy.typing import NDArray

    RealArray = NDArray[Union[np.integer, np.floating]]
    Float64Array = NDArray[np.float64]
else:
    # numpy.typing.NDArray was added after the minimum supported NumPy 1.19.
    RealArray = Float64Array = np.ndarray

Vector = Union[Sequence[float], RealArray]


class _PreparedQuery:
    """Query-side operands for one exact search metric, shared by scoring blocks."""

    def __init__(self, values: Vector, metric: str) -> None:
        self.metric = metric
        self.values = values
        self.array: Optional[Float64Array] = None
        self.cosine_norm: Optional[float] = None
        try:
            array = np.asarray(values, dtype=np.float64)
        except (ValueError, TypeError, OverflowError):
            return
        if array.ndim != 1 or not array.size:
            return
        if metric == "cosine":
            norm2 = 0.0
            for value in array:
                x = float(value)
                norm2 += x * x
            self.cosine_norm = norm2**0.5
        if np.isfinite(array).all():
            self.array = array

    def __len__(self) -> int:
        return len(self.values)


def _compute_scores(
    query: _PreparedQuery, matrix: Float64Array
) -> Optional[List[float]]:
    """Score an owned float64 matrix, or request scalar fallback with None."""
    metric = query.metric
    if (
        query.array is None
        or matrix.ndim != 2
        or matrix.shape[1] != len(query)
        or not np.isfinite(matrix).all()
        or metric not in ("l2", "cosine", "inner_product")
    ):
        return None
    with np.errstate(over="ignore", under="ignore", invalid="ignore", divide="ignore"):
        if metric == "l2":
            np.subtract(query.array, matrix, out=matrix)
            np.square(matrix, out=matrix)
            np.add.accumulate(matrix, axis=1, out=matrix)
            return (1.0 / (1.0 + matrix[:, -1])).tolist()
        if metric == "cosine":
            norms = np.square(matrix)
            np.add.accumulate(norms, axis=1, out=norms)
            stored_norms = norms[:, -1].copy()
            del norms
        np.multiply(matrix, query.array, out=matrix)
        if metric == "inner_product":
            return [sum(row) for row in matrix.tolist()]
        assert query.cosine_norm is not None
        matrix[:, 0] += 0.0  # Match a scalar accumulator initialized to +0.0.
        np.add.accumulate(matrix, axis=1, out=matrix)
        denominators = [query.cosine_norm * float(norm) ** 0.5 for norm in stored_norms]
        return [0.0 if denominator == 0 else float(dot) / denominator
                for dot, denominator in zip(matrix[:, -1], denominators)]


def _compute_score(prepared: _PreparedQuery, stored: Vector) -> float:
    query = prepared.values
    metric = prepared.metric
    if metric == "l2":
        sum_sq = 0.0
        for q, s in zip(query, stored):
            diff = float(q) - float(s)
            sum_sq += diff * diff
        return 1.0 / (1.0 + sum_sq)
    if metric == "cosine":
        dot = 0.0
        norm_b = 0.0
        for q, s in zip(query, stored):
            q = float(q)
            s = float(s)
            dot += q * s
            norm_b += s * s
        query_norm = prepared.cosine_norm
        if query_norm is None:
            # Irregular input remains subject to the scalar conversion rules.
            norm2 = 0.0
            for value in query:
                norm2 += float(value) * float(value)
            query_norm = norm2**0.5
        denominator = query_norm * (norm_b**0.5)
        return 0.0 if denominator == 0 else dot / denominator
    if metric == "inner_product":
        return sum(float(q) * float(s) for q, s in zip(query, stored))
    raise ValueError(f"Unknown vector search metric: {metric}")
