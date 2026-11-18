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

import threading
import unittest
import weakref
from contextlib import ExitStack
from typing import Dict, Iterator, List, Optional, Tuple
from unittest import mock

import pyarrow as pa
import pytest

from pypaimon.execution.python.read.search import vector_search_read as search
from pypaimon.read.split import Split
from pypaimon.tests.data_evolution_test_helpers import BatchModeMixin, DataEvolutionTestBase
from pypaimon.tests.vector_search_filter_test import _field
from pypaimon.utils.range import Range


SCHEMA = pa.schema([("_ROW_ID", pa.int64()), ("embedding", pa.list_(pa.float32(), 1))])


def batch(row_id: int, value: float) -> pa.RecordBatch:
    return pa.record_batch([[row_id], [[value]]], schema=SCHEMA)


def planned_read(*sources: Iterator[pa.RecordBatch]) -> search.AbstractVectorSearchReadImpl:
    """Give each split its own Arrow reader and run the real single-query read path."""
    splits = [mock.Mock(spec=Split) for _ in sources]
    by_split: Dict[object, Iterator[pa.RecordBatch]] = dict(zip(splits, sources))
    caller = threading.get_ident()

    def open_group(group: List[Split]) -> Tuple[pa.ipc.RecordBatchReader, Iterator[pa.RecordBatch]]:
        assert threading.get_ident() == caller
        split, = group
        source = by_split[split]
        return pa.ipc.RecordBatchReader.from_batches(SCHEMA, source), source

    table_read = mock.Mock()
    table_read._new_arrow_batch_reader.side_effect = open_group
    table_read._resolve_parallelism.return_value = len(splits)
    read = search.AbstractVectorSearchReadImpl(
        mock.Mock(), 2, _field(1, "embedding", "FLOAT"), options={"metric": "inner_product"})
    read._plan_raw_read = mock.Mock(return_value=(table_read, splits))
    return read


def test_serial_failure_closes_source_before_reading_another_batch() -> None:
    closed = False

    def source() -> Iterator[pa.RecordBatch]:
        nonlocal closed
        try:
            yield batch(0, 1.0)
            raise AssertionError("Read the next batch before scoring the first")
        finally:
            closed = True

    read = planned_read(source())
    with pytest.raises(ValueError, match="dimension mismatch"):
        read._read_raw_search([Range(0, 1)], None, [1.0, 0.0])
    assert closed


def test_filters_candidates_in_bounded_blocks() -> None:
    ids = list(range(2051))
    vectors = [[float(i)] for i in ids]
    vectors[1024] = [float(len(ids))]  # The excluded row would otherwise win.
    read = planned_read(iter([pa.record_batch([ids, vectors], schema=SCHEMA)]))
    with mock.patch.object(search, "_iter_arrow_scores", wraps=search._iter_arrow_scores) as score:
        result = read._read_raw_search([Range(0, 2050)], None, [1.0], score_candidates=set(ids) - {1024})
    assert max(len(args[0]) for args, _ in score.call_args_list) <= 1024
    assert result.results().to_list() == [2049, 2050]


def test_ready_split_makes_progress_with_bounded_lookahead() -> None:
    scored = [threading.Event() for _ in range(4)]
    caller = threading.get_ident()
    original = search._iter_arrow_scores

    def slow() -> Iterator[pa.RecordBatch]:
        assert scored[2].wait(timeout=5), "A slow split blocked the ready split"
        yield batch(4, 4.0)

    def fast() -> Iterator[pa.RecordBatch]:
        for i in range(4):
            if i >= 2:
                assert scored[i - 2].is_set(), "Read more than one batch ahead"
            yield batch(i, float(i))

    def score(vectors: pa.Array, query: List[float], metric: str) -> Iterator[Optional[float]]:
        assert threading.get_ident() == caller
        value = int(vectors[0].as_py()[0])
        if value < len(scored):
            scored[value].set()
        yield from original(vectors, query, metric)

    read = planned_read(slow(), fast())
    with mock.patch.object(search, "_iter_arrow_scores", score):
        result = read._read_raw_search([Range(0, 4)], None, [1.0])
    assert result.results().to_list() == [3, 4]


def test_scoring_error_closes_both_splits() -> None:
    both_started = threading.Barrier(2)
    closed = [False, False]

    def source(split: int) -> Iterator[pa.RecordBatch]:
        try:
            both_started.wait(timeout=5)
            yield batch(split, 1.0)
            yield batch(split + 2, 2.0)
        finally:
            closed[split] = True

    read = planned_read(source(0), source(1))
    with pytest.raises(ValueError, match="dimension mismatch"):
        read._read_raw_search([Range(0, 3)], None, [1.0, 0.0])
    assert all(closed)


def test_read_error_closes_the_other_split() -> None:
    started = threading.Event()
    closed = False

    def failing() -> Iterator[pa.RecordBatch]:
        yield batch(0, 0.0)
        assert started.wait(timeout=5)
        raise OSError("split read failed")

    def other() -> Iterator[pa.RecordBatch]:
        nonlocal closed
        try:
            started.set()
            while True:
                yield batch(1, 1.0)
        finally:
            closed = True

    read = planned_read(failing(), other())
    with pytest.raises(OSError, match="split read failed"):
        read._read_raw_search([Range(0, 1)], None, [1.0])
    assert closed


def test_last_batch_can_outlive_its_reader() -> None:
    source = mock.Mock()
    reader = pa.ipc.RecordBatchReader.from_batches(SCHEMA, [batch(0, 1.0)])
    ref = weakref.ref(reader)
    table_read = mock.Mock()
    table_read._new_arrow_batch_reader.return_value = reader, source
    with ExitStack() as streams:
        batches = search._open_prefetched_batches(table_read, [], streams)
        table_read._new_arrow_batch_reader.return_value = None
        del reader
        last = next(batches)
        source.close.assert_called_once()
        assert list(batches) == []
        assert ref() is None
        assert last.column("_ROW_ID").to_pylist() == [0]


def test_submit_failure_closes_reader_before_first_read() -> None:
    reader, source = mock.Mock(), mock.Mock()
    table_read = mock.Mock()
    table_read._new_arrow_batch_reader.return_value = reader, source
    with mock.patch.object(search.ThreadPoolExecutor, "submit", side_effect=RuntimeError("submit failed")), \
            pytest.raises(RuntimeError, match="submit failed"):
        next(search._parallel_raw_batches(table_read, [mock.Mock(spec=Split)], 1))
    reader.close.assert_called_once()
    source.close.assert_called_once()


class SingleVectorParallelReadTest(BatchModeMixin, DataEvolutionTestBase, unittest.TestCase):
    pa_schema = pa.schema([("embedding", pa.list_(pa.float32())), ("pt", pa.int32())])
    table_options = {"row-tracking.enabled": "true", "data-evolution.enabled": "true", "bucket": "-1",
                     "file.format": "parquet", "global-index.search-mode": "full"}

    def test_public_search_across_partitions(self) -> None:
        table = self._create_table(partition_keys=["pt"])
        for i in range(4):
            self._write_arrow(table, pa.table({"embedding": [[i, 1]], "pt": [i]}, schema=self.pa_schema))

        def query(parallelism: int) -> Dict[int, float]:
            result = (table.copy({"read.parallelism": str(parallelism)}).new_vector_search_builder()
                      .with_vector_column("embedding").with_query_vector([1, 0]).with_limit(2).execute_local())
            return {row: result.score_getter()(row) for row in result.results()}

        expected = query(1)
        assert len(expected) == 2
        assert query(2) == expected
