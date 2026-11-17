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

"""Distributed vector reader selection without a Ray runtime or index files."""

from types import SimpleNamespace

import pytest

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.execution.python.read.search.distributed_primary_key_vector_search import (
    DistributedBatchPrimaryKeyVectorRead, DistributedPrimaryKeyVectorRead,
)
from pypaimon.execution.python.read.search.distributed_search import create_distributed_reader
from pypaimon.execution.python.read.search.distributed_vector_search import (
    DistributedBatchVectorSearchRead, DistributedVectorSearchRead,
)
from pypaimon.execution.python.read.search.primary_key_batch_vector_read import BatchPrimaryKeyVectorRead
from pypaimon.execution.python.read.search.primary_key_vector_read import PrimaryKeyVectorRead
from pypaimon.execution.python.read.search.vector_search_read import (
    BatchVectorSearchReadImpl, DataEvolutionVectorRead,
)
from pypaimon.table.source.search_types import PrimaryKeyVectorScanPlan, VectorSearchScanPlan


VECTOR_READERS = [
    DataEvolutionVectorRead, BatchVectorSearchReadImpl,
    PrimaryKeyVectorRead, BatchPrimaryKeyVectorRead,
]
SUPPORTED_READERS = [
    (DataEvolutionVectorRead, DistributedVectorSearchRead),
    (BatchVectorSearchReadImpl, DistributedBatchVectorSearchRead),
    (PrimaryKeyVectorRead, DistributedPrimaryKeyVectorRead),
    (BatchPrimaryKeyVectorRead, DistributedBatchPrimaryKeyVectorRead),
]


class RecordingExecutor:
    """Expose query views and fail if selection tries to schedule work."""

    def __init__(self, concurrency=3, parent=None):
        self.concurrency = concurrency
        self.views = [] if parent is None else parent.views
        self.calls = {"map": 0} if parent is None else parent.calls

    def with_concurrency(self, concurrency):
        view = RecordingExecutor(concurrency, self)
        self.views.append(view)
        return view

    def map(self, tasks, ordered=False):
        self.calls["map"] += 1
        raise AssertionError("Reader selection must not schedule tasks.")


def _reader(reader_type, evolution=None):
    primary_key = issubclass(reader_type, PrimaryKeyVectorRead)
    evolution = not primary_key if evolution is None else evolution
    options = {"data-evolution.enabled": str(evolution).lower()}
    table = SimpleNamespace(
        is_primary_key_table=primary_key,
        options=CoreOptions(Options(options)),
        table_schema=SimpleNamespace(options=options),
    )
    column = SimpleNamespace(name="embedding")
    queries = ([[1., 0.], [0., 1.]] if issubclass(
        reader_type, (BatchVectorSearchReadImpl, BatchPrimaryKeyVectorRead)) else [1., 0.])
    return reader_type(table, 2, column, queries, options={"ivf.refine_factor": "2"})


@pytest.mark.parametrize("reader_type,distributed_type", SUPPORTED_READERS)
def test_supported_reader_selects_its_distributed_execution(reader_type, distributed_type):
    reader = _reader(reader_type)
    executor = RecordingExecutor()

    distributed = create_distributed_reader(reader, executor)

    assert type(distributed) is distributed_type
    assert distributed._table is reader._table
    assert distributed._limit == reader._limit
    assert distributed._executor is executor.views[0]
    assert distributed._executor.concurrency == executor.concurrency
    assert executor.calls["map"] == 0


@pytest.mark.parametrize("reader_type", [
    DataEvolutionVectorRead, BatchVectorSearchReadImpl,
])
def test_data_evolution_reader_rejects_plain_append_table(reader_type):
    executor = RecordingExecutor()

    with pytest.raises((ValueError, NotImplementedError)):
        create_distributed_reader(_reader(reader_type, evolution=False), executor)

    assert executor.calls["map"] == 0


@pytest.mark.parametrize("reader_type", VECTOR_READERS)
@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_queries_fail_before_task_submission(reader_type, value):
    reader = _reader(reader_type)
    executor = RecordingExecutor()
    if hasattr(reader, "_query_vectors"):
        # Include a valid first query so every batch entry must be validated.
        reader._query_vectors[1][0] = value
        read = reader.read_batch_plan
    else:
        reader._query_vector[0] = value
        read = reader.read_plan
    plan = (PrimaryKeyVectorScanPlan(0, []) if reader._table.is_primary_key_table
            else VectorSearchScanPlan([]))

    with pytest.raises(ValueError, match="finite"):
        read(plan, executor=executor)

    assert executor.calls["map"] == 0


@pytest.mark.parametrize("reader_type,_", SUPPORTED_READERS)
def test_custom_subclass_must_explicitly_opt_into_distributed_execution(reader_type, _):
    class CustomReader(reader_type):
        pass

    executor = RecordingExecutor()

    with pytest.raises((ValueError, NotImplementedError)):
        create_distributed_reader(_reader(CustomReader), executor)

    assert executor.calls["map"] == 0


def test_custom_reader_hook_preserves_its_execution_semantics():
    result = object()

    class CustomReader(DataEvolutionVectorRead):
        def _new_distributed_reader(self, executor):
            self.selected_executor = executor
            return self

        def _read(self, splits, snapshot):
            return result

    reader = _reader(CustomReader)
    executor = RecordingExecutor()

    assert reader.read_plan(VectorSearchScanPlan([]), executor=executor) is result
    assert reader.selected_executor is executor.views[0]
    assert executor.calls["map"] == 0


def test_reusing_executor_creates_independent_query_views():
    reader = _reader(PrimaryKeyVectorRead)
    executor = RecordingExecutor()

    first = create_distributed_reader(reader, executor)
    second = create_distributed_reader(reader, executor)

    assert first._executor is executor.views[0]
    assert second._executor is executor.views[1]
    assert first._executor is not second._executor
    assert first._executor is not executor
    assert second._executor is not executor
    assert executor.calls["map"] == 0
