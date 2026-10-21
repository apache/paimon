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

"""Attach distributed task execution to Python search readers."""

from contextlib import contextmanager
import math


@contextmanager
def closing_task_results(results):
    """Close scheduled work without replacing an existing execution failure."""
    try:
        yield results
    except BaseException:
        try:
            results.close()
        except BaseException:
            pass
        raise
    else:
        results.close()


def create_distributed_reader(reader, executor):
    executor = executor.with_concurrency(executor.concurrency)
    from pypaimon.execution.python.read.search.vector_search_read import (
        DataEvolutionVectorRead, BatchVectorSearchReadImpl,
    )
    from pypaimon.execution.python.read.search.full_text_read import DataEvolutionFullTextRead

    if isinstance(reader, DataEvolutionFullTextRead):
        if (type(reader) is not DataEvolutionFullTextRead
                or not reader._table.options.data_evolution_enabled()):
            raise ValueError("Ray full-text search supports only data-evolution tables.")
        from pypaimon.execution.python.read.search.distributed_full_text_search import DistributedFullTextRead
        return DistributedFullTextRead(reader, executor)
    if type(reader) not in (DataEvolutionVectorRead, BatchVectorSearchReadImpl):
        raise ValueError("Ray vector search supports only data-evolution tables.")
    if not reader._table.options.data_evolution_enabled():
        raise ValueError("Ray vector search supports only data-evolution tables.")
    from pypaimon.execution.python.read.search.distributed_vector_search import (
        DistributedVectorSearchRead, DistributedBatchVectorSearchRead,
    )
    if type(reader) is DataEvolutionVectorRead:
        if not all(math.isfinite(float(value)) for value in reader._query_vector):
            raise ValueError("Ray vector search requires a finite query vector.")
        return DistributedVectorSearchRead(reader, executor)
    return DistributedBatchVectorSearchRead(reader, executor)
