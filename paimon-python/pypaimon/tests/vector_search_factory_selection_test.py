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

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.python.read.search.primary_key_batch_vector_read import BatchPrimaryKeyVectorRead
from pypaimon.execution.python.read.search.primary_key_vector_read import PrimaryKeyVectorRead
from pypaimon.execution.python.read.search.primary_key_vector_scan import PrimaryKeyVectorScan
from pypaimon.execution.python.read.search.vector_search_read import (
    BatchVectorSearchReadImpl, DataEvolutionVectorRead,
)
from pypaimon.execution.python.read.search.vector_search_scan import DataEvolutionVectorScan


ARROW_SCHEMA = pa.schema([
    ("id", pa.int64()), ("pt", pa.int32()),
    ("embedding", pa.list_(pa.float32(), 2)), ("other", pa.list_(pa.float32(), 2)),
])


@pytest.fixture(params=[False, True], ids=["data-evolution", "primary-key"])
def table(tmp_path, request):
    options = {
        "file.format": "parquet", "read.native.enabled": "false", "write.native.enabled": "false",
        "scan.native-plan.enabled": "false", "vector-index.search-mode": "full",
    }
    if request.param:
        options.update({"bucket": "2", "pk-vector.index.columns": "embedding",
                        "fields.embedding.pk-vector.index.type": "ivf-flat"})
    else:
        options.update({"row-tracking.enabled": "true", "data-evolution.enabled": "true"})
    catalog = CatalogFactory.create({"warehouse": str(tmp_path)})
    catalog.create_database("default", True)
    catalog.create_table("default.vectors", Schema.from_pyarrow_schema(
        ARROW_SCHEMA, partition_keys=["pt"], primary_keys=["id", "pt"] if request.param else [],
        options=options), False)
    return catalog.get_table("default.vectors")


def _builders(table, column="embedding"):
    single = (table.new_vector_search_builder().with_vector_column(column)
              .with_query_vector([1., 0.]).with_limit(2))
    batch = (table.new_batch_vector_search_builder().with_vector_column(column)
             .with_query_vectors([[1., 0.], [0., 1.]]).with_limit(2))
    return single, batch


@pytest.mark.parametrize("column", ["embedding", "other"])
def test_public_builders_select_matching_scan_and_read_types(table, column):
    single, batch = _builders(table, column)
    uses_pk_index = table.is_primary_key_table and column == "embedding"
    scan_type, read_type, batch_type = (
        (PrimaryKeyVectorScan, PrimaryKeyVectorRead, BatchPrimaryKeyVectorRead) if uses_pk_index else
        (DataEvolutionVectorScan, DataEvolutionVectorRead, BatchVectorSearchReadImpl))
    assert type(single.new_vector_search_scan()) is scan_type
    assert type(batch.new_vector_search_scan()) is scan_type
    assert type(single.new_vector_search_read()) is read_type
    assert type(batch.new_batch_vector_search_read()) is batch_type


def test_public_builder_parameters_are_captured_independently(table):
    single, batch = _builders(table)
    queries = [[1., 0.], [0., 1.]]
    single.with_query_vector(queries[0])
    batch.with_query_vectors(queries)
    predicates = table.new_read_builder().new_predicate_builder()
    row_filter = predicates.greater_or_equal("id", 2)
    scans, readers = [], []
    for builder in (single, batch):
        builder.with_filter(row_filter).with_partition_filter(predicates.equal("pt", 1))
        builder.with_options({"ivf.nprobe": "2", "metric": "l2"})
        scans.append(builder.new_vector_search_scan())
    readers.append(single.new_vector_search_read())
    readers.append(batch.new_batch_vector_search_read())
    for builder in (single, batch):
        builder.with_limit(99).with_vector_column("other").with_option("ivf.nprobe", "8")
        builder.with_filter(predicates.greater_or_equal("id", 3))
    queries[0][0] = 99.
    queries.append([9., 9.])

    for execution in scans + readers:
        assert execution._table is table
        assert execution._vector_column.name == "embedding"
        assert execution._filter == row_filter
        assert execution._partition_filter.field == "pt"
        assert execution._partition_filter.index == 0
        assert execution._options == {"ivf.nprobe": "2", "metric": "l2"}
    assert [reader._limit for reader in readers] == [2, 2]
    assert readers[0]._query_vector == [1., 0.]
    assert readers[1]._query_vectors == [[1., 0.], [0., 1.]]


def _append(table, row_id):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist([
            {"id": row_id, "pt": 1, "embedding": [float(row_id), 0.], "other": [0., 1.]},
        ], schema=ARROW_SCHEMA))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def test_public_builders_preserve_the_selected_snapshot(table):
    _append(table, 1)
    snapshot = table.snapshot_manager().get_latest_snapshot()
    historical = table.copy({"scan.snapshot-id": str(snapshot.id)})
    single, batch = _builders(historical)
    scans = [single.new_vector_search_scan(), batch.new_vector_search_scan()]
    readers = [single.new_vector_search_read(), batch.new_batch_vector_search_read()]
    _append(table, 2)

    assert table.snapshot_manager().get_latest_snapshot().id > snapshot.id
    for scan, reader in zip(scans, readers):
        assert scan.scan().snapshot().id == snapshot.id
        assert reader._table is historical
        assert reader._table.options.options.to_map()["scan.snapshot-id"] == str(snapshot.id)
