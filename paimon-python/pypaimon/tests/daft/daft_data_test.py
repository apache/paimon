################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Tests for _read_table() — reads from a local Paimon filesystem catalog.

All tests run without Docker or external services; pypaimon is used directly
to create and populate test tables, then _read_table() is validated.
"""

from __future__ import annotations

import asyncio
import decimal

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from daft import col, lit

from pypaimon.daft.daft_paimon import _read_table
from pypaimon.daft.daft_predicate_visitor import convert_filters_to_paimon


def _contains_expr(py_expr):
    from daft.expressions import Expression

    expr_text = str(Expression._from_pyexpr(py_expr))
    return "contains" in expr_text


def _predicate_leaves(predicate):
    if predicate is None:
        return []
    if predicate.method in ("and", "or"):
        result = []
        for child in predicate.literals:
            result.extend(_predicate_leaves(child))
        return result
    return [(predicate.method, predicate.field, tuple(predicate.literals or []))]


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


class _UnserializableFileIoMarker:
    def __reduce__(self):
        raise TypeError("file io marker should not be serialized")


class _UnserializableStorageConfig:
    multithreaded_io = False

    def __reduce__(self):
        raise TypeError("storage config marker should not be serialized")


def _write_to_paimon(table, arrow_table, mode="append", overwrite_partition=None):
    write_builder = table.new_batch_write_builder()
    if mode == "overwrite":
        write_builder.overwrite(overwrite_partition or {})
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()
    try:
        table_write.write_arrow(arrow_table)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
    finally:
        table_write.close()
        table_commit.close()


async def _collect_paimon_source_batches(source, pushdowns):
    batches = []
    fallback_task_count = 0
    async for task in source.get_tasks(pushdowns):
        if type(task).__name__ == "_PaimonPKSplitTask":
            fallback_task_count += 1
        async for batch in task.read():
            batches.append(batch.to_pydict())
    assert fallback_task_count > 0
    return batches


async def _read_paimon_source_batches(
    table,
    filter_expr=None,
    columns=None,
    limit=None,
    call_push_filters=True,
):
    from daft import context, runners
    from daft.daft import StorageConfig
    from daft.io.pushdowns import Pushdowns

    from pypaimon.daft.daft_datasource import PaimonDataSource

    io_config = context.get_context().daft_planning_config.default_io_config
    storage_config = StorageConfig(runners.get_or_create_runner().name != "ray", io_config)
    source = PaimonDataSource(table, storage_config=storage_config, catalog_options={})

    if filter_expr is not None and call_push_filters:
        pushed_filters, remaining_filters = source.push_filters([filter_expr._expr])
        assert pushed_filters
        assert not remaining_filters

    pushdowns = Pushdowns(filters=filter_expr, columns=columns, limit=limit)
    return await _collect_paimon_source_batches(source, pushdowns)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def local_paimon_catalog(tmp_path):
    catalog = pypaimon.CatalogFactory.create({"warehouse": str(tmp_path)})
    catalog.create_database("test_db", ignore_if_exists=True)
    return catalog, tmp_path


@pytest.fixture
def append_only_table(local_paimon_catalog):
    catalog, tmp_path = local_paimon_catalog
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64()),
            pa.field("dt", pa.string()),
        ]),
        partition_keys=["dt"],
        options={"bucket": "1", "file.format": "parquet"},
    )
    catalog.create_table("test_db.append_table", schema, ignore_if_exists=False)
    return catalog.get_table("test_db.append_table"), tmp_path


@pytest.fixture
def pk_table(local_paimon_catalog):
    catalog, tmp_path = local_paimon_catalog
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("dt", pa.string()),
        ]),
        partition_keys=["dt"],
        primary_keys=["id", "dt"],
        options={"bucket": "1", "file.format": "parquet"},
    )
    catalog.create_table("test_db.pk_table", schema, ignore_if_exists=False)
    return catalog.get_table("test_db.pk_table"), tmp_path


# ---------------------------------------------------------------------------
# Basic read roundtrip
# ---------------------------------------------------------------------------


def test_read_paimon_basic(append_only_table):
    """Write data via pypaimon, read back via _read_table(table), verify correctness."""
    table, tmp_path = append_only_table
    data = pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "name": pa.array(["alice", "bob", "charlie"], pa.string()),
            "value": pa.array([1.1, 2.2, 3.3], pa.float64()),
            "dt": pa.array(["2024-01-01", "2024-01-01", "2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, data)

    df = _read_table(table)
    result = df.sort("id").to_arrow()

    assert result.num_rows == 3
    assert result.schema.field("id").type == pa.int64()
    assert result.schema.field("name").type in (pa.string(), pa.large_string())
    assert result.column("id").to_pylist() == [1, 2, 3]
    assert result.column("name").to_pylist() == ["alice", "bob", "charlie"]


def test_read_paimon_empty_table(append_only_table):
    """Reading an empty table should return a DataFrame with the correct schema but zero rows."""
    table, _ = append_only_table
    df = _read_table(table)
    result = df.to_arrow()

    assert result.num_rows == 0
    assert "id" in result.schema.names
    assert "name" in result.schema.names
    assert "dt" in result.schema.names


def test_read_paimon_schema_matches(append_only_table):
    """The schema reported by _read_table(table) should match the Paimon table schema."""
    table, _ = append_only_table
    df = _read_table(table)
    schema = df.schema()

    assert "id" in schema.column_names()
    assert "name" in schema.column_names()
    assert "value" in schema.column_names()
    assert "dt" in schema.column_names()


def test_read_paimon_source_is_serializable(append_only_table):
    """The Daft source must not serialize live table/file_io/storage objects."""
    from daft.pickle import dumps, loads

    from pypaimon.daft.daft_datasource import PaimonDataSource

    table, _ = append_only_table
    table.file_io._unserializable_marker = _UnserializableFileIoMarker()

    source = PaimonDataSource(
        table,
        storage_config=_UnserializableStorageConfig(),
        catalog_options={},
    )

    restored = loads(dumps(source))

    assert restored is not source
    assert restored.schema.column_names() == source.schema.column_names()
    assert restored._table is not table
    assert restored._table.identifier.get_full_name() == table.identifier.get_full_name()
    assert restored._storage_config.multithreaded_io is False


def test_read_paimon_source_serialization_preserves_pushed_filter_for_fallback(local_paimon_catalog):
    """A serialized source must keep filters accepted by SupportsPushdownFilters."""
    from daft import context, runners
    from daft.daft import StorageConfig
    from daft.io.pushdowns import Pushdowns
    from daft.pickle import dumps, loads

    from pypaimon.daft.daft_datasource import PaimonDataSource

    catalog, _ = local_paimon_catalog
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ]),
        options={
            "file.format": "avro",
            "source.split.target-size": "800b",
            "source.split.open-file-cost": "600b",
        },
    )
    catalog.create_table("test_db.avro_serialized_pushdown_filter", schema, ignore_if_exists=False)
    table = catalog.get_table("test_db.avro_serialized_pushdown_filter")
    _write_to_paimon(table, pa.table({"id": [1], "name": ["first"]}))
    _write_to_paimon(table, pa.table({"id": [999], "name": ["match"]}))

    io_config = context.get_context().daft_planning_config.default_io_config
    storage_config = StorageConfig(runners.get_or_create_runner().name != "ray", io_config)
    source = PaimonDataSource(table, storage_config=storage_config, catalog_options={})
    pushed_filters, remaining_filters = source.push_filters([(col("id") == 999)._expr])
    assert pushed_filters
    assert not remaining_filters

    restored = loads(dumps(source))
    batches = asyncio.run(
        _collect_paimon_source_batches(
            restored,
            Pushdowns(filters=None, limit=1),
        )
    )

    assert batches == [{"id": [999], "name": ["match"]}]


def test_read_paimon_remote_ray_task_is_serializable(pk_table, monkeypatch):
    """A fallback PK split task must reopen the table from metadata on Ray workers.

    Splits that need an LSM merge (here, overlapping primary-key writes) are read
    by the pypaimon reader task. Under the Ray runner that task is pickled to
    remote workers, so it must serialize only rebuildable metadata -- never the
    live table / file_io / storage objects.
    """
    from daft import runners
    from daft.io.pushdowns import Pushdowns
    from daft.pickle import dumps, loads

    from pypaimon.daft.daft_datasource import PaimonDataSource

    class _RayRunner:
        name = "ray"

    table, _ = pk_table
    # Two overlapping writes on id=1 create non-raw-convertible splits that
    # require the pypaimon merge reader (the fallback _PaimonPKSplitTask).
    _write_to_paimon(
        table,
        pa.table(
            {
                "id": pa.array([1, 2], pa.int64()),
                "name": pa.array(["old_a", "old_b"], pa.string()),
                "dt": pa.array(["2024-01-01", "2024-01-01"], pa.string()),
            }
        ),
    )
    _write_to_paimon(
        table,
        pa.table(
            {
                "id": pa.array([1], pa.int64()),
                "name": pa.array(["new_a"], pa.string()),
                "dt": pa.array(["2024-01-01"], pa.string()),
            }
        ),
    )
    table.file_io._unserializable_marker = _UnserializableFileIoMarker()

    source = PaimonDataSource(
        table,
        storage_config=_UnserializableStorageConfig(),
        catalog_options={},
    )
    monkeypatch.setattr(runners, "get_or_create_runner", lambda: _RayRunner())

    async def first_task():
        async for task in source.get_tasks(Pushdowns()):
            return task
        raise AssertionError("Expected at least one task")

    async def read_task(task):
        rows = []
        async for batch in task.read():
            rows.append(batch.to_pydict())
        return rows

    task = asyncio.run(first_task())
    assert type(task).__name__ == "_PaimonPKSplitTask"

    restored_task = loads(dumps(task))
    batches = asyncio.run(read_task(restored_task))

    merged = {
        _id: name
        for batch in batches
        for _id, name in zip(batch["id"], batch["name"])
    }
    assert merged == {1: "new_a", 2: "old_b"}


# ---------------------------------------------------------------------------
# Multi-partition reads
# ---------------------------------------------------------------------------


def test_read_paimon_multiple_partitions(append_only_table):
    """Data spread across multiple partitions should all be read back."""
    table, _ = append_only_table
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], pa.int64()),
            "name": pa.array(["a", "b", "c", "d"], pa.string()),
            "value": pa.array([1.0, 2.0, 3.0, 4.0], pa.float64()),
            "dt": pa.array(["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"], pa.string()),
        }
    )
    _write_to_paimon(table, data)

    df = _read_table(table)
    result = df.sort("id").to_arrow()

    assert result.num_rows == 4
    assert sorted(result.column("id").to_pylist()) == [1, 2, 3, 4]
    assert set(result.column("dt").to_pylist()) == {"2024-01-01", "2024-01-02"}


def test_read_paimon_partition_column_present(append_only_table):
    """Partition columns must appear in the result schema and have correct values."""
    table, _ = append_only_table
    data = pa.table(
        {
            "id": pa.array([10], pa.int64()),
            "name": pa.array(["x"], pa.string()),
            "value": pa.array([9.9], pa.float64()),
            "dt": pa.array(["2024-03-15"], pa.string()),
        }
    )
    _write_to_paimon(table, data)

    df = _read_table(table)
    result = df.to_arrow()

    assert "dt" in result.schema.names
    assert result.column("dt").to_pylist() == ["2024-03-15"]


# ---------------------------------------------------------------------------
# Column projection
# ---------------------------------------------------------------------------


def test_read_paimon_column_projection(append_only_table):
    """Requesting a subset of columns should work without errors."""
    table, _ = append_only_table
    data = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "name": pa.array(["a", "b"], pa.string()),
            "value": pa.array([1.0, 2.0], pa.float64()),
            "dt": pa.array(["2024-01-01", "2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, data)

    df = _read_table(table).select("id", "name")
    result = df.sort("id").to_arrow()

    assert result.schema.names == ["id", "name"]
    assert result.num_rows == 2
    assert result.column("id").to_pylist() == [1, 2]


def test_read_paimon_multiple_batches(local_paimon_catalog):
    """Writing data in two separate batches (two snapshots) should read all rows."""
    catalog, tmp_path = local_paimon_catalog
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("v", pa.string()),
                pa.field("dt", pa.string()),
            ]
        ),
        partition_keys=["dt"],
        options={"bucket": "1", "file.format": "parquet"},
    )
    catalog.create_table("test_db.multi_batch", schema, ignore_if_exists=False)
    table = catalog.get_table("test_db.multi_batch")

    batch1 = pa.table({"id": [1, 2], "v": ["a", "b"], "dt": ["2024-01-01", "2024-01-01"]})
    batch2 = pa.table({"id": [3, 4], "v": ["c", "d"], "dt": ["2024-01-02", "2024-01-02"]})

    _write_to_paimon(table, batch1)
    _write_to_paimon(table, batch2)

    df = _read_table(table)
    result = df.sort("id").to_arrow()

    assert result.num_rows == 4
    assert result.column("id").to_pylist() == [1, 2, 3, 4]


# ---------------------------------------------------------------------------
# Primary-key table (LSM merge fall-back)
# ---------------------------------------------------------------------------


def test_read_paimon_pk_table_basic(pk_table):
    """Primary-key table: falls back to pypaimon reader, data should be correct."""
    table, _ = pk_table
    data = pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "name": pa.array(["x", "y", "z"], pa.string()),
            "dt": pa.array(["2024-01-01", "2024-01-01", "2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, data)

    df = _read_table(table)
    result = df.sort("id").to_arrow()

    assert result.num_rows == 3
    assert result.column("id").to_pylist() == [1, 2, 3]
    assert result.column("name").to_pylist() == ["x", "y", "z"]


def test_read_paimon_pk_table_deduplication(pk_table):
    """Writing the same PK twice should result in the latest value being visible."""
    table, _ = pk_table
    batch1 = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "name": pa.array(["old_a", "old_b"], pa.string()),
            "dt": pa.array(["2024-01-01", "2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, batch1)

    batch2 = pa.table(
        {
            "id": pa.array([1], pa.int64()),
            "name": pa.array(["new_a"], pa.string()),
            "dt": pa.array(["2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, batch2)

    df = _read_table(table)
    result = df.sort("id").to_arrow()

    assert result.num_rows == 2
    id1_row = [row for row in zip(result.column("id").to_pylist(), result.column("name").to_pylist()) if row[0] == 1]
    assert len(id1_row) == 1
    assert id1_row[0][1] == "new_a"


def test_read_paimon_pk_fallback_applies_pushed_filter(pk_table):
    """Fallback PK reads must apply filters pushed into the Paimon source."""
    table, _ = pk_table
    batch1 = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "name": pa.array(["old_a", "old_b"], pa.string()),
            "dt": pa.array(["2024-01-01", "2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, batch1)

    batch2 = pa.table(
        {
            "id": pa.array([1], pa.int64()),
            "name": pa.array(["new_a"], pa.string()),
            "dt": pa.array(["2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, batch2)

    batches = asyncio.run(_read_paimon_source_batches(table, filter_expr=col("id") == 1))
    ids = [value for batch in batches for value in batch["id"]]
    names = [value for batch in batches for value in batch["name"]]

    assert ids == [1]
    assert names == ["new_a"]


def test_read_paimon_pk_fallback_filters_before_projection(pk_table):
    """Fallback reads keep filter columns available for Daft's upper filter."""
    table, _ = pk_table
    batch1 = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "name": pa.array(["old_a", "old_b"], pa.string()),
            "dt": pa.array(["2024-01-01", "2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, batch1)

    batch2 = pa.table(
        {
            "id": pa.array([1], pa.int64()),
            "name": pa.array(["new_a"], pa.string()),
            "dt": pa.array(["2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, batch2)

    batches = asyncio.run(
        _read_paimon_source_batches(
            table,
            filter_expr=col("id") == 1,
            columns=["name"],
        )
    )

    assert batches == [{"name": ["new_a"], "id": [1]}]


def test_read_paimon_fallback_plans_pushdown_filter_without_push_filters(local_paimon_catalog):
    """Fallback planning must use Pushdowns.filters even if push_filters was not called."""
    catalog, _ = local_paimon_catalog
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ]),
        options={
            "file.format": "avro",
            "source.split.target-size": "800b",
            "source.split.open-file-cost": "600b",
        },
    )
    catalog.create_table("test_db.avro_pushdown_filter", schema, ignore_if_exists=False)
    table = catalog.get_table("test_db.avro_pushdown_filter")
    _write_to_paimon(table, pa.table({"id": [1], "name": ["first"]}))
    _write_to_paimon(table, pa.table({"id": [999], "name": ["match"]}))

    batches = asyncio.run(
        _read_paimon_source_batches(
            table,
            filter_expr=col("id") == 999,
            limit=1,
            call_push_filters=False,
        )
    )

    assert batches == [{"id": [999], "name": ["match"]}]


def test_read_paimon_fallback_not_in_filter_excludes_nulls_before_limit(local_paimon_catalog):
    """Fallback datasource tasks must satisfy pushed NOT IN filters before limit."""
    catalog, _ = local_paimon_catalog
    pa_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
    ])
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa_schema,
        options={"file.format": "row"},
    )
    catalog.create_table("test_db.row_not_in_filter_limit", schema, ignore_if_exists=False)
    table = catalog.get_table("test_db.row_not_in_filter_limit")
    _write_to_paimon(table, pa.table({"id": [None], "name": ["null-row"]}, schema=pa_schema))
    _write_to_paimon(table, pa.table({"id": [3], "name": ["match"]}, schema=pa_schema))

    batches = asyncio.run(
        _read_paimon_source_batches(
            table,
            filter_expr=~col("id").is_in([1, 2]),
            limit=1,
            call_push_filters=False,
        )
    )

    assert batches == [{"id": [3], "name": ["match"]}]


def test_read_paimon_fallback_keeps_limit_above_remaining_filter(local_paimon_catalog):
    """Fallback reads must not apply limit before Daft evaluates remaining filters."""
    catalog, _ = local_paimon_catalog
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ]),
        options={"file.format": "avro"},
    )
    catalog.create_table("test_db.avro_remaining_filter_limit", schema, ignore_if_exists=False)
    table = catalog.get_table("test_db.avro_remaining_filter_limit")
    _write_to_paimon(table, pa.table({"id": [1, 999], "name": ["first", "match"]}))

    result = _read_table(table).where(~(col("id") == 1)).limit(1).to_pydict()

    assert result == {"id": [999], "name": ["match"]}


def test_read_paimon_keeps_limit_above_partition_filter(append_only_table):
    """Scan planning must not apply limit before datasource partition pruning."""
    table, _ = append_only_table
    _write_to_paimon(
        table,
        pa.table(
            {
                "id": pa.array([1], pa.int64()),
                "name": pa.array(["first"], pa.string()),
                "value": pa.array([1.0], pa.float64()),
                "dt": pa.array(["2024-01-01"], pa.string()),
            }
        ),
    )
    _write_to_paimon(
        table,
        pa.table(
            {
                "id": pa.array([2], pa.int64()),
                "name": pa.array(["match"], pa.string()),
                "value": pa.array([2.0], pa.float64()),
                "dt": pa.array(["2024-01-02"], pa.string()),
            }
        ),
    )

    result = _read_table(table).where(col("dt") == "2024-01-02").limit(1).to_pydict()

    assert result["id"] == [2]


def test_read_paimon_pk_fallback_filter_then_project_dataframe(pk_table):
    """Daft may keep the filter above the source while pushing projection."""
    table, _ = pk_table
    batch1 = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "name": pa.array(["old_a", "old_b"], pa.string()),
            "dt": pa.array(["2024-01-01", "2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, batch1)

    batch2 = pa.table(
        {
            "id": pa.array([1], pa.int64()),
            "name": pa.array(["new_a"], pa.string()),
            "dt": pa.array(["2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, batch2)

    result = _read_table(table).where(col("id") == 1).select("name").to_pydict()

    assert result == {"name": ["new_a"]}


def test_read_paimon_pk_fallback_applies_limit(pk_table):
    """Fallback PK reads must use the same limit as the planning read builder."""
    table, _ = pk_table
    data = pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "name": pa.array(["a", "b", "c"], pa.string()),
            "dt": pa.array(["2024-01-01", "2024-01-01", "2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, data)
    _write_to_paimon(table, data)

    batches = asyncio.run(_read_paimon_source_batches(table, limit=1))
    ids = [value for batch in batches for value in batch["id"]]

    assert len(ids) == 1


# ---------------------------------------------------------------------------
# Filter pushdown
# ---------------------------------------------------------------------------


def test_read_paimon_partition_filter(append_only_table):
    """Partition filter should prune partitions at scan time."""
    table, _ = append_only_table
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], pa.int64()),
            "name": pa.array(["a", "b", "c", "d"], pa.string()),
            "value": pa.array([1.0, 2.0, 3.0, 4.0], pa.float64()),
            "dt": pa.array(["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"], pa.string()),
        }
    )
    _write_to_paimon(table, data)

    df = _read_table(table).where(col("dt") == "2024-01-01")
    result = df.sort("id").to_arrow()

    assert result.num_rows == 2
    assert result.column("id").to_pylist() == [1, 2]
    assert all(dt == "2024-01-01" for dt in result.column("dt").to_pylist())


def test_read_paimon_row_filter(append_only_table):
    """Row-level filter should be applied after reading data."""
    table, _ = append_only_table
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], pa.int64()),
            "name": pa.array(["alice", "bob", "charlie", "dave"], pa.string()),
            "value": pa.array([10.0, 20.0, 30.0, 40.0], pa.float64()),
            "dt": pa.array(["2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, data)

    df = _read_table(table).where(col("id") > 2)
    result = df.sort("id").to_arrow()

    assert result.num_rows == 2
    assert result.column("id").to_pylist() == [3, 4]


def test_read_paimon_combined_filter(append_only_table):
    """Combined partition + row filter should work together."""
    table, _ = append_only_table
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
            "name": pa.array(["a", "b", "c", "d", "e", "f"], pa.string()),
            "value": pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], pa.float64()),
            "dt": pa.array(
                ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
                pa.string(),
            ),
        }
    )
    _write_to_paimon(table, data)

    df = _read_table(table).where((col("dt") == "2024-01-01") & (col("id") == 2))
    result = df.to_arrow()

    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [2]
    assert result.column("dt").to_pylist() == ["2024-01-01"]


# ---------------------------------------------------------------------------
# Filter pushdown tests
# ---------------------------------------------------------------------------


class TestFilterPushdown:
    """Tests for filter pushdown to Paimon."""

    @pytest.fixture
    def filter_table(self, local_paimon_catalog):
        """Create a table for filter pushdown tests."""
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.filter_test", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.filter_test")

        data = pa.table({"id": [1, 2, 3, 4, 5], "value": ["a", "b", "c", "d", "e"]})
        _write_to_paimon(table, data)
        return table

    def test_filter_pushdown_equal(self, filter_table):
        df = _read_table(filter_table).where(col("id") == 3)
        result = df.to_pydict()
        assert result["id"] == [3]
        assert result["value"] == ["c"]

    def test_filter_pushdown_comparison(self, filter_table):
        df = _read_table(filter_table).where(col("id") > 3)
        result = df.to_pydict()
        assert result["id"] == [4, 5]

        df = _read_table(filter_table).where(col("id") <= 2)
        result = df.to_pydict()
        assert result["id"] == [1, 2]

    def test_filter_pushdown_is_in(self, filter_table):
        df = _read_table(filter_table).where(col("id").is_in([2, 4]))
        result = df.to_pydict()
        assert result["id"] == [2, 4]

    def test_filter_pushdown_is_null(self, local_paimon_catalog):
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.filter_null", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.filter_null")

        data = pa.table({"id": [1, 2, 3], "value": ["a", None, "c"]})
        _write_to_paimon(table, data)

        df = _read_table(table).where(col("value").is_null())
        result = df.to_pydict()
        assert result["id"] == [2]

    def test_filter_pushdown_string(self, filter_table):
        df = _read_table(filter_table).where(col("value").startswith("a"))
        result = df.to_pydict()
        assert result["value"] == ["a"]

        df = _read_table(filter_table).where(col("value").contains("b"))
        result = df.to_pydict()
        assert result["value"] == ["b"]

    def test_filter_pushdown_combined(self, filter_table):
        df = _read_table(filter_table).where((col("id") >= 2) & (col("id") <= 4))
        result = df.to_pydict()
        assert result["id"] == [2, 3, 4]

    def test_filter_pushdown_splits_supported_conjuncts(self, filter_table):
        unsupported = col("value").contains(col("id"))
        cases = [
            ((col("id") == 1) & unsupported, [("equal", "id", (1,))]),
            (unsupported & (col("id") == 1), [("equal", "id", (1,))]),
            (
                (col("id") == 1) & (unsupported & (col("value") == "a")),
                [("equal", "id", (1,)), ("equal", "value", ("a",))],
            ),
        ]

        for expr, expected_leaves in cases:
            pushed_filters, remaining_filters, predicate = convert_filters_to_paimon(filter_table, expr._expr)

            assert len(pushed_filters) == len(expected_leaves)
            assert len(remaining_filters) == 1
            assert _contains_expr(remaining_filters[0])
            assert _predicate_leaves(predicate) == expected_leaves

    def test_filter_pushdown_does_not_split_or_with_unsupported_branch(self, filter_table):
        expr = (col("id") == 1) | col("value").contains(col("id"))

        pushed_filters, remaining_filters, predicate = convert_filters_to_paimon(filter_table, expr._expr)

        assert pushed_filters == []
        assert remaining_filters == [expr._expr]
        assert predicate is None

    def test_filter_pushdown_pushes_supported_or_conjunct(self, filter_table):
        supported_or = (col("id") == 1) | (col("id") == 2)
        expr = supported_or & col("value").contains(col("id"))

        pushed_filters, remaining_filters, predicate = convert_filters_to_paimon(filter_table, expr._expr)

        assert len(pushed_filters) == 1
        assert len(remaining_filters) == 1
        assert _contains_expr(remaining_filters[0])
        assert predicate is not None
        assert predicate.method == "or"
        assert _predicate_leaves(predicate) == [("equal", "id", (1,)), ("equal", "id", (2,))]

    def test_filter_pushdown_rewrites_supported_not_predicates(self, filter_table):
        cases = [
            (~(col("id") == 1), [("notEqual", "id", (1,))]),
            (~col("id").is_in([1, 2]), [("notIn", "id", (1, 2))]),
            (~col("id").between(1, 3), [("notBetween", "id", (1, 3))]),
            (~col("value").is_null(), [("isNotNull", "value", ())]),
            (~col("value").not_null(), [("isNull", "value", ())]),
        ]

        for expr, expected_leaves in cases:
            pushed_filters, remaining_filters, predicate = convert_filters_to_paimon(filter_table, expr._expr)

            assert len(pushed_filters) == 1
            assert remaining_filters == []
            assert _predicate_leaves(predicate) == expected_leaves

    def test_filter_pushdown_supported_not_predicates_read_path(self, local_paimon_catalog):
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([
            ("id", pa.int64()),
            ("value", pa.string()),
        ])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.filter_not_read", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.filter_not_read")

        data = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "value": ["a", "b", None, "d", None],
            },
            schema=pa_schema,
        )
        _write_to_paimon(table, data)

        cases = [
            (~(col("id") == 1), [2, 3, 4, 5]),
            (~col("id").is_in([1, 3]), [2, 4, 5]),
            (~col("id").between(2, 4), [1, 5]),
            (~col("value").is_null(), [1, 2, 4]),
            (~col("value").not_null(), [3, 5]),
        ]

        for expr, expected_ids in cases:
            result = _read_table(table).where(expr).select("id").sort("id").to_pydict()

            assert result["id"] == expected_ids

    def test_filter_pushdown_does_not_demorgan_not_compound_predicates(self, filter_table):
        expressions = [
            ~((col("id") == 1) & (col("value") == "a")),
            ~((col("id") == 1) | (col("value") == "a")),
        ]

        for expr in expressions:
            pushed_filters, remaining_filters, predicate = convert_filters_to_paimon(filter_table, expr._expr)

            assert pushed_filters == []
            assert remaining_filters == [expr._expr]
            assert predicate is None

    def test_unsupported_expression_remains_in_daft(self, filter_table):
        expressions = [
            col("id") == lit(1).cast("int64"),
            col("id") == lit(None),
            col("value").contains(col("id")),
            col("value").startswith(col("id")),
            col("value").endswith(col("id")),
            col("value").contains(lit("a").cast("string")),
            col("id").between(col("id"), 3),
            col("id").is_in([1, None]),
        ]

        for expr in expressions:
            pushed_filters, remaining_filters, predicate = convert_filters_to_paimon(filter_table, expr._expr)

            assert pushed_filters == []
            assert remaining_filters == [expr._expr]
            assert predicate is None

    def test_mixed_string_expression_is_filtered_by_daft(self, local_paimon_catalog):
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([
            ("id", pa.int64()),
            ("value", pa.string()),
            ("pattern", pa.string()),
        ])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.filter_mixed_string", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.filter_mixed_string")

        data = pa.table(
            {
                "id": [1, 2, 3],
                "value": ["alpha", "bravo", "charlie"],
                "pattern": ["lp", "zz", "lie"],
            }
        )
        _write_to_paimon(table, data)

        df = _read_table(table).where(col("value").contains(col("pattern")))
        result = df.sort("id").to_pydict()

        assert result["id"] == [1, 3]

    def test_mixed_conjunctive_expression_is_filtered_by_daft(self, local_paimon_catalog):
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([
            ("id", pa.int64()),
            ("value", pa.string()),
            ("pattern", pa.string()),
        ])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.filter_mixed_conjunctive", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.filter_mixed_conjunctive")

        data = pa.table(
            {
                "id": [1, 1, 2, 3],
                "value": ["alpha", "bravo", "alps", "charlie"],
                "pattern": ["lp", "zz", "lp", "lie"],
            }
        )
        _write_to_paimon(table, data)

        df = _read_table(table).where((col("id") == 1) & col("value").contains(col("pattern")))
        result = df.sort("value").to_pydict()

        assert result["id"] == [1]
        assert result["value"] == ["alpha"]


# ---------------------------------------------------------------------------
# Advanced data types
# ---------------------------------------------------------------------------


class TestNestedTypes:
    """Tests for nested data types (list, map, struct)."""

    def test_read_nested_list(self, local_paimon_catalog):
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([("id", pa.int64()), ("list_col", pa.list_(pa.int64()))])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.nested_list", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.nested_list")

        data = pa.table({"id": [1, 2], "list_col": [[1, 2, 3], [4, 5]]}, schema=pa_schema)
        _write_to_paimon(table, data)

        df = _read_table(table)
        result = df.to_pydict()
        assert len(result["id"]) == 2
        assert result["list_col"][0] == [1, 2, 3]

    def test_read_nested_map(self, local_paimon_catalog):
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([("id", pa.int64()), ("map_col", pa.map_(pa.string(), pa.int64()))])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.nested_map", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.nested_map")

        data = pa.table(
            {"id": [1, 2], "map_col": [[("k1", 1), ("k2", 2)], [("k3", 3)]]},
            schema=pa_schema,
        )
        _write_to_paimon(table, data)

        df = _read_table(table)
        result = df.to_pydict()
        assert len(result["id"]) == 2


class TestDecimalType:
    """Tests for decimal type support."""

    def test_read_decimal(self, local_paimon_catalog):
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([("id", pa.int64()), ("amount", pa.decimal128(10, 2))])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.decimal_table", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.decimal_table")

        data = pa.table(
            {
                "id": [1, 2, 3],
                "amount": [decimal.Decimal("100.50"), decimal.Decimal("200.75"), decimal.Decimal("300.00")],
            },
            schema=pa_schema,
        )
        _write_to_paimon(table, data)

        df = _read_table(table)
        result = df.to_pydict()
        assert len(result["id"]) == 3


class TestNullValues:
    """Tests for null value handling."""

    def test_read_with_nulls(self, local_paimon_catalog):
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([("id", pa.int64()), ("name", pa.string()), ("value", pa.float64())])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.null_table", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.null_table")

        data = pa.table({"id": [1, 2, 3], "name": ["alice", None, "charlie"], "value": [1.1, 2.2, None]})
        _write_to_paimon(table, data)

        df = _read_table(table)
        result = df.to_pydict()
        assert len(result["id"]) == 3
        assert result["name"][1] is None
        assert result["value"][2] is None


class TestCompositePrimaryKey:
    """Tests for composite primary key tables."""

    def test_read_composite_pk_table(self, local_paimon_catalog):
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([("pk1", pa.int64()), ("pk2", pa.string()), ("value", pa.float64())])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(
            pa_schema, primary_keys=["pk1", "pk2"], options={"bucket": "2"}
        )
        catalog.create_table("test_db.composite_pk", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.composite_pk")

        data = pa.table({"pk1": [1, 1, 2], "pk2": ["a", "b", "a"], "value": [1.1, 2.2, 3.3]})
        _write_to_paimon(table, data)

        df = _read_table(table)
        result = df.to_pydict()
        assert len(result["pk1"]) == 3
