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

"""Tests for the public pypaimon.daft read_paimon() / write_paimon() API."""

from __future__ import annotations

import datetime
import decimal
import time

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from daft import col

from pypaimon.daft import explain_paimon_scan, read_paimon, write_paimon
from pypaimon.daft.daft_paimon import _timestamp_scan_option
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import Move, SchemaChange


@pytest.fixture
def catalog_options(tmp_path):
    options = {"warehouse": str(tmp_path)}
    catalog = pypaimon.CatalogFactory.create(options)
    catalog.create_database("test_db", ignore_if_exists=True)
    return options


def _create_table(
    catalog_options,
    table_name: str,
    pa_schema: pa.Schema,
    *,
    partition_keys: list[str] | None = None,
    options: dict[str, str] | None = None,
):
    identifier = f"test_db.{table_name}"
    catalog = pypaimon.CatalogFactory.create(catalog_options)
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa_schema,
        partition_keys=partition_keys,
        options=options,
    )
    catalog.create_table(identifier, schema, ignore_if_exists=False)
    return identifier, catalog.get_table(identifier)


def _write_arrow(table, data: pa.Table) -> None:
    write_builder = table.new_batch_write_builder()
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()
    try:
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
    finally:
        table_write.close()
        table_commit.close()


def _create_and_populate_table(
    catalog_options,
    table_name: str,
    data: pa.Table,
    *,
    partition_keys: list[str] | None = None,
    options: dict[str, str] | None = None,
) -> str:
    identifier, table = _create_table(
        catalog_options,
        table_name,
        data.schema,
        partition_keys=partition_keys,
        options=options,
    )
    _write_arrow(table, data)
    return identifier


def test_read_paimon_basic(catalog_options):
    data = pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "name": pa.array(["alice", "bob", "carol"], pa.string()),
            "value": pa.array([10, 20, 30], pa.int64()),
        }
    )
    identifier = _create_and_populate_table(catalog_options, "read_basic", data)

    result = read_paimon(identifier, catalog_options).sort("id").to_pydict()

    assert result == {
        "id": [1, 2, 3],
        "name": ["alice", "bob", "carol"],
        "value": [10, 20, 30],
    }


def test_read_paimon_data_evolution_merges_column_fragments(catalog_options):
    pa_schema = pa.schema([
        ("id", pa.int32()),
        ("name", pa.string()),
        ("score", pa.float64()),
    ])
    identifier, table = _create_table(
        catalog_options,
        "read_data_evolution",
        pa_schema,
        options={
            "row-tracking.enabled": "true",
            "data-evolution.enabled": "true",
            "file.format": "parquet",
        },
    )
    write_builder = table.new_batch_write_builder()
    id_name_write = write_builder.new_write().with_write_type(["id", "name"])
    score_write = write_builder.new_write().with_write_type(["score"])
    table_commit = write_builder.new_commit()
    try:
        id_name_write.write_arrow(pa.table({
            "id": pa.array([1, 2, 3], pa.int32()),
            "name": pa.array(["a", "b", "c"], pa.string()),
        }))
        score_write.write_arrow(pa.table({
            "score": pa.array([1.1, 2.2, 3.3], pa.float64()),
        }))
        commit_messages = id_name_write.prepare_commit() + score_write.prepare_commit()
        # Both files are column fragments for the same logical row range.
        for message in commit_messages:
            for data_file in message.new_files:
                data_file.first_row_id = 0
        table_commit.commit(commit_messages)
    finally:
        id_name_write.close()
        score_write.close()
        table_commit.close()

    splits = table.new_read_builder().new_scan().plan().splits()
    assert len(splits) == 1
    assert splits[0].raw_convertible is False

    result = read_paimon(identifier, catalog_options).to_pydict()

    assert result == {
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
        "score": [1.1, 2.2, 3.3],
    }


def test_read_paimon_renamed_column_from_old_schema_file(catalog_options):
    old_schema = pa.schema([
        ("id", pa.int64()),
        ("value", pa.string()),
    ])
    identifier, table = _create_table(
        catalog_options,
        "read_renamed_column",
        old_schema,
        options={"bucket": "-1", "file.format": "parquet"},
    )
    old_schema_id = table.table_schema.id
    _write_arrow(
        table,
        pa.table({"id": [1, 2], "value": ["a", "b"]}, schema=old_schema),
    )

    catalog = pypaimon.CatalogFactory.create(catalog_options)
    catalog.alter_table(
        identifier,
        [SchemaChange.rename_column("value", "renamed")],
        ignore_if_not_exists=False,
    )
    table = catalog.get_table(identifier)
    splits = table.new_read_builder().new_scan().plan().splits()

    assert all(split.raw_convertible for split in splits)
    assert table.table_schema.id != old_schema_id
    assert {
        data_file.schema_id
        for split in splits
        for data_file in split.files
    } == {old_schema_id}

    projected_result = (
        read_paimon(identifier, catalog_options)
        .select("id")
        .sort("id")
        .to_pydict()
    )
    result = read_paimon(identifier, catalog_options).sort("id").to_pydict()

    assert projected_result == {"id": [1, 2]}
    assert result == {
        "id": [1, 2],
        "renamed": ["a", "b"],
    }


def test_read_paimon_renamed_nested_field_from_old_schema_file(catalog_options):
    old_schema = pa.schema([
        ("id", pa.int64()),
        ("payload", pa.struct([
            ("value", pa.string()),
            ("count", pa.int32()),
        ])),
    ])
    identifier, table = _create_table(
        catalog_options,
        "read_renamed_nested_field",
        old_schema,
        options={"bucket": "-1", "file.format": "parquet"},
    )
    _write_arrow(
        table,
        pa.Table.from_pylist(
            [
                {"id": 1, "payload": {"value": "a", "count": 10}},
                {"id": 2, "payload": {"value": "b", "count": 20}},
            ],
            schema=old_schema,
        ),
    )

    catalog = pypaimon.CatalogFactory.create(catalog_options)
    catalog.alter_table(
        identifier,
        [SchemaChange.rename_column(["payload", "value"], "renamed")],
        ignore_if_not_exists=False,
    )

    explain = explain_paimon_scan(identifier, catalog_options, verbose=True)
    result = read_paimon(identifier, catalog_options).sort("id").to_pydict()

    assert explain.pypaimon_fallback_split_count == 1
    assert explain.native_parquet_split_count == 0
    assert result == {
        "id": [1, 2],
        "payload": [
            {"renamed": "a", "count": 10},
            {"renamed": "b", "count": 20},
        ],
    }


def test_read_paimon_drop_readd_same_column_isolates_field_ids(
    catalog_options,
):
    old_schema = pa.schema([
        ("id", pa.int64()),
        ("value", pa.string()),
    ])
    identifier, table = _create_table(
        catalog_options,
        "read_drop_readd_column",
        old_schema,
        options={"bucket": "-1", "file.format": "parquet"},
    )
    old_value_field = table.fields[1]
    _write_arrow(
        table,
        pa.table({"id": [1, 2], "value": ["a", "b"]}, schema=old_schema),
    )

    catalog = pypaimon.CatalogFactory.create(catalog_options)
    catalog.alter_table(
        identifier,
        [SchemaChange.drop_column("value")],
        ignore_if_not_exists=False,
    )
    catalog.alter_table(
        identifier,
        [SchemaChange.add_column("value", AtomicType("STRING"))],
        ignore_if_not_exists=False,
    )
    table = catalog.get_table(identifier)
    new_value_field = table.fields[1]

    assert new_value_field.name == old_value_field.name
    assert new_value_field.type == old_value_field.type
    assert new_value_field.id != old_value_field.id

    explain = explain_paimon_scan(identifier, catalog_options, verbose=True)

    assert explain.pypaimon_fallback_split_count == 1
    assert explain.native_parquet_split_count == 0

    result = read_paimon(identifier, catalog_options).sort("id").to_pydict()

    assert result == {
        "id": [1, 2],
        "value": [None, None],
    }


def test_native_read_handles_name_aligned_schema_evolution(catalog_options):
    old_schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("value", pa.string()),
    ])
    identifier, table = _create_table(
        catalog_options,
        "read_native_schema_evolution",
        old_schema,
        options={"bucket": "-1", "file.format": "parquet"},
    )
    _write_arrow(
        table,
        pa.table({"id": [1, 2], "value": ["a", "b"]}, schema=old_schema),
    )

    catalog = pypaimon.CatalogFactory.create(catalog_options)
    catalog.alter_table(
        identifier,
        [
            SchemaChange.add_column("added", AtomicType("STRING")),
            SchemaChange.update_column_position(Move.first("added")),
            SchemaChange.update_column_nullability("id", True),
            SchemaChange.update_column_type("id", AtomicType("BIGINT")),
            SchemaChange.drop_column("value"),
        ],
        ignore_if_not_exists=False,
    )

    explain = explain_paimon_scan(identifier, catalog_options, verbose=True)
    result = read_paimon(identifier, catalog_options).sort("id").to_pydict()
    filtered_result = (
        read_paimon(identifier, catalog_options)
        .where(col("id") == 2)
        .select("id")
        .to_pydict()
    )

    assert explain.native_parquet_split_count == 1
    assert explain.pypaimon_fallback_split_count == 0
    assert explain.fallback_reasons == {}
    assert result == {
        "added": [None, None],
        "id": [1, 2],
    }
    assert filtered_result == {"id": [2]}


def test_native_read_handles_nested_type_widening(catalog_options):
    old_schema = pa.schema([
        ("id", pa.int64()),
        ("payload", pa.struct([
            ("value", pa.int32()),
            ("label", pa.string()),
        ])),
    ])
    identifier, table = _create_table(
        catalog_options,
        "read_nested_type_widening",
        old_schema,
        options={"bucket": "-1", "file.format": "parquet"},
    )
    _write_arrow(
        table,
        pa.Table.from_pylist(
            [
                {"id": 1, "payload": {"value": 10, "label": "a"}},
                {"id": 2, "payload": {"value": 20, "label": "b"}},
            ],
            schema=old_schema,
        ),
    )

    catalog = pypaimon.CatalogFactory.create(catalog_options)
    catalog.alter_table(
        identifier,
        [SchemaChange.update_column_type(["payload", "value"], AtomicType("BIGINT"))],
        ignore_if_not_exists=False,
    )

    explain = explain_paimon_scan(identifier, catalog_options, verbose=True)
    result = read_paimon(identifier, catalog_options).sort("id").to_arrow()

    assert explain.native_parquet_split_count == 1
    assert explain.pypaimon_fallback_split_count == 0
    assert explain.fallback_reasons == {}
    assert result.schema.field("payload").type.field("value").type == pa.int64()
    assert result.to_pydict() == {
        "id": [1, 2],
        "payload": [
            {"value": 10, "label": "a"},
            {"value": 20, "label": "b"},
        ],
    }


@pytest.mark.parametrize(
    (
        "case_name",
        "old_type",
        "pypaimon_type",
        "daft_type",
        "new_paimon_type",
        "values",
        "expected",
    ),
    [
        pytest.param(
            "decimal_scale_down",
            pa.decimal128(10, 4),
            pa.decimal128(10, 2),
            pa.decimal128(10, 2),
            AtomicType("DECIMAL(10, 2)"),
            [decimal.Decimal("1.2355"), decimal.Decimal("-4.5678")],
            [decimal.Decimal("1.23"), decimal.Decimal("-4.56")],
            id="decimal-scale-down",
        ),
        pytest.param(
            "timestamp_to_time",
            pa.timestamp("ms"),
            pa.time32("ms"),
            pa.time64("us"),
            AtomicType("TIME(3)"),
            [
                datetime.datetime(2025, 1, 2, 3, 4, 5, 678000),
                datetime.datetime(1999, 12, 31, 23, 59, 58, 987000),
            ],
            [
                datetime.time(3, 4, 5, 678000),
                datetime.time(23, 59, 58, 987000),
            ],
            id="timestamp-to-time",
        ),
    ],
)
def test_read_paimon_falls_back_for_native_cast_semantic_mismatch(
    catalog_options,
    case_name,
    old_type,
    pypaimon_type,
    daft_type,
    new_paimon_type,
    values,
    expected,
):
    old_schema = pa.schema([("id", pa.int64()), ("value", old_type)])
    identifier, table = _create_table(
        catalog_options,
        f"read_{case_name}",
        old_schema,
        options={"bucket": "-1", "file.format": "parquet"},
    )
    _write_arrow(
        table,
        pa.table({"id": [1, 2], "value": values}, schema=old_schema),
    )

    catalog = pypaimon.CatalogFactory.create(catalog_options)
    catalog.alter_table(
        identifier,
        [SchemaChange.update_column_type("value", new_paimon_type)],
        ignore_if_not_exists=False,
    )
    table = catalog.get_table(identifier)

    explain = explain_paimon_scan(identifier, catalog_options, verbose=True)
    daft_result = read_paimon(identifier, catalog_options).sort("id").to_arrow()
    read_builder = table.new_read_builder()
    pypaimon_result = read_builder.new_read().to_arrow(
        read_builder.new_scan().plan().splits()
    ).sort_by([("id", "ascending")])
    expected_result = pa.table(
        {"id": [1, 2], "value": expected},
        schema=pa.schema([("id", pa.int64()), ("value", pypaimon_type)]),
    )

    assert explain.pypaimon_fallback_split_count == 1
    assert explain.native_parquet_split_count == 0
    assert daft_result.schema == pa.schema(
        [("id", pa.int64()), ("value", daft_type)]
    )
    assert daft_result.to_pydict() == pypaimon_result.to_pydict()
    assert pypaimon_result.equals(expected_result)


def test_read_paimon_time_uses_pypaimon_fallback(catalog_options):
    schema = pa.schema([("id", pa.int64()), ("value", pa.time32("ms"))])
    values = [
        datetime.time(3, 4, 5, 678000),
        datetime.time(23, 59, 58, 987000),
    ]
    identifier, table = _create_table(
        catalog_options,
        "read_time",
        schema,
        options={"bucket": "-1", "file.format": "parquet"},
    )
    _write_arrow(
        table,
        pa.table({"id": [1, 2], "value": values}, schema=schema),
    )

    explain = explain_paimon_scan(identifier, catalog_options, verbose=True)
    daft_result = read_paimon(identifier, catalog_options).sort("id").to_arrow()
    read_builder = table.new_read_builder()
    pypaimon_result = read_builder.new_read().to_arrow(
        read_builder.new_scan().plan().splits()
    ).sort_by([("id", "ascending")])

    assert explain.pypaimon_fallback_split_count == 1
    assert explain.native_parquet_split_count == 0
    assert daft_result.schema == pa.schema(
        [("id", pa.int64()), ("value", pa.time64("us"))]
    )
    assert pypaimon_result.schema == schema
    assert daft_result.to_pydict() == pypaimon_result.to_pydict()


def test_read_paimon_projection(catalog_options):
    data = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "name": pa.array(["alice", "bob"], pa.string()),
            "value": pa.array([10, 20], pa.int64()),
        }
    )
    identifier = _create_and_populate_table(catalog_options, "read_projection", data)

    result = read_paimon(identifier, catalog_options).select("id", "name").sort("id").to_pydict()

    assert result == {
        "id": [1, 2],
        "name": ["alice", "bob"],
    }


def test_read_paimon_filter(catalog_options):
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], pa.int64()),
            "category": pa.array(["A", "B", "A", "C"], pa.string()),
            "amount": pa.array([100, 200, 150, 300], pa.int64()),
        }
    )
    identifier = _create_and_populate_table(catalog_options, "read_filter", data)

    result = (
        read_paimon(identifier, catalog_options)
        .where((col("category") == "A") & (col("amount") >= 120))
        .sort("id")
        .to_pydict()
    )

    assert result == {
        "id": [3],
        "category": ["A"],
        "amount": [150],
    }


def test_read_paimon_limit(catalog_options):
    data = pa.table(
        {
            "id": pa.array(list(range(10)), pa.int64()),
            "name": pa.array([f"name-{i}" for i in range(10)], pa.string()),
        }
    )
    identifier = _create_and_populate_table(catalog_options, "read_limit", data)

    result = read_paimon(identifier, catalog_options).limit(3).to_pydict()

    assert len(result["id"]) == 3


def test_read_paimon_with_snapshot_id(catalog_options):
    pa_schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    identifier, table = _create_table(catalog_options, "read_snapshot_id", pa_schema)
    _write_arrow(table, pa.table({"id": [1], "name": ["first"]}, schema=pa_schema))
    _write_arrow(table, pa.table({"id": [2], "name": ["second"]}, schema=pa_schema))

    latest = read_paimon(identifier, catalog_options).sort("id").to_pydict()
    snap1 = read_paimon(identifier, catalog_options, snapshot_id=1).to_pydict()

    assert latest["id"] == [1, 2]
    assert snap1 == {"id": [1], "name": ["first"]}


def test_read_paimon_with_tag_name(catalog_options):
    pa_schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    identifier, table = _create_table(catalog_options, "read_tag_name", pa_schema)
    _write_arrow(table, pa.table({"id": [1], "name": ["tagged"]}, schema=pa_schema))
    table.create_tag("v1")
    _write_arrow(table, pa.table({"id": [2], "name": ["latest"]}, schema=pa_schema))

    result = read_paimon(identifier, catalog_options, tag_name="v1").to_pydict()

    assert result == {"id": [1], "name": ["tagged"]}


def test_read_paimon_with_timestamp(catalog_options):
    pa_schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    identifier, table = _create_table(catalog_options, "read_timestamp", pa_schema)
    _write_arrow(table, pa.table({"id": [1], "name": ["first"]}, schema=pa_schema))
    # A cutoff strictly between the two commits: scan.timestamp-millis returns the
    # latest snapshot at or before it, i.e. the first snapshot only.
    time.sleep(0.05)
    cutoff_ms = int(time.time() * 1000)
    time.sleep(0.05)
    _write_arrow(table, pa.table({"id": [2], "name": ["second"]}, schema=pa_schema))

    latest = read_paimon(identifier, catalog_options).sort("id").to_pydict()
    at_millis = read_paimon(identifier, catalog_options, timestamp=cutoff_ms).to_pydict()
    at_datetime = read_paimon(
        identifier,
        catalog_options,
        timestamp=datetime.datetime.fromtimestamp(cutoff_ms / 1000),
    ).to_pydict()

    assert latest["id"] == [1, 2]
    assert at_millis == {"id": [1], "name": ["first"]}
    assert at_datetime == {"id": [1], "name": ["first"]}


def test_timestamp_scan_option_mapping():
    assert _timestamp_scan_option(1751990400000) == {"scan.timestamp-millis": "1751990400000"}
    assert _timestamp_scan_option("2026-07-09 10:00:00") == {"scan.timestamp": "2026-07-09 10:00:00"}
    dt = datetime.datetime(2026, 7, 9, 10, 0, 0)
    assert _timestamp_scan_option(dt) == {"scan.timestamp-millis": str(int(dt.timestamp() * 1000))}
    with pytest.raises(TypeError):
        _timestamp_scan_option(True)
    with pytest.raises(TypeError):
        _timestamp_scan_option(1.5)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"snapshot_id": 1, "tag_name": "v1"},
        {"snapshot_id": 1, "timestamp": 1},
        {"tag_name": "v1", "timestamp": 1},
        {"snapshot_id": 1, "tag_name": "v1", "timestamp": 1},
    ],
)
def test_read_paimon_rejects_multiple_time_travel(catalog_options, kwargs):
    with pytest.raises(ValueError, match="Only one of snapshot_id, tag_name, timestamp"):
        read_paimon("test_db.dummy", catalog_options, **kwargs)


def test_write_paimon_append(catalog_options):
    pa_schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
    ])
    identifier, _ = _create_table(catalog_options, "write_append", pa_schema)
    df = daft.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    write_paimon(df, identifier, catalog_options)

    result = read_paimon(identifier, catalog_options).sort("id").to_pydict()
    assert result == {"id": [1, 2, 3], "name": ["a", "b", "c"]}


def test_write_paimon_overwrite(catalog_options):
    pa_schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
    ])
    identifier, _ = _create_table(catalog_options, "write_overwrite", pa_schema)
    write_paimon(
        daft.from_pydict({"id": [1, 2], "name": ["old-a", "old-b"]}),
        identifier,
        catalog_options,
    )

    write_paimon(
        daft.from_pydict({"id": [3], "name": ["new"]}),
        identifier,
        catalog_options,
        mode="overwrite",
    )

    result = read_paimon(identifier, catalog_options).to_pydict()
    assert result == {"id": [3], "name": ["new"]}
