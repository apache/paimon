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

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from daft import col

from pypaimon.daft import read_paimon, write_paimon


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


def test_read_paimon_rejects_snapshot_id_and_tag_name_together(catalog_options):
    with pytest.raises(ValueError, match="snapshot_id and tag_name cannot be set at the same time"):
        read_paimon(
            "test_db.dummy",
            catalog_options,
            snapshot_id=1,
            tag_name="v1",
        )


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
