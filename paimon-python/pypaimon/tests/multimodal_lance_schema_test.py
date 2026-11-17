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

"""Lance exports distinguish logical schema changes from data snapshots."""

import pyarrow as pa
import pytest

import pypaimon.multimodal as pmm
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange

lance = pytest.importorskip("lance", minversion="13.0.0", reason="pylance >= 13.0.0 is required for Lance exports")

_ORIGINAL_ROWS = [{"id": 1, "text": "original"}]


@pytest.fixture(params=["add", "drop", "rename"])
def schema_only_alter(request, tmp_path):
    conn = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = conn.create_table(
        "schema_only", schema=pa.schema([("id", pa.int32()), ("text", pa.string())]),
        data=_ORIGINAL_ROWS, options={"file.format": "parquet"})
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    table.raw_table.create_tag("before-alter", snapshot_id=snapshot.id)
    captured = table.scan().where("id = 1")
    if request.param == "add":
        change = SchemaChange.add_column("note", AtomicType("STRING"))
        expected = [{"id": 1, "text": "original", "note": None}]
        projection = ["note", "id"]
    elif request.param == "drop":
        change = SchemaChange.drop_column("text")
        expected = [{"id": 1}]
        projection = ["id"]
    else:
        change = SchemaChange.rename_column("text", "renamed")
        expected = [{"id": 1, "renamed": "original"}]
        projection = ["renamed", "id"]
    conn.catalog.alter_table("default.schema_only", [change])
    current = conn.get_table("schema_only")
    assert current.raw_table.table_schema.id > snapshot.schema_id
    assert current.raw_table.snapshot_manager().get_latest_snapshot().id == snapshot.id
    return current, captured, snapshot.id, expected, projection


def test_export_lance_preserves_latest_schema_only_alter(schema_only_alter, tmp_path):
    current, _, _, expected, projection = schema_only_alter
    logical = current.scan().to_arrow()
    assert logical.to_pylist() == expected

    exported = current.scan().export_lance(str(tmp_path / "latest.lance"))

    assert exported.schema.names == logical.schema.names
    assert exported.to_table().to_pylist() == expected
    projected = current.scan().select(projection)
    projected_logical = projected.to_arrow()
    projected_export = projected.export_lance(str(tmp_path / "projected.lance"))
    assert projected_export.schema.names == projection
    assert projected_export.to_table().to_pylist() == projected_logical.to_pylist()


@pytest.mark.parametrize("view", ["snapshot", "tag"])
def test_export_lance_schema_only_alter_retains_explicit_history(schema_only_alter, tmp_path, view):
    current, _, snapshot_id, _, _ = schema_only_alter
    selector = {"snapshot_id": snapshot_id} if view == "snapshot" else {"tag_name": "before-alter"}
    query = current.scan(**selector)
    assert query.to_list() == _ORIGINAL_ROWS

    exported = query.export_lance(str(tmp_path / (view + ".lance")))

    assert exported.schema.names == ["id", "text"]
    assert exported.to_table().to_pylist() == _ORIGINAL_ROWS


def test_export_lance_scan_captured_before_schema_only_alter(schema_only_alter, tmp_path):
    _, captured, _, _, _ = schema_only_alter
    assert captured.to_list() == _ORIGINAL_ROWS

    exported = captured.export_lance(str(tmp_path / "captured.lance"))

    assert exported.schema.names == ["id", "text"]
    assert exported.to_table().to_pylist() == _ORIGINAL_ROWS
    assert captured.to_list() == _ORIGINAL_ROWS
