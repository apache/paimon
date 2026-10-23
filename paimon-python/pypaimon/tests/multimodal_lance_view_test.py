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

"""Lance exports preserve resolved logical schemas and moving read views."""

import pyarrow as pa
import pytest

import pypaimon.multimodal as pmm
from pypaimon.multimodal.query import ScanQuery
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import Move, SchemaChange

lance = pytest.importorskip(
    "lance", minversion="13.0.0",
    reason="pylance >= 13.0.0 is required for Lance exports")


def test_export_lance_preserves_pinned_logical_schema(tmp_path):
    conn = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = conn.create_table(
        "pinned", schema=pa.schema([
            ("id", pa.int32()), ("text", pa.string())]),
        data=[{"id": 1, "text": "before"}], options={"file.format": "parquet"})
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    conn.catalog.alter_table("default.pinned", [
        SchemaChange.add_column(
            "note", AtomicType("STRING"), move=Move.first("note")),
    ])
    current = conn.get_table("pinned")
    query = ScanQuery(current.raw_table._copy_with_snapshot(snapshot))
    query.where("id = 1")
    expected = query.to_arrow()
    path = tmp_path / "pinned.lance"

    query.export_lance(str(path))

    result = lance.dataset(str(path)).to_table()
    assert result.equals(expected)
    assert result.to_pylist() == [{"note": None, "id": 1, "text": "before"}]


def test_export_lance_reloads_schema_when_tag_moves_backwards(tmp_path):
    conn = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = conn.create_table(
        "backwards", schema=pa.schema([
            ("id", pa.int32()), ("text", pa.string())]),
        data=[{"id": 1, "text": "before"}], options={"file.format": "parquet"})
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    table.raw_table.create_tag("training")
    conn.catalog.alter_table("default.backwards", [
        SchemaChange.add_column(
            "note", AtomicType("STRING"), move=Move.first("note")),
    ])
    current = conn.get_table("backwards")
    current.add([{"note": "new-schema", "id": 2, "text": "after"}])
    current.raw_table.replace_tag("training")
    query = current.scan(tag_name="training").where("id = 1")
    current.raw_table.replace_tag("training", snapshot.id)
    path = tmp_path / "backwards.lance"

    query.export_lance(str(path))

    result = lance.dataset(str(path)).to_table()
    assert result.schema.names == ["id", "text"]
    assert result.to_pylist() == [{"id": 1, "text": "before"}]
