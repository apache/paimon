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

"""Tests for _write_table() — writes to a local Paimon filesystem catalog.

All tests run without Docker or external services. Data written via Daft is
verified by reading back with both _read_table() and pypaimon's native
reader to ensure correctness.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from pypaimon.daft.daft_compat import has_file_range_reads
from pypaimon.daft.daft_catalog import PaimonTable
from pypaimon.daft.daft_paimon import _read_table, _write_table

requires_blob = pytest.mark.skipif(not has_file_range_reads(), reason="BLOB support requires daft >= 0.7.11")


# ---------------------------------------------------------------------------
# Helpers & Fixtures
# ---------------------------------------------------------------------------


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
def append_only_table_no_partition(local_paimon_catalog):
    catalog, tmp_path = local_paimon_catalog
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ]),
        options={"bucket": "1", "file.format": "parquet"},
    )
    catalog.create_table("test_db.append_no_part", schema, ignore_if_exists=False)
    return catalog.get_table("test_db.append_no_part"), tmp_path


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
# Basic append
# ---------------------------------------------------------------------------


def test_write_paimon_append_basic(append_only_table):
    """_write_table with mode='append' should persist data readable by _read_table."""
    table, _ = append_only_table
    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["alice", "bob", "charlie"],
            "value": [1.1, 2.2, 3.3],
            "dt": ["2024-01-01", "2024-01-01", "2024-01-01"],
        }
    )
    _write_table(df, table)

    result = _read_table(table).sort("id").to_arrow()
    assert result.num_rows == 3
    assert result.column("id").to_pylist() == [1, 2, 3]
    assert result.column("name").to_pylist() == ["alice", "bob", "charlie"]


def test_write_paimon_append_returns_summary(append_only_table):
    """_write_table should return a DataFrame with operation metadata columns."""
    table, _ = append_only_table
    df = daft.from_pydict(
        {
            "id": [10, 20],
            "name": ["x", "y"],
            "value": [5.0, 6.0],
            "dt": ["2024-02-01", "2024-02-01"],
        }
    )
    result = _write_table(df, table)
    result_dict = result.to_pydict()

    assert "operation" in result_dict
    assert "rows" in result_dict
    assert "file_size" in result_dict
    assert "file_name" in result_dict

    assert all(op == "ADD" for op in result_dict["operation"])
    assert sum(result_dict["rows"]) == 2
    assert all(s > 0 for s in result_dict["file_size"])
    assert all(len(fn) > 0 for fn in result_dict["file_name"])


def test_write_paimon_append_multiple_times(append_only_table):
    """Multiple append writes should accumulate rows."""
    table, _ = append_only_table
    df1 = daft.from_pydict({"id": [1], "name": ["a"], "value": [1.0], "dt": ["2024-01-01"]})
    df2 = daft.from_pydict({"id": [2], "name": ["b"], "value": [2.0], "dt": ["2024-01-02"]})
    _write_table(df1, table)
    _write_table(df2, table)

    result = _read_table(table).sort("id").to_arrow()
    assert result.num_rows == 2
    assert result.column("id").to_pylist() == [1, 2]


def test_write_paimon_roundtrip_native_verify(append_only_table):
    """Data written by Daft should also be readable via pypaimon's native reader."""
    table, _ = append_only_table
    df = daft.from_pydict(
        {
            "id": [7, 8, 9],
            "name": ["p", "q", "r"],
            "value": [7.0, 8.0, 9.0],
            "dt": ["2024-05-01", "2024-05-01", "2024-05-01"],
        }
    )
    _write_table(df, table)

    # Verify via pypaimon native reader
    read_builder = table.new_read_builder()
    table_scan = read_builder.new_scan()
    table_read = read_builder.new_read()
    splits = table_scan.plan().splits()
    arrow_table = table_read.to_arrow(splits)

    assert arrow_table.num_rows == 3
    ids = sorted(arrow_table.column("id").to_pylist())
    assert ids == [7, 8, 9]


# ---------------------------------------------------------------------------
# Overwrite
# ---------------------------------------------------------------------------


def test_write_paimon_overwrite_full_unpartitioned(append_only_table_no_partition):
    """mode='overwrite' on an unpartitioned table should replace all existing data."""
    table, _ = append_only_table_no_partition
    initial = daft.from_pydict({"id": [1, 2], "name": ["a", "b"]})
    _write_table(initial, table)

    replacement = daft.from_pydict({"id": [100], "name": ["z"]})
    result = _write_table(replacement, table, mode="overwrite")
    result_dict = result.to_pydict()
    assert all(op == "OVERWRITE" for op in result_dict["operation"])

    final = _read_table(table).to_arrow()
    assert final.num_rows == 1
    assert final.column("id").to_pylist() == [100]


def test_write_paimon_overwrite_dynamic_partition(append_only_table):
    """mode='overwrite' on a partitioned table should only replace touched partitions."""
    table, _ = append_only_table
    initial = daft.from_pydict(
        {
            "id": [1, 2, 3, 4],
            "name": ["a", "b", "c", "d"],
            "value": [1.0, 2.0, 3.0, 4.0],
            "dt": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
        }
    )
    _write_table(initial, table)

    replacement = daft.from_pydict({"id": [10], "name": ["x"], "value": [10.0], "dt": ["2024-01-01"]})
    result = _write_table(replacement, table, mode="overwrite")
    result_dict = result.to_pydict()
    assert all(op == "OVERWRITE" for op in result_dict["operation"])

    final = _read_table(table).sort("id").to_pydict()
    assert final["id"] == [3, 4, 10]
    assert final["dt"] == ["2024-01-02", "2024-01-02", "2024-01-01"]


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_write_paimon_invalid_mode(append_only_table):
    """An unsupported mode should raise a ValueError."""
    table, _ = append_only_table
    df = daft.from_pydict({"id": [1], "name": ["a"], "value": [1.0], "dt": ["2024-01-01"]})
    with pytest.raises(ValueError, match="Only 'append' or 'overwrite' mode is supported"):
        _write_table(df, table, mode="upsert")


def test_write_paimon_pk_table(pk_table):
    """Writing to a PK table should work and be readable back."""
    table, _ = pk_table
    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["x", "y", "z"],
            "dt": ["2024-01-01", "2024-01-01", "2024-01-01"],
        }
    )
    _write_table(df, table)

    result = _read_table(table).sort("id").to_arrow()
    assert result.num_rows == 3
    assert result.column("id").to_pylist() == [1, 2, 3]


# ---------------------------------------------------------------------------
# Schema conversion tests
# ---------------------------------------------------------------------------


class TestSchemaConversion:
    """Tests for schema conversion utilities."""

    def test_write_large_string_conversion(self, local_paimon_catalog):
        """Test that large_string columns are converted to string for pypaimon."""
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([("id", pa.int64()), ("text", pa.string())])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.large_str", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.large_str")

        df = daft.from_pydict({"id": [1, 2, 3], "text": ["a", "b", "c"]})
        _write_table(df, table, mode="append")

        result = _read_table(table).to_pydict()
        assert result["id"] == [1, 2, 3]
        assert result["text"] == ["a", "b", "c"]

    def test_write_large_binary_conversion(self, local_paimon_catalog):
        """Test that large_binary columns are converted to binary for pypaimon."""
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([("id", pa.int64()), ("data", pa.binary())])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.large_bin", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.large_bin")

        df = daft.from_pydict({"id": [1, 2], "data": [b"abc", b"def"]})
        _write_table(df, table, mode="append")

        result = _read_table(table).to_pydict()
        assert result["id"] == [1, 2]


# ---------------------------------------------------------------------------
# Complex type tests
# ---------------------------------------------------------------------------


class TestComplexTypes:
    """Tests for writing complex data types."""

    def test_write_nested_list(self, local_paimon_catalog):
        """Test writing Paimon table with list type."""
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([("id", pa.int64()), ("list_col", pa.list_(pa.int64()))])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.write_list", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.write_list")

        df = daft.from_pydict({"id": [1, 2], "list_col": [[1, 2, 3], [4, 5]]})
        _write_table(df, table, mode="append")

        result = _read_table(table).to_pydict()
        assert result["id"] == [1, 2]


@requires_blob
class TestBlobType:
    """Tests for BLOB type support (pypaimon 1.4+)."""

    def test_write_read_blob_type(self, local_paimon_catalog):
        """Test that BLOB columns are returned as FileReference objects."""
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([("id", pa.int64()), ("blob_data", pa.large_binary())])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(
            pa_schema,
            options={
                "bucket": "1",
                "file.format": "parquet",
                "row-tracking.enabled": "true",
                "data-evolution.enabled": "true",
            },
        )
        catalog.create_table("test_db.blob_table", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.blob_table")

        df = daft.from_pydict({"id": [1, 2], "blob_data": [b"hello", b"world"]})
        _write_table(df, table, mode="append")

        result_df = _read_table(table).sort("id")
        assert str(result_df.schema()["blob_data"].dtype) == "File[Unknown]"

        result = result_df.to_pydict()
        assert result["id"] == [1, 2]

        blob_refs = result["blob_data"]
        assert len(blob_refs) == 2
        for ref in blob_refs:
            assert isinstance(ref, daft.File)
            assert isinstance(ref.path, str)
            assert ".blob" in ref.path
            assert ref.offset is not None
            assert ref.length is not None


# ---------------------------------------------------------------------------
# Truncate tests
# ---------------------------------------------------------------------------


class TestTruncate:
    """Tests for table truncate operations (pypaimon 1.4+)."""

    def test_truncate_table(self, append_only_table):
        """truncate() should remove all data from the table."""
        table, _ = append_only_table
        df = daft.from_pydict(
            {"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0], "dt": ["2024-01-01"] * 3}
        )
        _write_table(df, table)
        assert _read_table(table).count_rows() == 3

        paimon_table = PaimonTable(table)
        paimon_table.truncate()
        assert _read_table(table).count_rows() == 0

    def test_truncate_partitions(self, append_only_table):
        """truncate_partitions() should remove only the specified partition data."""
        table, _ = append_only_table
        df = daft.from_pydict(
            {
                "id": [1, 2, 3, 4],
                "name": ["a", "b", "c", "d"],
                "value": [1.0, 2.0, 3.0, 4.0],
                "dt": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
            }
        )
        _write_table(df, table)
        assert _read_table(table).count_rows() == 4

        paimon_table = PaimonTable(table)
        paimon_table.truncate_partitions([{"dt": "2024-01-01"}])
        result = _read_table(table).sort("id").to_pydict()
        assert result["id"] == [3, 4]
        assert result["dt"] == ["2024-01-02", "2024-01-02"]
