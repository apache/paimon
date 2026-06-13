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

"""Tests for Daft-side Paimon scan explain diagnostics."""

from __future__ import annotations

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from daft import col

from pypaimon.daft import explain_paimon_scan
from pypaimon.daft.daft_catalog import PaimonTable
from pypaimon.daft.daft_explain import (
    READER_MODE_NATIVE_PARQUET,
    READER_MODE_PYPAIMON_FALLBACK,
)
from pypaimon.daft.daft_compat import has_file_range_reads
from pypaimon.daft.daft_datasource import PaimonDataSource
from pypaimon.daft.daft_paimon import _explain_table
from pypaimon.read.explain import ExplainResult, ExplainSplitInfo


requires_blob = pytest.mark.skipif(not has_file_range_reads(), reason="BLOB support requires daft >= 0.7.11")


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
    primary_keys: list[str] | None = None,
    options: dict[str, str] | None = None,
):
    identifier = f"test_db.{table_name}"
    catalog = pypaimon.CatalogFactory.create(catalog_options)
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa_schema,
        partition_keys=partition_keys,
        primary_keys=primary_keys,
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


def _single_split_explain(
    *,
    table_identifier: str,
    raw_convertible: bool,
    has_deletion_vectors: bool,
) -> ExplainResult:
    split = ExplainSplitInfo(
        partition={},
        bucket=0,
        file_count=1,
        row_count=4,
        merged_row_count=None,
        file_size=128,
        raw_convertible=raw_convertible,
        has_deletion_vectors=has_deletion_vectors,
        level_histogram={0: 1},
        deletion_file_count=1 if has_deletion_vectors else 0,
        file_paths=["/tmp/fake.parquet"],
    )
    return ExplainResult(
        table_identifier=table_identifier,
        is_primary_key_table=False,
        bucket_mode="unaware",
        deletion_vectors_enabled=has_deletion_vectors,
        data_evolution_enabled=False,
        snapshot_id=1,
        schema_id=0,
        file_count=1,
        total_file_size=split.file_size,
        estimated_row_count=split.row_count,
        deletion_file_count=split.deletion_file_count,
        level_histogram=split.level_histogram,
        split_count=1,
        splits_raw_convertible=1 if raw_convertible else 0,
        splits_with_deletion_vectors=1 if has_deletion_vectors else 0,
        files_per_split_min=1,
        files_per_split_max=1,
        files_per_split_avg=1.0,
        split_size_min=split.file_size,
        split_size_max=split.file_size,
        split_size_avg=float(split.file_size),
        split_size_p50=split.file_size,
        split_size_p95=split.file_size,
        splits=[split],
    )


def test_explain_paimon_scan_reports_native_parquet_routing(catalog_options):
    pa_schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
    ])
    identifier, table = _create_table(
        catalog_options,
        "explain_native",
        pa_schema,
        options={"bucket": "-1", "file.format": "parquet"},
    )
    _write_arrow(table, pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}, schema=pa_schema))

    result = explain_paimon_scan(
        identifier,
        catalog_options,
        filters=col("id") == 2,
        columns=["name"],
        limit=1,
        verbose=True,
    )

    assert result.native_parquet_split_count == result.paimon_scan.split_count
    assert result.native_parquet_split_count > 0
    assert result.pypaimon_fallback_split_count == 0
    assert result.fallback_reasons == {}
    assert result.requested_columns == ["name"]
    assert result.requested_limit == 1
    assert result.source_limit == 1
    assert result.limit_pushed is True
    assert any("id" in pushed for pushed in result.pushed_filters)
    assert result.remaining_filters == []
    assert result.splits is not None
    assert all(split.reader_mode == READER_MODE_NATIVE_PARQUET for split in result.splits)
    assert "Daft Paimon Scan" in str(result)
    assert "PyPaimon Scan Plan" in str(result)


def test_explain_scan_keeps_limit_above_remaining_filters(catalog_options):
    pa_schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
    ])
    identifier, table = _create_table(
        catalog_options,
        "explain_remaining_filter",
        pa_schema,
        options={"bucket": "-1", "file.format": "parquet"},
    )
    _write_arrow(table, pa.table({"id": [1, 2], "name": ["a", "b"]}, schema=pa_schema))

    result = PaimonTable(table, catalog_options=catalog_options).explain_scan(
        filters=~((col("id") == 1) & (col("name") == "a")),
        limit=1,
    )

    assert result.native_parquet_split_count == result.paimon_scan.split_count
    assert result.pypaimon_fallback_split_count == 0
    assert result.pushed_filters == []
    assert any("id" in remaining for remaining in result.remaining_filters)
    assert result.source_limit is None
    assert result.limit_pushed is False
    assert result.splits is None
    assert result.paimon_scan.splits is None


def test_explain_scan_pushes_supported_not_and_limit(catalog_options):
    pa_schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
    ])
    identifier, table = _create_table(
        catalog_options,
        "explain_not_filter",
        pa_schema,
        options={"bucket": "-1", "file.format": "parquet"},
    )
    _write_arrow(table, pa.table({"id": [1, 2], "name": ["a", "b"]}, schema=pa_schema))

    result = PaimonTable(table, catalog_options=catalog_options).explain_scan(
        filters=~(col("id") == 1),
        limit=1,
    )

    assert result.native_parquet_split_count == result.paimon_scan.split_count
    assert result.pypaimon_fallback_split_count == 0
    assert any("!=" in pushed or "not" in pushed for pushed in result.pushed_filters)
    assert result.remaining_filters == []
    assert result.source_limit == 1
    assert result.limit_pushed is True
    assert result.splits is None
    assert result.paimon_scan.splits is None


def test_explain_scan_partially_pushes_conjuncts_and_keeps_limit(catalog_options):
    pa_schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
    ])
    identifier, table = _create_table(
        catalog_options,
        "explain_partial_conjunct",
        pa_schema,
        options={"bucket": "-1", "file.format": "parquet"},
    )
    _write_arrow(table, pa.table({"id": [1, 2], "name": ["alpha", "bravo"]}, schema=pa_schema))

    result = PaimonTable(table, catalog_options=catalog_options).explain_scan(
        filters=(col("id") == 1) & col("name").contains(col("id")),
        limit=1,
    )

    assert any("id" in pushed for pushed in result.pushed_filters)
    assert any("contains" in remaining for remaining in result.remaining_filters)
    assert result.source_limit is None
    assert result.limit_pushed is False


def test_explain_scan_applies_partition_filters_to_reader_counts(catalog_options):
    pa_schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
        ("dt", pa.string()),
    ])
    identifier, table = _create_table(
        catalog_options,
        "explain_partition_filter",
        pa_schema,
        partition_keys=["dt"],
        options={"bucket": "1", "file.format": "parquet"},
    )
    _write_arrow(
        table,
        pa.table({"id": [1], "name": ["a"], "dt": ["2024-01-01"]}, schema=pa_schema),
    )
    _write_arrow(
        table,
        pa.table({"id": [2], "name": ["b"], "dt": ["2024-01-02"]}, schema=pa_schema),
    )

    result = explain_paimon_scan(
        identifier,
        catalog_options,
        partition_filters=col("dt") == "2024-01-02",
        verbose=True,
    )

    assert result.paimon_scan.split_count == 2
    assert result.native_parquet_split_count == 1
    assert result.pypaimon_fallback_split_count == 0
    assert any("dt" in partition_filter for partition_filter in result.partition_filters)
    assert result.splits is not None
    assert len(result.splits) == 1
    assert result.splits[0].partition == {"dt": "2024-01-02"}


def test_explain_scan_reports_pk_lsm_fallback(catalog_options):
    pa_schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
        ("dt", pa.string()),
    ])
    _, table = _create_table(
        catalog_options,
        "explain_pk_fallback",
        pa_schema,
        partition_keys=["dt"],
        primary_keys=["id", "dt"],
        options={"bucket": "1", "file.format": "parquet"},
    )
    _write_arrow(
        table,
        pa.table({"id": [1, 2], "name": ["old-a", "old-b"], "dt": ["2024-01-01", "2024-01-01"]}, schema=pa_schema),
    )
    _write_arrow(
        table,
        pa.table({"id": [1], "name": ["new-a"], "dt": ["2024-01-01"]}, schema=pa_schema),
    )

    result = _explain_table(
        table,
        catalog_options=catalog_options,
        filters=col("id") == 1,
        columns=["name"],
        limit=1,
        verbose=True,
    )

    assert result.pypaimon_fallback_split_count > 0
    assert result.native_parquet_split_count == 0
    assert result.fallback_reasons["LSM merge required"] == result.pypaimon_fallback_split_count
    assert result.fallback_read_columns is not None
    assert "name" in result.fallback_read_columns
    assert "id" in result.fallback_read_columns
    assert result.splits is not None
    assert all(split.reader_mode == READER_MODE_PYPAIMON_FALLBACK for split in result.splits)
    assert all(split.fallback_reason == "LSM merge required" for split in result.splits)


def test_explain_scan_reports_non_parquet_fallback(catalog_options):
    pa_schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
    ])
    _, table = _create_table(
        catalog_options,
        "explain_avro_fallback",
        pa_schema,
        options={"bucket": "-1", "file.format": "avro"},
    )
    _write_arrow(table, pa.table({"id": [1], "name": ["a"]}, schema=pa_schema))

    result = _explain_table(table, catalog_options=catalog_options, verbose=True)

    assert result.pypaimon_fallback_split_count == result.paimon_scan.split_count
    assert result.pypaimon_fallback_split_count > 0
    assert result.native_parquet_split_count == 0
    assert result.fallback_reasons["non-parquet format"] == result.pypaimon_fallback_split_count
    assert result.splits is not None
    assert all(split.fallback_reason == "non-parquet format" for split in result.splits)


@requires_blob
def test_explain_scan_reports_blob_fallback(catalog_options):
    pa_schema = pa.schema([
        ("id", pa.int64()),
        ("payload", pa.large_binary()),
    ])
    _, table = _create_table(
        catalog_options,
        "explain_blob_fallback",
        pa_schema,
        options={
            "bucket": "-1",
            "file.format": "parquet",
            "row-tracking.enabled": "true",
            "data-evolution.enabled": "true",
        },
    )
    _write_arrow(table, pa.table({"id": [1], "payload": [b"hello"]}, schema=pa_schema))

    result = _explain_table(table, catalog_options=catalog_options, verbose=True)

    assert result.pypaimon_fallback_split_count == result.paimon_scan.split_count
    assert result.pypaimon_fallback_split_count > 0
    assert result.native_parquet_split_count == 0
    assert result.fallback_reasons["blob columns present"] == result.pypaimon_fallback_split_count
    assert result.splits is not None
    assert all(split.reader_mode == READER_MODE_PYPAIMON_FALLBACK for split in result.splits)
    assert all(split.fallback_reason == "blob columns present" for split in result.splits)


def test_explain_scan_reports_deletion_vector_fallback(catalog_options, monkeypatch):
    pa_schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
    ])
    _, table = _create_table(
        catalog_options,
        "explain_deletion_vector_fallback",
        pa_schema,
        options={"bucket": "-1", "file.format": "parquet"},
    )

    class FakeReadBuilder:
        def explain(self, verbose: bool = False) -> ExplainResult:
            assert verbose is True
            return _single_split_explain(
                table_identifier="test_db.explain_deletion_vector_fallback",
                raw_convertible=True,
                has_deletion_vectors=True,
            )

    def fake_scan_read_builder(self, table, read_pushdowns):
        return FakeReadBuilder()

    monkeypatch.setattr(PaimonDataSource, "_scan_read_builder", fake_scan_read_builder)

    result = _explain_table(table, catalog_options=catalog_options, verbose=True)

    assert result.pypaimon_fallback_split_count == 1
    assert result.native_parquet_split_count == 0
    assert result.fallback_reasons == {"deletion vectors present": 1}
    assert result.splits is not None
    assert len(result.splits) == 1
    assert result.splits[0].reader_mode == READER_MODE_PYPAIMON_FALLBACK
    assert result.splits[0].fallback_reason == "deletion vectors present"
