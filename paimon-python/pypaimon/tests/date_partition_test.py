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

from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.scanner.append_table_split_generator import AppendTableSplitGenerator

pytestmark = pytest.mark.python_plan


def _create_table(tmp_path, options=None, day_type=pa.date32(), primary_keys=None):
    catalog = CatalogFactory.create({"warehouse": str(tmp_path)})
    catalog.create_database("default", False)
    schema = pa.schema([("id", pa.int64()), ("day", day_type)])
    catalog.create_table(
        "default.t",
        Schema.from_pyarrow_schema(
            schema,
            partition_keys=["day"],
            primary_keys=primary_keys,
            options=options or {},
        ),
        False,
    )
    return catalog.get_table("default.t"), schema


def _write(table, schema, rows):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        messages = writer.prepare_commit()
        paths = [
            file.external_path or file.file_path
            for message in messages
            for file in list(message.new_files) + list(message.changelog_files)
        ]
        assert paths and all(table.file_io.exists(path) for path in paths)
        commit.commit(messages)
        return paths
    finally:
        writer.close()
        commit.close()


def _read(table):
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by("id")


@pytest.mark.parametrize(
    "mode", ["append", "pk", "de", "chunk", "de-chunk", "external"]
)
@pytest.mark.parametrize("legacy", [None, "false"])
def test_date_partition_round_trip(tmp_path, mode, legacy):
    options = {"partition.default-name": "NULL_DAY"}
    if legacy is not None:
        options["partition.legacy-name"] = legacy
    if mode == "pk":
        options.update({"bucket": "1", "changelog-producer": "input"})
    if mode.startswith("de"):
        options.update(
            {"data-evolution.enabled": "true", "row-tracking.enabled": "true"}
        )
    if mode == "external":
        options["data-file.external-paths"] = (tmp_path / "external").as_uri()
        options["data-file.external-paths.strategy"] = "round-robin"
    table, schema = _create_table(
        tmp_path, options, primary_keys=["id", "day"] if mode == "pk" else None
    )
    dates = [date(1969, 12, 31), date(1970, 1, 1), date(2000, 2, 29)]
    names = ["-1", "0", "11016"] if legacy is None else [str(day) for day in dates]
    if mode != "pk":
        dates.append(None)
        names.append("NULL_DAY")
    rows = [{"id": i, "day": day} for i, day in enumerate(dates)]
    paths = _write(table, schema, rows)
    # Assert directory names independently: a reader and writer sharing the
    # same wrong formatter must not make the round-trip test pass.
    assert {Path(path).parent.parent.name for path in paths} == {
        "day=" + name for name in names
    }
    if mode == "pk":
        assert any("/changelog-" in path for path in paths)
    builder = table.new_read_builder()
    scan = builder.new_scan()
    if "chunk" in mode:
        scan.with_chunk_shuffle(42, 1)
    with patch.object(
        table.file_io, "list_status", wraps=table.file_io.list_status
    ) as listing:
        splits = scan.plan().splits()
    assert all("/day=" not in str(call.args[0]) for call in listing.call_args_list)
    if mode == "external":
        assert all(file.external_path for split in splits for file in split.files)
    actual = builder.new_read().to_arrow(splits).sort_by("id")
    assert actual.to_pylist() == rows
    assert actual["day"].type == pa.date32()


@pytest.mark.parametrize(
    "kind,value",
    [
        (pa.string(), "1970-01-02"),
        (pa.int32(), 19700102),
        (pa.timestamp("us"), datetime(2026, 8, 28, 12, 30)),
    ],
)
def test_non_date_partition_keeps_existing_path(tmp_path, kind, value):
    table, schema = _create_table(tmp_path, day_type=kind)
    rows = [{"id": 1, "day": value}]
    paths = _write(table, schema, rows)
    assert {Path(path).parent.parent.name for path in paths} == {"day=" + str(value)}
    assert _read(table).to_pylist() == rows


def test_explicit_data_file_path_is_preserved(tmp_path):
    table, schema = _create_table(tmp_path)
    rows = [{"id": 1, "day": date(1970, 1, 1)}]
    _write(table, schema, rows)
    builder = table.new_read_builder()
    split = builder.new_scan().plan().splits()[0]
    file = split.files[0]
    destination = tmp_path / "explicit.parquet"
    Path(file.file_path).rename(destination)
    file.file_path = str(destination)
    entry = ManifestEntry(
        kind=0,
        partition=split.partition,
        bucket=split.bucket,
        total_buckets=1,
        file=file,
    )
    splits = AppendTableSplitGenerator(table, 1024 * 1024, 0).create_splits([entry])
    assert builder.new_read().to_arrow(splits).to_pylist() == rows


def test_date_deletion_vector_uses_data_directory(tmp_path):
    from pypaimon.manifest.index_manifest_file import IndexManifestFile

    table, schema = _create_table(
        tmp_path,
        {
            "data-evolution.enabled": "true",
            "row-tracking.enabled": "true",
            "deletion-vectors.enabled": "true",
            "index-file-in-data-file-dir": "true",
        },
    )
    rows = [{"id": i, "day": date(1970, 1, 1)} for i in range(2)]
    _write(table, schema, rows)
    builder = table.new_batch_write_builder()
    commit = builder.new_commit()
    try:
        commit.commit(builder.new_update().delete_by_row_id([0]))
    finally:
        commit.close()
    assert _read(table).to_pylist() == rows[1:]
    snapshot = table.snapshot_manager().get_latest_snapshot()
    entries = IndexManifestFile(table).read(snapshot.index_manifest)
    assert entries
    for entry in entries:
        path = table.path_factory().bucket_index_path(
            tuple(entry.partition.values), entry.bucket, entry.index_file, table.file_io
        )
        assert "/day=0/bucket-0/" in path and table.file_io.exists(path)
