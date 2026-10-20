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

"""Verify DATE partition paths match Java and preserve historical reads."""

from datetime import date
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.common.identifier import Identifier
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.utils.file_store_path_factory import FileStorePathFactory
from pypaimon.write.writer.data_writer import DataWriter


def _create_table(
    tmp_path, legacy_partition_name=True, composite_partition=False, table_options=None
):
    table_path = str(tmp_path / "table")
    file_io = LocalFileIO()
    fields = [("id", pa.int64()), ("day", pa.date32())]
    partition_keys = ["day"]
    if composite_partition:
        fields.append(("region", pa.string()))
        partition_keys.append("region")
    arrow_schema = pa.schema(fields)
    options = {
        "partition.legacy-name": str(legacy_partition_name).lower(),
        "scan.native-plan.enabled": "false",
        "write.native.enabled": "false",
        "commit.native.enabled": "false",
    }
    options.update(table_options or {})
    schema = Schema.from_pyarrow_schema(
        arrow_schema,
        partition_keys=partition_keys,
        options=options,
    )
    table_schema = SchemaManager(file_io, table_path).create_table(schema)
    table = FileStoreTable(
        file_io,
        Identifier.create("default", "t"),
        table_path,
        table_schema,
    )
    return table, arrow_schema


def _write_rows(table, arrow_schema, rows):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table(
            {
                name: [row[name] for row in rows]
                for name in arrow_schema.names
            },
            schema=arrow_schema,
        ))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _write_one_row(table, arrow_schema):
    _write_rows(table, arrow_schema, [{"id": 1, "day": date(1970, 1, 2)}])


def _write_legacy_composite_row(table, arrow_schema, row):
    historical_bucket = (
        Path(table.table_path)
        / "day=1970-01-02"
        / "region=a"
        / "b"
        / "bucket-0"
    )
    with patch.object(
        DataWriter,
        "_generate_file_path",
        lambda writer, file_name: str(historical_bucket / file_name),
    ):
        _write_rows(table, arrow_schema, [row])


def _write_legacy_date_row(table, arrow_schema, row):
    historical_bucket = Path(table.table_path) / "day=1970-01-02" / "bucket-0"
    with patch.object(
        DataWriter,
        "_generate_file_path",
        lambda writer, file_name: str(historical_bucket / file_name),
    ):
        _write_rows(table, arrow_schema, [row])


def _read_rows(table):
    builder = table.new_read_builder()
    splits = builder.new_scan().plan().splits()
    result = builder.new_read().to_arrow(splits)
    columns = result.to_pydict()
    return [
        {name: columns[name][index] for name in result.column_names}
        for index in range(result.num_rows)
    ]


def test_legacy_date_partition_write_uses_java_epoch_day(tmp_path):
    table, arrow_schema = _create_table(tmp_path)

    _write_one_row(table, arrow_schema)

    partition_directories = sorted(
        path.name for path in Path(table.table_path).glob("day=*")
    )
    assert partition_directories == ["day=1"]
    assert _read_rows(table) == [{"id": 1, "day": date(1970, 1, 2)}]


@pytest.mark.python_plan
@pytest.mark.python_read
@pytest.mark.python_write
@pytest.mark.python_commit
def test_legacy_python_date_partition_directory_remains_readable(tmp_path):
    table, arrow_schema = _create_table(tmp_path)

    _write_legacy_date_row(
        table, arrow_schema, {"id": 1, "day": date(1970, 1, 2)}
    )
    historical_directory = Path(table.table_path) / "day=1970-01-02"
    assert historical_directory.joinpath("bucket-0").is_dir()

    assert _read_rows(table) == [{"id": 1, "day": date(1970, 1, 2)}]


def test_abort_removes_serialized_date_partition_file(tmp_path):
    from pypaimon.write.commit_message_serializer import (
        deserialize_commit_message,
        serialize_commit_message,
    )

    table, arrow_schema = _create_table(tmp_path)
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_arrow(pa.Table.from_pylist(
            [{"id": 1, "day": date(1970, 1, 2)}], schema=arrow_schema
        ))
        messages = writer.prepare_commit()
    finally:
        writer.close()

    file_paths = [
        file.file_path
        for message in messages
        for file in message.new_files
    ]
    assert file_paths
    assert all(table.file_io.exists(path) for path in file_paths)

    restored_messages = [
        deserialize_commit_message(
            serialize_commit_message(message, table.partition_keys_fields),
            table.partition_keys_fields,
        )
        for message in messages
    ]
    assert all(
        file.file_path is None
        for message in restored_messages
        for file in message.new_files
    )

    commit = builder.new_commit()
    try:
        commit.abort(restored_messages)
    finally:
        commit.close()

    assert not any(table.file_io.exists(path) for path in file_paths)


def test_non_legacy_date_partition_write_keeps_iso_name(tmp_path):
    table, arrow_schema = _create_table(tmp_path, legacy_partition_name=False)

    _write_one_row(table, arrow_schema)

    partition_directories = sorted(
        path.name for path in Path(table.table_path).glob("day=*")
    )
    assert partition_directories == ["day=1970-01-02"]


def test_legacy_composite_date_write_reads_back_from_canonical_path(tmp_path):
    table, arrow_schema = _create_table(tmp_path, composite_partition=True)

    _write_rows(table, arrow_schema, [{
        "id": 1, "day": date(1970, 1, 2), "region": "a/b",
    }])

    assert _read_rows(table) == [{
        "id": 1, "day": date(1970, 1, 2), "region": "a/b",
    }]
    assert (Path(table.table_path) / "day=1" / "region=a%2Fb" / "bucket-0").is_dir()


@pytest.mark.python_plan
@pytest.mark.python_read
@pytest.mark.python_write
@pytest.mark.python_commit
def test_old_and_new_composite_partition_files_coexist_after_append(tmp_path):
    table, arrow_schema = _create_table(tmp_path, composite_partition=True)
    row = {"day": date(1970, 1, 2), "region": "a/b"}

    _write_legacy_composite_row(table, arrow_schema, {"id": 1, **row})
    first_snapshot_id = table.snapshot_manager().get_latest_snapshot().id
    historical_bucket = (
        Path(table.table_path)
        / "day=1970-01-02"
        / "region=a"
        / "b"
        / "bucket-0"
    )
    assert historical_bucket.is_dir()

    _write_rows(table, arrow_schema, [{"id": 2, **row}])
    canonical_directory = Path(table.table_path) / "day=1" / "region=a%2Fb"

    assert {item["id"] for item in _read_rows(table)} == {1, 2}
    assert canonical_directory.joinpath("bucket-0").is_dir()

    table.rollback_to(first_snapshot_id)

    assert [item["id"] for item in _read_rows(table)] == [1]


@pytest.mark.python_plan
@pytest.mark.python_read
@pytest.mark.python_write
@pytest.mark.python_commit
@pytest.mark.parametrize("historical_layout", [False, True])
def test_composite_date_path_lookups_are_batched_across_split_packs(
    tmp_path, monkeypatch, historical_layout
):
    table, arrow_schema = _create_table(
        tmp_path,
        composite_partition=True,
        table_options={
            CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(): "1b",
            CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(): "1b",
        },
    )
    row = {"day": date(1970, 1, 2), "region": "a/b"}

    for row_id in (1, 2):
        data = {"id": row_id, **row}
        if historical_layout:
            _write_legacy_composite_row(table, arrow_schema, data)
        else:
            _write_rows(table, arrow_schema, [data])

    lookup_batches = []
    file_io = table.file_io
    exists_batch = file_io.exists_batch

    def track_exists_batch(paths):
        lookup_batches.append(list(paths))
        return exists_batch(paths)

    monkeypatch.setattr(file_io, "exists_batch", track_exists_batch)
    splits = table.new_read_builder().new_scan().plan().splits()

    assert len(splits) == 2
    assert sum(len(split.files) for split in splits) == 2
    expected_batch_sizes = [2, 2] if historical_layout else [2]
    assert [len(paths) for paths in lookup_batches] == expected_batch_sizes
    assert {item["id"] for item in _read_rows(table)} == {1, 2}


def test_non_legacy_composite_date_write_uses_canonical_escaping(tmp_path):
    table, arrow_schema = _create_table(
        tmp_path, legacy_partition_name=False, composite_partition=True)

    _write_rows(table, arrow_schema, [{
        "id": 1, "day": date(1970, 1, 2), "region": "a/b",
    }])

    assert _read_rows(table) == [{
        "id": 1, "day": date(1970, 1, 2), "region": "a/b",
    }]
    assert (Path(table.table_path) / "day=1970-01-02" / "region=a%2Fb" / "bucket-0").is_dir()


def test_date_compatibility_does_not_reformat_other_partition_types():
    factory = FileStorePathFactory(
        "/table",
        ["day", "region"],
        "__DEFAULT_PARTITION__",
        "parquet",
        "data-",
        "changelog-",
        True,
        False,
        None,
        partition_types=[AtomicType("DATE"), AtomicType("STRING")],
    )

    assert factory.relative_bucket_path(
        (date(1970, 1, 2), "a/b"), 0, canonical_partition=True
    ) == "day=1/region=a%2Fb/bucket-0"


def test_date_compatibility_applies_to_external_data_and_bucket_index_paths():
    partition = (date(1970, 1, 2),)
    factory = FileStorePathFactory(
        "/table",
        ["day"],
        "__DEFAULT_PARTITION__",
        "parquet",
        "data-",
        "changelog-",
        True,
        False,
        None,
        external_paths=["/external"],
        external_path_strategy="round-robin",
        index_file_in_data_file_dir=True,
        partition_types=[AtomicType("DATE")],
    )

    external_provider = factory.create_external_path_provider(
        partition, 0, canonical_partition=True)
    assert external_provider.get_next_external_data_path("data.parquet") == (
        "/external/day=1/bucket-0/data.parquet"
    )
    assert factory.new_bucket_index_path(partition, 0, "index-1") == (
        "/external/day=1/bucket-0/index-1",
        True,
    )
