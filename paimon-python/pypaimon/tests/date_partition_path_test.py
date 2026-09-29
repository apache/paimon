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

"""验证 DATE 分区路径与 Java 命名规则的一致性及历史目录兼容性。"""

from datetime import date
from pathlib import Path

import pyarrow as pa

from pypaimon.common.identifier import Identifier
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.utils.file_store_path_factory import FileStorePathFactory


def _create_table(tmp_path, legacy_partition_name=True):
    table_path = str(tmp_path / "table")
    file_io = LocalFileIO()
    arrow_schema = pa.schema([("id", pa.int64()), ("day", pa.date32())])
    schema = Schema.from_pyarrow_schema(
        arrow_schema,
        partition_keys=["day"],
        options={
            "partition.legacy-name": str(legacy_partition_name).lower(),
            "scan.native-plan.enabled": "false",
            "write.native.enabled": "false",
        },
    )
    table_schema = SchemaManager(file_io, table_path).create_table(schema)
    table = FileStoreTable(
        file_io,
        Identifier.create("default", "t"),
        table_path,
        table_schema,
    )
    return table, arrow_schema


def _write_one_row(table, arrow_schema):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(
            pa.table(
                {"id": [1], "day": [date(1970, 1, 2)]},
                schema=arrow_schema,
            )
        )
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _read_rows(table):
    builder = table.new_read_builder()
    splits = builder.new_scan().plan().splits()
    return builder.new_read().to_arrow(splits).to_pylist()


def test_legacy_date_partition_write_uses_java_epoch_day(tmp_path):
    table, arrow_schema = _create_table(tmp_path)

    _write_one_row(table, arrow_schema)

    partition_directories = sorted(
        path.name for path in Path(table.table_path).glob("day=*")
    )
    assert partition_directories == ["day=1"]
    assert _read_rows(table) == [{"id": 1, "day": date(1970, 1, 2)}]


def test_legacy_python_date_partition_directory_remains_readable(tmp_path):
    table, arrow_schema = _create_table(tmp_path)

    _write_one_row(table, arrow_schema)
    canonical_directory = Path(table.table_path) / "day=1"
    historical_directory = Path(table.table_path) / "day=1970-01-02"
    canonical_directory.rename(historical_directory)

    assert _read_rows(table) == [{"id": 1, "day": date(1970, 1, 2)}]


def test_non_legacy_date_partition_write_keeps_iso_name(tmp_path):
    table, arrow_schema = _create_table(tmp_path, legacy_partition_name=False)

    _write_one_row(table, arrow_schema)

    partition_directories = sorted(
        path.name for path in Path(table.table_path).glob("day=*")
    )
    assert partition_directories == ["day=1970-01-02"]


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

    assert factory.data_file_relative_bucket_path(
        (date(1970, 1, 2), "a/b"), 0
    ) == "day=1/region=a/b/bucket-0"


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

    external_provider = factory.create_external_path_provider(partition, 0)
    assert external_provider.get_next_external_data_path("data.parquet") == (
        "/external/day=1/bucket-0/data.parquet"
    )
    assert factory.new_bucket_index_path(partition, 0, "index-1") == (
        "/external/day=1/bucket-0/index-1",
        True,
    )
