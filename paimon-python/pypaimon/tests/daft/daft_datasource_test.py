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
import unittest
from dataclasses import dataclass
from typing import Optional

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from pypaimon.daft.daft_datasource import (
    PaimonDataSource,
    _native_read_fields_compatible,
    _native_read_types_compatible,
    _promote_time32_schema_for_daft,
)
from pypaimon.schema.data_types import (
    ArrayType,
    AtomicType,
    DataField,
    MapType,
    RowType,
    VectorType,
)


@dataclass
class _DataFile:
    file_path: str
    external_path: Optional[str] = None


@dataclass
class _TableSchema:
    id: int
    fields: list[DataField]


class _SchemaManager:
    def __init__(self, schemas: list[_TableSchema]):
        self._schemas = {schema.id: schema for schema in schemas}

    def get_schema(self, schema_id: int) -> _TableSchema:
        return self._schemas[schema_id]


@dataclass
class _TableWithSchemas:
    table_schema: _TableSchema
    schema_manager: _SchemaManager


def _build_uri(warehouse_scheme: str, file_path: str) -> str:
    class _Stub:
        pass
    stub = _Stub()
    stub._warehouse_scheme = warehouse_scheme
    return PaimonDataSource._build_file_uri(stub, file_path)


class BuildFileUriTest(unittest.TestCase):

    def test_passes_through_when_path_already_has_scheme(self):
        cases = [
            ("",     "oss://bucket/db.db/tbl/data.parquet"),
            ("",     "s3://bucket/key.parquet"),
            ("",     "s3a://bucket/key.parquet"),
            ("",     "s3n://bucket/key.parquet"),
            ("",     "hdfs://nameservice/path/data.parquet"),
            ("file", "file:///abs/path/data.parquet"),
            ("oss",  "oss://bucket/db.db/tbl/data.parquet"),
            ("",     "oss://clg-paimon-fe4767/db.db/tbl/bucket-0/data-0.parquet"),
        ]
        for warehouse_scheme, file_path in cases:
            with self.subTest(warehouse_scheme=warehouse_scheme, file_path=file_path):
                self.assertEqual(_build_uri(warehouse_scheme, file_path), file_path)

    def test_adds_warehouse_scheme_when_path_unschemed(self):
        self.assertEqual(
            _build_uri("oss", "bucket/db.db/tbl/data.parquet"),
            "oss://bucket/db.db/tbl/data.parquet",
        )

    def test_defaults_to_file_scheme_when_both_unschemed(self):
        self.assertEqual(
            _build_uri("", "/tmp/pytest-xxx/db.db/tbl/data.parquet"),
            "file:///tmp/pytest-xxx/db.db/tbl/data.parquet",
        )


class DataFilePathTest(unittest.TestCase):

    def test_prefers_external_path(self):
        data_file = _DataFile(
            file_path="file:///warehouse/db.db/tbl/bucket-0/data.parquet",
            external_path="s3://external-bucket/data/db.db/tbl/bucket-0/data.parquet",
        )

        self.assertEqual(
            PaimonDataSource._data_file_path(data_file),
            "s3://external-bucket/data/db.db/tbl/bucket-0/data.parquet",
        )

    def test_falls_back_to_file_path(self):
        data_file = _DataFile(
            file_path="file:///warehouse/db.db/tbl/bucket-0/data.parquet",
            external_path=None,
        )

        self.assertEqual(
            PaimonDataSource._data_file_path(data_file),
            "file:///warehouse/db.db/tbl/bucket-0/data.parquet",
        )


@pytest.mark.parametrize(
    ("file_fields", "current_fields"),
    [
        pytest.param(
            [DataField(0, "id", AtomicType("INT"))],
            [
                DataField(0, "id", AtomicType("INT")),
                DataField(1, "added", AtomicType("STRING")),
            ],
            id="add-nullable-field",
        ),
        pytest.param(
            [
                DataField(0, "id", AtomicType("INT")),
                DataField(1, "removed", AtomicType("STRING")),
            ],
            [DataField(0, "id", AtomicType("INT"))],
            id="drop-field",
        ),
        pytest.param(
            [
                DataField(0, "id", AtomicType("INT")),
                DataField(1, "value", AtomicType("STRING")),
            ],
            [
                DataField(1, "value", AtomicType("STRING")),
                DataField(0, "id", AtomicType("INT")),
            ],
            id="reorder-fields",
        ),
    ],
)
def test_native_read_fields_allow_name_aligned_evolution(
    file_fields,
    current_fields,
):
    assert _native_read_fields_compatible(file_fields, current_fields)


@pytest.mark.parametrize(
    ("file_field", "current_field"),
    [
        pytest.param(
            DataField(1, "value", AtomicType("STRING")),
            DataField(1, "renamed", AtomicType("STRING")),
            id="rename",
        ),
        pytest.param(
            DataField(1, "value", AtomicType("STRING")),
            DataField(2, "value", AtomicType("STRING")),
            id="drop-then-readd",
        ),
    ],
)
def test_native_read_fields_reject_field_id_mapping(file_field, current_field):
    assert not _native_read_fields_compatible([file_field], [current_field])


@pytest.mark.parametrize(
    ("file_type", "current_type", "expected"),
    [
        pytest.param(
            AtomicType("INT", nullable=False),
            AtomicType("INT", nullable=True),
            True,
            id="relax-nullability",
        ),
        pytest.param(
            AtomicType("INT"),
            AtomicType("BIGINT"),
            True,
            id="promote-int",
        ),
        pytest.param(
            AtomicType("INT"),
            AtomicType("INT", nullable=False),
            False,
            id="tighten-nullability",
        ),
        pytest.param(
            AtomicType("BIGINT"),
            AtomicType("INT"),
            False,
            id="narrow-int",
        ),
        pytest.param(
            AtomicType("DECIMAL(10, 4)"),
            AtomicType("DECIMAL(10, 2)"),
            False,
            id="decimal-scale-down",
        ),
        pytest.param(
            AtomicType("TIMESTAMP(3)"),
            AtomicType("TIME(3)"),
            False,
            id="timestamp-to-time",
        ),
        pytest.param(
            AtomicType("TIME(3)"),
            AtomicType("TIME(3)"),
            False,
            id="unsupported-time",
        ),
    ],
)
def test_native_read_atomic_type_compatibility(file_type, current_type, expected):
    assert _native_read_types_compatible(file_type, current_type) is expected


@pytest.mark.parametrize(
    ("file_type", "current_type"),
    [
        pytest.param(
            RowType(True, [DataField(1, "value", AtomicType("INT"))]),
            RowType(True, [DataField(1, "value", AtomicType("BIGINT"))]),
            id="row",
        ),
        pytest.param(
            ArrayType(True, AtomicType("INT")),
            ArrayType(True, AtomicType("BIGINT")),
            id="array",
        ),
        pytest.param(
            MapType(True, AtomicType("STRING"), AtomicType("INT")),
            MapType(True, AtomicType("STRING"), AtomicType("BIGINT")),
            id="map",
        ),
        pytest.param(
            VectorType(True, AtomicType("INT"), 3),
            VectorType(True, AtomicType("BIGINT"), 3),
            id="vector",
        ),
    ],
)
def test_native_read_types_allow_recursive_promotion(file_type, current_type):
    assert _native_read_types_compatible(file_type, current_type)


def test_native_read_types_allow_nested_name_aligned_evolution():
    file_fields = [
        DataField(1, "first", AtomicType("INT")),
        DataField(2, "second", AtomicType("STRING")),
    ]
    current_fields = [
        DataField(2, "second", AtomicType("STRING")),
        DataField(1, "first", AtomicType("INT")),
    ]

    assert _native_read_types_compatible(
        RowType(True, file_fields),
        RowType(True, current_fields),
    )


def _identity(data_type):
    return data_type


def _array(data_type):
    return ArrayType(True, data_type)


def _map_value(data_type):
    return MapType(True, AtomicType("STRING"), data_type)


@pytest.mark.parametrize(
    ("wrap", "file_child", "current_child"),
    [
        pytest.param(
            _identity,
            DataField(1, "value", AtomicType("INT")),
            DataField(1, "renamed", AtomicType("INT")),
            id="row-nested-rename",
        ),
        pytest.param(
            _array,
            DataField(1, "value", AtomicType("INT")),
            DataField(2, "value", AtomicType("INT")),
            id="array-row-nested-drop-then-readd",
        ),
        pytest.param(
            _map_value,
            DataField(1, "value", AtomicType("INT")),
            DataField(1, "renamed", AtomicType("INT")),
            id="map-row-nested-rename",
        ),
    ],
)
def test_native_read_types_reject_nested_field_id_mapping(
    wrap,
    file_child,
    current_child,
):
    file_type = wrap(RowType(True, [file_child]))
    current_type = wrap(RowType(True, [current_child]))

    assert not _native_read_types_compatible(file_type, current_type)


def test_native_read_types_reject_vector_length_change():
    assert not _native_read_types_compatible(
        VectorType(True, AtomicType("INT"), 3),
        VectorType(True, AtomicType("INT"), 4),
    )


def _row_value(data_type):
    return RowType(True, [DataField(1, "value", data_type)])


@pytest.mark.parametrize(
    ("wrap", "file_type", "current_type"),
    [
        pytest.param(
            _row_value,
            AtomicType("DECIMAL(10, 4)"),
            AtomicType("DECIMAL(10, 2)"),
            id="row-decimal-scale-down",
        ),
        pytest.param(
            _array,
            AtomicType("TIMESTAMP(3)"),
            AtomicType("TIME(3)"),
            id="array-timestamp-to-time",
        ),
        pytest.param(
            _map_value,
            AtomicType("DECIMAL(10, 4)"),
            AtomicType("DECIMAL(10, 2)"),
            id="map-decimal-scale-down",
        ),
    ],
)
def test_native_read_types_reject_semantic_mismatch_recursively(
    wrap,
    file_type,
    current_type,
):
    assert not _native_read_types_compatible(
        wrap(file_type),
        wrap(current_type),
    )


def test_promote_time32_schema_for_daft_recursively():
    schema = pa.schema(
        [
            ("value", pa.time32("ms")),
            ("row", pa.struct([("value", pa.time32("ms"))])),
            ("array", pa.list_(pa.time32("ms"))),
            ("map", pa.map_(pa.string(), pa.time32("ms"))),
        ]
    )

    assert _promote_time32_schema_for_daft(schema) == pa.schema(
        [
            ("value", pa.time64("us")),
            ("row", pa.struct([("value", pa.time64("us"))])),
            ("array", pa.list_(pa.time64("us"))),
            ("map", pa.map_(pa.string(), pa.time64("us"))),
        ]
    )


def test_schema_compatibility_uses_task_columns_for_mixed_schema_split():
    old_schema = _TableSchema(
        0,
        [
            DataField(0, "id", AtomicType("BIGINT")),
            DataField(1, "value", AtomicType("STRING")),
        ],
    )
    current_schema = _TableSchema(
        1,
        [
            DataField(0, "id", AtomicType("BIGINT")),
            DataField(1, "renamed", AtomicType("STRING")),
        ],
    )
    table = _TableWithSchemas(
        current_schema,
        _SchemaManager([old_schema, current_schema]),
    )

    assert not PaimonDataSource._has_incompatible_file_schema(
        table,
        [current_schema.id, old_schema.id],
        ["id"],
        {},
    )
    assert PaimonDataSource._has_incompatible_file_schema(
        table,
        [current_schema.id, old_schema.id],
        ["renamed"],
        {},
    )


if __name__ == "__main__":
    unittest.main()
