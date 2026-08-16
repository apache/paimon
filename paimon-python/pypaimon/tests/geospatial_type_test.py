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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pyarrow
import pyarrow.parquet as parquet
import pytest

from pypaimon.casting.data_type_casts import supports_cast
from pypaimon.schema.data_types import DataField
from pypaimon.schema.data_types import DataTypeParser
from pypaimon.schema.data_types import EdgeAlgorithm
from pypaimon.schema.data_types import GeographyType
from pypaimon.schema.data_types import GeometryType
from pypaimon.schema.data_types import ArrayType
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.schema.data_types import RowType
from pypaimon.schema.schema_manager import _validate_geospatial_fields
from pypaimon.write.writer.data_writer import DataWriter


def test_iceberg_compatible_type_round_trip():
    geometry = DataTypeParser.parse_data_type("GEOMETRY(ogc:crs84) NOT NULL")
    geography = DataTypeParser.parse_data_type("GEOGRAPHY(EPSG:4326, karney)")

    assert geometry == GeometryType("OGC:CRS84", nullable=False)
    assert geography == GeographyType("EPSG:4326", EdgeAlgorithm.KARNEY)
    assert DataTypeParser.parse_data_type(geometry.to_dict()) == geometry
    assert DataTypeParser.parse_data_type(geography.to_dict()) == geography
    custom = GeometryType("custom, crs's definition")
    assert DataTypeParser.parse_data_type(custom.to_dict()) == custom


def test_defaults_and_invalid_parameters():
    assert str(DataTypeParser.parse_data_type("GEOMETRY")) == "GEOMETRY(OGC:CRS84)"
    assert str(DataTypeParser.parse_data_type("GEOGRAPHY")) == \
        "GEOGRAPHY(OGC:CRS84, spherical)"

    with pytest.raises(ValueError, match="Invalid edge interpolation algorithm"):
        DataTypeParser.parse_data_type("GEOGRAPHY(OGC:CRS84, rhumb)")
    with pytest.raises(ValueError, match="Invalid geometry type"):
        DataTypeParser.parse_data_type("GEOMETRY(EPSG:4326) trailing")


def test_pyarrow_uses_wkb_binary_and_preserves_type_metadata():
    fields = [
        DataField(0, "geom", GeometryType()),
        DataField(1, "geog", GeographyType("EPSG:4326", EdgeAlgorithm.VINCENTY,
                                            nullable=False)),
    ]

    arrow_schema = PyarrowFieldParser.from_paimon_schema(fields)
    assert arrow_schema.field("geom").type == pyarrow.binary()
    assert arrow_schema.field("geog").type == pyarrow.binary()
    assert arrow_schema.field("geom").metadata[b'paimon.type'] == \
        b'GEOMETRY(OGC:CRS84)'
    assert PyarrowFieldParser.to_paimon_schema(arrow_schema) == fields


def test_nested_arrow_and_parquet_wkb_round_trip(tmp_path):
    fields = [
        DataField(0, "nested", RowType(True, [
            DataField(1, "geom", GeometryType(nullable=False)),
        ])),
        DataField(2, "geographies", ArrayType(True, GeographyType())),
    ]
    schema = PyarrowFieldParser.from_paimon_schema(fields)
    assert schema.field("nested").type.field("geom").metadata[b'paimon.type'] == \
        b'GEOMETRY(OGC:CRS84) NOT NULL'
    assert schema.field("geographies").type.value_field.metadata[b'paimon.type'] == \
        b'GEOGRAPHY(OGC:CRS84, spherical)'
    assert PyarrowFieldParser.to_paimon_schema(schema) == fields

    point_wkb = bytes.fromhex(
        "0101000000000000000000f03f0000000000000040")
    table = pyarrow.Table.from_arrays([
        pyarrow.array([{"geom": point_wkb}], type=schema.field("nested").type),
        pyarrow.array([[point_wkb]], type=schema.field("geographies").type),
    ], schema=schema)
    path = tmp_path / "geo.parquet"
    parquet.write_table(table, path)
    restored = parquet.read_table(path)
    assert restored.to_pylist() == table.to_pylist()
    assert PyarrowFieldParser.to_paimon_schema(restored.schema) == fields


def test_geospatial_cast_stats_and_schema_validation():
    assert supports_cast(GeometryType("OGC:CRS84"),
                         GeometryType("ogc:crs84", nullable=False))
    assert not supports_cast(GeometryType(), GeometryType("EPSG:3857"))
    assert not supports_cast(
        GeographyType(algorithm=EdgeAlgorithm.SPHERICAL),
        GeographyType(algorithm=EdgeAlgorithm.KARNEY))

    fields = [DataField(0, "geom", GeometryType())]
    values = pyarrow.table({"geom": [b'\x02', None, b'\x01']})
    stats_fields = DataWriter._resolve_stats_fields(values.schema, fields)
    assert stats_fields == fields
    stats = DataWriter._get_column_stats(values, "geom", GeometryType())
    assert stats == {"min_values": None, "max_values": None, "null_counts": 1}

    _validate_geospatial_fields(fields, {}, [], [])
    with pytest.raises(ValueError, match="file.format"):
        _validate_geospatial_fields(fields, {"file.format": "orc"}, [], [])
    with pytest.raises(ValueError, match="primary keys"):
        _validate_geospatial_fields(fields, {}, ["geom"], [])
    with pytest.raises(ValueError, match="format-version"):
        _validate_geospatial_fields(
            fields, {"metadata.iceberg.storage": "table-location"}, [], [])
    _validate_geospatial_fields(
        fields,
        {"metadata.iceberg.storage": "table-location",
         "metadata.iceberg.format-version": "3"},
        [], [])
