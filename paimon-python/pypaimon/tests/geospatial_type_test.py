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

import builtins
import json
import sys
from enum import Enum
from types import ModuleType

import pyarrow
import pytest

from pypaimon.casting.data_type_casts import supports_cast
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.data_types import DataField
from pypaimon.schema.data_types import DataTypeParser
from pypaimon.schema.data_types import EdgeAlgorithm
from pypaimon.schema.data_types import ArrayType
from pypaimon.schema.data_types import GeographyType
from pypaimon.schema.data_types import GeometryType
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

    for invalid_type in ("GEOMETRY()", "GEOGRAPHY()",
                         "GEOGRAPHY(, spherical)"):
        with pytest.raises(ValueError, match="Invalid CRS"):
            DataTypeParser.parse_data_type(invalid_type)
    with pytest.raises(ValueError, match="Invalid edge interpolation algorithm"):
        DataTypeParser.parse_data_type("GEOGRAPHY(OGC:CRS84, rhumb)")
    with pytest.raises(ValueError, match="Invalid geometry type"):
        DataTypeParser.parse_data_type("GEOMETRY(EPSG:4326) trailing")


class _TestEdgeType(Enum):
    PLANAR = 'planar'
    SPHERICAL = 'spherical'
    VINCENTY = 'vincenty'
    THOMAS = 'thomas'
    ANDOYER = 'andoyer'
    KARNEY = 'karney'


class _TestGeoArrowWkbType(pyarrow.ExtensionType):

    def __init__(self, crs=None, edges=None):
        self._crs = crs
        self._edges = edges
        super().__init__(pyarrow.binary(), 'geoarrow.wkb')

    def __arrow_ext_serialize__(self):
        metadata = {}
        if self._crs is not None:
            metadata['crs'] = self._crs
        if self._edges is not None:
            metadata['edges'] = self._edges
        return json.dumps(metadata).encode('utf-8')

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        metadata = json.loads(serialized.decode('utf-8'))
        return cls(metadata.get('crs'), metadata.get('edges'))

    def with_crs(self, crs):
        return _TestGeoArrowWkbType(crs, self._edges)

    def with_edge_type(self, edge_type):
        return _TestGeoArrowWkbType(self._crs, edge_type.value)


def _install_test_geoarrow(monkeypatch):
    geoarrow_package = ModuleType('geoarrow')
    geoarrow_package.__path__ = []
    geoarrow_module = ModuleType('geoarrow.pyarrow')
    geoarrow_module.EdgeType = _TestEdgeType
    geoarrow_module.wkb = _TestGeoArrowWkbType
    geoarrow_package.pyarrow = geoarrow_module
    monkeypatch.setitem(sys.modules, 'geoarrow', geoarrow_package)
    monkeypatch.setitem(sys.modules, 'geoarrow.pyarrow', geoarrow_module)


def test_pyarrow_uses_geoarrow_wkb_extension(monkeypatch):
    _install_test_geoarrow(monkeypatch)
    fields = [
        DataField(0, "geom", GeometryType()),
        DataField(
            1,
            "geog",
            GeographyType(
                "EPSG:4326", EdgeAlgorithm.VINCENTY, nullable=False)),
    ]

    arrow_schema = PyarrowFieldParser.from_paimon_schema(fields)
    geometry_type = arrow_schema.field("geom").type
    geography_type = arrow_schema.field("geog").type
    assert geometry_type.extension_name == 'geoarrow.wkb'
    assert json.loads(geometry_type.__arrow_ext_serialize__()) == {
        'crs': 'OGC:CRS84'
    }
    assert json.loads(geography_type.__arrow_ext_serialize__()) == {
        'crs': 'EPSG:4326',
        'edges': 'vincenty',
    }
    assert b'paimon.type' not in (arrow_schema.field("geom").metadata or {})
    assert PyarrowFieldParser.to_paimon_schema(arrow_schema) == fields

    nested_fields = [
        DataField(0, "nested", RowType(True, [
            DataField(1, "geom", GeometryType(nullable=False)),
        ])),
        DataField(
            2,
            "geographies",
            ArrayType(
                True,
                GeographyType(algorithm=EdgeAlgorithm.KARNEY))),
    ]
    nested_schema = PyarrowFieldParser.from_paimon_schema(nested_fields)
    assert PyarrowFieldParser.to_paimon_schema(nested_schema) == nested_fields


def test_pyarrow_geospatial_fallback_without_geoarrow(monkeypatch):
    original_import = builtins.__import__

    def block_geoarrow(name, *args, **kwargs):
        if name.startswith('geoarrow'):
            raise ImportError("No module named '{}'".format(name))
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', block_geoarrow)
    fields = [
        DataField(0, "geom", GeometryType()),
        DataField(1, "geog", GeographyType()),
    ]
    arrow_schema = PyarrowFieldParser.from_paimon_schema(fields)
    assert arrow_schema.field("geom").type == pyarrow.large_binary()
    assert arrow_schema.field("geog").type == pyarrow.large_binary()
    assert b'paimon.type' not in (arrow_schema.field("geom").metadata or {})

    inferred = PyarrowFieldParser.to_paimon_schema(arrow_schema)
    assert inferred == [
        DataField(0, "geom", AtomicType('BLOB')),
        DataField(1, "geog", AtomicType('BLOB')),
    ]

    legacy_field = pyarrow.field(
        "value",
        pyarrow.binary(),
        metadata={b'paimon.type': b'GEOMETRY(OGC:CRS84)'})
    assert PyarrowFieldParser.to_paimon_schema(
        pyarrow.schema([legacy_field])) == [
            DataField(0, "value", AtomicType('BYTES')),
        ]


def test_real_geoarrow_type_round_trip():
    pytest.importorskip('geoarrow.pyarrow')
    fields = [
        DataField(0, "geom", GeometryType("EPSG:3857")),
        DataField(
            1,
            "geog",
            GeographyType("EPSG:4326", EdgeAlgorithm.THOMAS)),
    ]
    assert PyarrowFieldParser.to_paimon_schema(
        PyarrowFieldParser.from_paimon_schema(fields)) == fields


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
