"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory
from pypaimon.schema.data_types import (
    AtomicType, DataField, DataTypeParser, PyarrowFieldParser, Keyword
)


class DataTypesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_blob_keyword_enum(self):
        """Test that BLOB is properly defined in the Keyword enum"""
        self.assertEqual(Keyword.BLOB.value, "BLOB")
        self.assertIn(Keyword.BLOB, list(Keyword))

    def test_blob_atomic_type_creation(self):
        """Test creating BLOB AtomicType"""
        blob_type = AtomicType("BLOB", nullable=True)
        self.assertEqual(blob_type.type, "BLOB")
        self.assertTrue(blob_type.nullable)
        self.assertEqual(str(blob_type), "BLOB")

        blob_type_not_null = AtomicType("BLOB", nullable=False)
        self.assertEqual(str(blob_type_not_null), "BLOB NOT NULL")

    def test_blob_data_type_parser(self):
        """Test parsing BLOB type from string"""
        blob_type = DataTypeParser.parse_atomic_type_sql_string("BLOB")
        self.assertIsInstance(blob_type, AtomicType)
        self.assertEqual(blob_type.type, "BLOB")
        self.assertTrue(blob_type.nullable)

        blob_type_not_null = DataTypeParser.parse_atomic_type_sql_string("BLOB NOT NULL")
        self.assertIsInstance(blob_type_not_null, AtomicType)
        self.assertEqual(blob_type_not_null.type, "BLOB")
        self.assertFalse(blob_type_not_null.nullable)

    def test_blob_to_pyarrow_conversion(self):
        """Test converting BLOB type to PyArrow large_binary"""
        blob_type = AtomicType("BLOB")
        pa_type = PyarrowFieldParser.from_paimon_type(blob_type)
        self.assertTrue(pa.types.is_large_binary(pa_type))

    def test_pyarrow_large_binary_to_blob_conversion(self):
        """Test converting PyArrow large_binary to BLOB type"""
        pa_type = pa.large_binary()
        paimon_type = PyarrowFieldParser.to_paimon_type(pa_type, nullable=True)
        self.assertIsInstance(paimon_type, AtomicType)
        self.assertEqual(paimon_type.type, "BLOB")
        self.assertTrue(paimon_type.nullable)

    def test_blob_field_conversion(self):
        """Test converting BLOB DataField to PyArrow field and back"""
        # Create a BLOB DataField
        blob_field = DataField(
            id=1,
            name="binary_data",
            type=AtomicType("BLOB"),
            description="Binary large object field"
        )

        # Convert to PyArrow field
        pa_field = PyarrowFieldParser.from_paimon_field(blob_field)
        self.assertEqual(pa_field.name, "binary_data")
        self.assertTrue(pa.types.is_large_binary(pa_field.type))
        self.assertTrue(pa_field.nullable)
        self.assertEqual(
            pa_field.metadata[b'description'].decode('utf-8'),
            "Binary large object field"
        )

        # Convert back to Paimon field
        paimon_field = PyarrowFieldParser.to_paimon_field(1, pa_field)
        self.assertEqual(paimon_field.id, 1)
        self.assertEqual(paimon_field.name, "binary_data")
        self.assertIsInstance(paimon_field.type, AtomicType)
        self.assertEqual(paimon_field.type.type, "BLOB")
        self.assertTrue(paimon_field.type.nullable)
        self.assertEqual(paimon_field.description, "Binary large object field")

    def test_blob_schema_conversion(self):
        """Test schema conversion with BLOB fields"""
        # Create PyArrow schema with large_binary
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('blob_data', pa.large_binary()),
            ('regular_binary', pa.binary())
        ])

        # Convert to Paimon schema
        paimon_fields = PyarrowFieldParser.to_paimon_schema(pa_schema)
        self.assertEqual(len(paimon_fields), 4)

        # Check BLOB field
        blob_field = paimon_fields[2]
        self.assertEqual(blob_field.name, "blob_data")
        self.assertIsInstance(blob_field.type, AtomicType)
        self.assertEqual(blob_field.type.type, "BLOB")

        # Check regular binary field
        binary_field = paimon_fields[3]
        self.assertEqual(binary_field.name, "regular_binary")
        self.assertIsInstance(binary_field.type, AtomicType)
        self.assertEqual(binary_field.type.type, "BYTES")

        # Convert back to PyArrow schema
        pa_schema_converted = PyarrowFieldParser.from_paimon_schema(paimon_fields)
        self.assertEqual(len(pa_schema_converted), 4)
        self.assertTrue(pa.types.is_large_binary(pa_schema_converted.field('blob_data').type))
        self.assertTrue(pa.types.is_binary(pa_schema_converted.field('regular_binary').type))

    def test_blob_avro_conversion(self):
        """Test BLOB type conversion to Avro"""
        pa_type = pa.large_binary()
        avro_type = PyarrowFieldParser.to_avro_type(pa_type, "blob_field")
        self.assertEqual(avro_type, "bytes")

    def test_blob_json_serialization(self):
        """Test BLOB type JSON serialization and deserialization"""
        blob_field = DataField(
            id=1,
            name="blob_field",
            type=AtomicType("BLOB", nullable=True)
        )

        # Serialize to dict
        field_dict = blob_field.to_dict()
        self.assertEqual(field_dict['name'], 'blob_field')
        self.assertEqual(field_dict['type'], 'BLOB')

        # Deserialize from dict
        restored_field = DataField.from_dict(field_dict)
        self.assertEqual(restored_field.name, 'blob_field')
        self.assertIsInstance(restored_field.type, AtomicType)
        self.assertEqual(restored_field.type.type, 'BLOB')
        self.assertTrue(restored_field.type.nullable)


if __name__ == '__main__':
    unittest.main()
