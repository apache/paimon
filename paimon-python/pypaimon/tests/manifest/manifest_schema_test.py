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
import tempfile
import unittest
from io import BytesIO
from unittest.mock import Mock

import fastavro

from pypaimon.common.options import Options
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema import data_file_meta
from pypaimon.manifest.schema.data_file_meta import DATA_FILE_META_SCHEMA
from pypaimon.manifest.schema.manifest_file_meta import MANIFEST_FILE_META_SCHEMA
from pypaimon.manifest.schema.simple_stats import (
    KEY_STATS_SCHEMA,
    VALUE_STATS_SCHEMA,
    PARTITION_STATS_SCHEMA,
)

LEGACY_MANIFEST_FILE_META_SCHEMA = {
    "type": "record",
    "name": "ManifestFileMeta",
    "fields": [
        {"name": "_VERSION", "type": "int"},
        {"name": "_FILE_NAME", "type": "string"},
        {"name": "_FILE_SIZE", "type": "long"},
        {"name": "_NUM_ADDED_FILES", "type": "long"},
        {"name": "_NUM_DELETED_FILES", "type": "long"},
        {"name": "_PARTITION_STATS", "type": PARTITION_STATS_SCHEMA},
        {"name": "_SCHEMA_ID", "type": "long"},
    ],
}


def _empty_partition_stats_bytes():
    return b"\x00\x00\x00\x00\x00"


class ManifestSchemaTest(unittest.TestCase):
    def test_file_source_field_type_and_default(self):
        schema = data_file_meta.DATA_FILE_META_SCHEMA
        fields = schema.get("fields", [])
        file_source_field = next((f for f in fields if f.get("name") == "_FILE_SOURCE"), None)

        self.assertIsNotNone(file_source_field, "_FILE_SOURCE field not found in DATA_FILE_META_SCHEMA")
        self.assertEqual(file_source_field.get("type"), ["null", "int"])
        self.assertIsNone(file_source_field.get("default"))

    def test_data_file_meta_schema_structure(self):
        """Test that DATA_FILE_META_SCHEMA has the correct structure."""
        self.assertIsInstance(DATA_FILE_META_SCHEMA, dict)
        self.assertEqual(DATA_FILE_META_SCHEMA["type"], "record")
        self.assertEqual(DATA_FILE_META_SCHEMA["name"], "DataFileMeta")
        self.assertIn("fields", DATA_FILE_META_SCHEMA)

        fields = DATA_FILE_META_SCHEMA["fields"]
        self.assertIsInstance(fields, list)

        # Create a mapping of field names to field definitions for easier testing
        field_map = {field["name"]: field for field in fields}

        # Check that all expected fields are present
        expected_fields = [
            "_FILE_NAME", "_FILE_SIZE", "_ROW_COUNT", "_MIN_KEY", "_MAX_KEY",
            "_KEY_STATS", "_VALUE_STATS", "_MIN_SEQUENCE_NUMBER",
            "_MAX_SEQUENCE_NUMBER", "_SCHEMA_ID", "_LEVEL", "_EXTRA_FILES",
            "_CREATION_TIME", "_DELETE_ROW_COUNT", "_EMBEDDED_FILE_INDEX",
            "_FILE_SOURCE", "_VALUE_STATS_COLS", "_EXTERNAL_PATH",
            "_FIRST_ROW_ID", "_WRITE_COLS"
        ]

        for field_name in expected_fields:
            self.assertIn(field_name, field_map, f"Field {field_name} is missing")

        # Check specific field types
        self.assertEqual(field_map["_FILE_NAME"]["type"], "string")
        self.assertEqual(field_map["_FILE_SIZE"]["type"], "long")
        self.assertEqual(field_map["_ROW_COUNT"]["type"], "long")
        self.assertEqual(field_map["_MIN_KEY"]["type"], "bytes")
        self.assertEqual(field_map["_MAX_KEY"]["type"], "bytes")
        self.assertEqual(field_map["_KEY_STATS"]["type"], KEY_STATS_SCHEMA)
        self.assertEqual(field_map["_VALUE_STATS"]["type"], VALUE_STATS_SCHEMA)
        self.assertEqual(field_map["_MIN_SEQUENCE_NUMBER"]["type"], "long")
        self.assertEqual(field_map["_MAX_SEQUENCE_NUMBER"]["type"], "long")
        self.assertEqual(field_map["_SCHEMA_ID"]["type"], "long")
        self.assertEqual(field_map["_LEVEL"]["type"], "int")
        self.assertEqual(field_map["_EXTRA_FILES"]["type"], {"type": "array", "items": "string"})
        self.assertEqual(field_map["_CREATION_TIME"]["type"],
                         ["null", {"type": "long", "logicalType": "timestamp-millis"}])
        self.assertEqual(field_map["_DELETE_ROW_COUNT"]["type"], ["null", "long"])
        self.assertEqual(field_map["_EMBEDDED_FILE_INDEX"]["type"], ["null", "bytes"])
        self.assertEqual(field_map["_FILE_SOURCE"]["type"], ["null", "int"])
        self.assertEqual(field_map["_VALUE_STATS_COLS"]["type"], ["null", {"type": "array", "items": "string"}])
        self.assertEqual(field_map["_EXTERNAL_PATH"]["type"], ["null", "string"])
        self.assertEqual(field_map["_FIRST_ROW_ID"]["type"], ["null", "long"])
        self.assertEqual(field_map["_WRITE_COLS"]["type"], ["null", {"type": "array", "items": "string"}])

    def test_manifest_file_meta_schema_structure(self):
        """Test that MANIFEST_FILE_META_SCHEMA has the correct structure."""
        self.assertIsInstance(MANIFEST_FILE_META_SCHEMA, dict)
        self.assertEqual(MANIFEST_FILE_META_SCHEMA["type"], "record")
        self.assertEqual(MANIFEST_FILE_META_SCHEMA["name"], "ManifestFileMeta")
        self.assertIn("fields", MANIFEST_FILE_META_SCHEMA)

        fields = MANIFEST_FILE_META_SCHEMA["fields"]
        self.assertIsInstance(fields, list)

        # Create a mapping of field names to field definitions for easier testing
        field_map = {field["name"]: field for field in fields}

        # Check that all expected fields are present
        expected_fields = [
            "_VERSION", "_FILE_NAME", "_FILE_SIZE", "_NUM_ADDED_FILES",
            "_NUM_DELETED_FILES", "_PARTITION_STATS", "_SCHEMA_ID",
            "_MIN_ROW_ID", "_MAX_ROW_ID",
        ]

        for field_name in expected_fields:
            self.assertIn(field_name, field_map, f"Field {field_name} is missing")

        # Check specific field types
        self.assertEqual(field_map["_VERSION"]["type"], "int")
        self.assertEqual(field_map["_FILE_NAME"]["type"], "string")
        self.assertEqual(field_map["_FILE_SIZE"]["type"], "long")
        self.assertEqual(field_map["_NUM_ADDED_FILES"]["type"], "long")
        self.assertEqual(field_map["_NUM_DELETED_FILES"]["type"], "long")
        self.assertEqual(field_map["_PARTITION_STATS"]["type"], PARTITION_STATS_SCHEMA)
        self.assertEqual(field_map["_SCHEMA_ID"]["type"], "long")
        self.assertEqual(field_map["_MIN_ROW_ID"]["type"], ["null", "long"])
        self.assertEqual(field_map["_MAX_ROW_ID"]["type"], ["null", "long"])
        self.assertIsNone(
            field_map["_MIN_ROW_ID"].get("default"),
            "_MIN_ROW_ID should have default None for backward compatibility",
        )
        self.assertIsNone(
            field_map["_MAX_ROW_ID"].get("default"),
            "_MAX_ROW_ID should have default None for backward compatibility",
        )

    def test_schema_references(self):
        """Test that schema references are correctly used."""
        data_file_fields = {field["name"]: field for field in DATA_FILE_META_SCHEMA["fields"]}
        manifest_file_fields = {field["name"]: field for field in MANIFEST_FILE_META_SCHEMA["fields"]}

        # Check that _KEY_STATS references KEY_STATS_SCHEMA
        key_stats_field = data_file_fields["_KEY_STATS"]
        self.assertEqual(key_stats_field["type"], KEY_STATS_SCHEMA)

        # Check that _VALUE_STATS references VALUE_STATS_SCHEMA
        value_stats_field = data_file_fields["_VALUE_STATS"]
        self.assertEqual(value_stats_field["type"], VALUE_STATS_SCHEMA)

        # Check that _PARTITION_STATS references PARTITION_STATS_SCHEMA
        partition_stats_field = manifest_file_fields["_PARTITION_STATS"]
        self.assertEqual(partition_stats_field["type"], PARTITION_STATS_SCHEMA)

    def test_schema_consistency(self):
        """Test that schema definitions are consistent."""
        # Verify that all stats schemas have the same structure
        self.assertEqual(KEY_STATS_SCHEMA["type"], "record")
        self.assertEqual(VALUE_STATS_SCHEMA["type"], "record")
        self.assertEqual(PARTITION_STATS_SCHEMA["type"], "record")

        # Verify that all stats schemas have different names
        names = [
            KEY_STATS_SCHEMA["name"],
            VALUE_STATS_SCHEMA["name"],
            PARTITION_STATS_SCHEMA["name"]
        ]
        self.assertEqual(len(names), len(set(names)), "Schema names should be unique")

    def test_read_legacy_manifest_list(self):
        temp_dir = tempfile.mkdtemp(prefix="manifest_schema_test_")
        table_path = f"file://{temp_dir}"
        file_io = LocalFileIO(table_path, Options({}))
        os.makedirs(os.path.join(temp_dir, "manifest"), exist_ok=True)
        manifest_list_name = "legacy-manifest-list-avro"

        legacy_record = {
            "_VERSION": 2,
            "_FILE_NAME": "data-12345.parquet",
            "_FILE_SIZE": 1000,
            "_NUM_ADDED_FILES": 1,
            "_NUM_DELETED_FILES": 0,
            "_PARTITION_STATS": {
                "_MIN_VALUES": _empty_partition_stats_bytes(),
                "_MAX_VALUES": _empty_partition_stats_bytes(),
                "_NULL_COUNTS": None,
            },
            "_SCHEMA_ID": 0,
        }
        with self.assertRaises(KeyError):
            _ = legacy_record["_MIN_ROW_ID"]
        self.assertIsNone(legacy_record.get("_MIN_ROW_ID"))
        self.assertIsNone(legacy_record.get("_MAX_ROW_ID"))

        buffer = BytesIO()
        fastavro.writer(buffer, LEGACY_MANIFEST_FILE_META_SCHEMA, [legacy_record])
        with file_io.new_output_stream(f"{table_path}/manifest/{manifest_list_name}") as out:
            out.write(buffer.getvalue())

        table = Mock()
        table.table_path = table_path
        table.file_io = file_io
        table.partition_keys_fields = []
        manager = ManifestListManager(table)
        metas = manager.read(manifest_list_name)

        self.assertEqual(len(metas), 1)
        meta = metas[0]
        self.assertEqual(meta.file_name, "data-12345.parquet")
        self.assertEqual(meta.file_size, 1000)
        self.assertEqual(meta.schema_id, 0)
        self.assertIsNone(meta.min_row_id)
        self.assertIsNone(meta.max_row_id)

