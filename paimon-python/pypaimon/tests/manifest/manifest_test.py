import unittest

import pypaimon.manifest.schema.data_file_meta as data_file_meta


class ManifestTest(unittest.TestCase):
    def test_file_source_field_type_and_default(self):
        schema = data_file_meta.DATA_FILE_META_SCHEMA
        fields = schema.get("fields", [])
        file_source_field = next((f for f in fields if f.get("name") == "_FILE_SOURCE"), None)

        self.assertIsNotNone(file_source_field, "_FILE_SOURCE field not found in DATA_FILE_META_SCHEMA")
        self.assertEqual(file_source_field.get("type"), ["null", "int"])
        self.assertIsNone(file_source_field.get("default"))
