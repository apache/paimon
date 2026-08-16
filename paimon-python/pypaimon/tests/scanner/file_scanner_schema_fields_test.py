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

import unittest
from unittest.mock import MagicMock

from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.schema.data_types import AtomicType, DataField


class FileScannerSchemaFieldsShortCircuitTest(unittest.TestCase):
    """Test _schema_fields short-circuits current schema id (REST catalog 403 fix)."""

    def _make_scanner(self, current_schema_id, current_fields,
                      historical_fields_map=None):
        """Build a FileScanner bypassing __init__ to isolate _schema_fields."""
        scanner = FileScanner.__new__(FileScanner)
        scanner.table = MagicMock()
        scanner.table.table_schema.id = current_schema_id
        scanner.table.table_schema.fields = current_fields
        scanner.table.schema_manager = MagicMock()

        def get_schema(schema_id):
            if historical_fields_map is None or schema_id not in historical_fields_map:
                raise AssertionError(
                    f"schema_manager.get_schema({schema_id}) was called "
                    "but no historical schema was registered for it"
                )
            historical = MagicMock()
            historical.fields = historical_fields_map[schema_id]
            return historical

        scanner.table.schema_manager.get_schema.side_effect = get_schema
        return scanner

    def test_short_circuits_current_schema_id(self):
        """Current schema id returns in-memory fields without filesystem access."""
        current_fields = [DataField(0, "a", AtomicType("INT"))]
        scanner = self._make_scanner(
            current_schema_id=5, current_fields=current_fields
        )

        result = scanner._schema_fields(5)

        self.assertIs(result, current_fields)
        scanner.table.schema_manager.get_schema.assert_not_called()

    def test_delegates_for_historical_schema(self):
        """Historical schema id delegates to schema_manager.get_schema()."""
        current_fields = [DataField(0, "a", AtomicType("INT"))]
        historical_fields = [
            DataField(0, "a", AtomicType("INT")),
            DataField(1, "b", AtomicType("STRING")),
        ]
        scanner = self._make_scanner(
            current_schema_id=5,
            current_fields=current_fields,
            historical_fields_map={3: historical_fields},
        )

        result = scanner._schema_fields(3)

        self.assertEqual(result, historical_fields)
        scanner.table.schema_manager.get_schema.assert_called_once_with(3)

    def test_short_circuit_works_for_zero_schema_id(self):
        """Schema id == 0 still short-circuits (guards against truthiness bugs)."""
        current_fields = [DataField(0, "x", AtomicType("INT"))]
        scanner = self._make_scanner(
            current_schema_id=0, current_fields=current_fields
        )

        result = scanner._schema_fields(0)

        self.assertIs(result, current_fields)
        scanner.table.schema_manager.get_schema.assert_not_called()


if __name__ == "__main__":
    unittest.main()
