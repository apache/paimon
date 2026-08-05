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

import sys
import types
import unittest
from unittest.mock import Mock, patch

# Skip pypaimon/__init__.py (pulls catalog -> polars) when collecting this module.
if "pypaimon" not in sys.modules:
    _pypaimon = types.ModuleType("pypaimon")
    _pypaimon.__path__ = [
        __file__.rsplit("/tests/", 1)[0],
    ]
    sys.modules["pypaimon"] = _pypaimon

from pypaimon.common.options.core_options import ChangelogProducer, CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        MapType)
from pypaimon.write.file_store_write import FileStoreWrite


class TestFileStoreWritePrimaryKeyBlobRouting(unittest.TestCase):
    """Writer routing for primary-key managed BLOB tables."""

    def _write(self, fields, is_primary_key_table):
        table = Mock()
        table.is_primary_key_table = is_primary_key_table
        table.table_schema.fields = fields
        table.options = CoreOptions(Options({}))
        table.bucket_mode = Mock(return_value=0)
        table.trimmed_primary_keys = ["id"]
        table.primary_keys = ["id"]

        write = FileStoreWrite.__new__(FileStoreWrite)
        write.table = table
        write.write_cols = None
        write.blob_consumer = None
        write.changelog_producer = ChangelogProducer.NONE
        write.options = CoreOptions.copy(table.options)
        return write

    @patch("pypaimon.write.file_store_write.KeyValueDataWriter")
    def test_primary_key_scalar_blob_uses_key_value_writer(self, kv_writer):
        fields = [
            DataField(0, "id", AtomicType("INT", False)),
            DataField(1, "name", AtomicType("STRING")),
            DataField(2, "payload", AtomicType("BLOB")),
        ]
        write = self._write(fields, True)
        write._seq_number_stats = Mock(return_value={0: 1})

        write._create_data_writer((), 0, write.options)
        kwargs = kv_writer.call_args.kwargs
        self.assertEqual(kwargs["managed_blob_fields"], {"payload"})

    @patch("pypaimon.write.file_store_write.KeyValueDataWriter")
    def test_primary_key_inline_blob_skips_managed_fields(self, kv_writer):
        fields = [
            DataField(0, "id", AtomicType("INT", False)),
            DataField(1, "payload", AtomicType("BLOB")),
        ]
        write = self._write(fields, True)
        write.options = CoreOptions.copy(CoreOptions(Options({
            "blob-descriptor-field": "payload",
        })))
        write.table.options = write.options
        write._seq_number_stats = Mock(return_value={0: 1})

        write._create_data_writer((), 0, write.options)
        kwargs = kv_writer.call_args.kwargs
        self.assertEqual(kwargs["managed_blob_fields"], set())

    @patch("pypaimon.write.file_store_write.DedicatedFormatWriter")
    def test_append_blob_table_uses_dedicated_format_writer(self, blob_writer):
        fields = [
            DataField(0, "payload", AtomicType("BLOB")),
            DataField(1, "tag", AtomicType("STRING")),
        ]
        write = self._write(fields, False)

        write._create_data_writer((), 0, write.options)
        blob_writer.assert_called_once()

    @patch("pypaimon.write.file_store_write.KeyValueDataWriter")
    def test_primary_key_array_blob_managed_fields(self, kv_writer):
        fields = [
            DataField(0, "id", AtomicType("INT", False)),
            DataField(1, "name", AtomicType("STRING")),
            DataField(2, "pictures", ArrayType(True, AtomicType("BLOB"))),
        ]
        write = self._write(fields, True)
        write._seq_number_stats = Mock(return_value={0: 1})
        write._create_data_writer((), 0, write.options)
        kwargs = kv_writer.call_args.kwargs
        self.assertEqual(kwargs["managed_blob_fields"], {"pictures"})

    @patch("pypaimon.write.file_store_write.KeyValueDataWriter")
    def test_primary_key_map_blob_managed_fields(self, kv_writer):
        fields = [
            DataField(0, "id", AtomicType("INT", False)),
            DataField(1, "name", AtomicType("STRING")),
            DataField(
                2,
                "payload",
                MapType(True, AtomicType("STRING", False), AtomicType("BLOB")),
            ),
        ]
        write = self._write(fields, True)
        write._seq_number_stats = Mock(return_value={0: 1})
        write._create_data_writer((), 0, write.options)
        kwargs = kv_writer.call_args.kwargs
        self.assertEqual(kwargs["managed_blob_fields"], {"payload"})


if __name__ == "__main__":
    unittest.main()
