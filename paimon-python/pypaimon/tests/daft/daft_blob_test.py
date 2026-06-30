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
# limitations under the License.
################################################################################
import unittest

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from daft.datatype import DataType
from daft.io import IOConfig, S3Config

from pypaimon.daft.daft_blob import blob_column_to_file_array
from pypaimon.daft.daft_compat import (
    file_range_position_field,
    file_range_size_field,
    has_file_range_reads,
)
from pypaimon.daft.daft_io_config import serialize_io_config
from pypaimon.table.row.blob import BlobDescriptor


def _descriptor_column(specs):
    """Build a large_binary column of serialized BlobDescriptors (None for gaps)."""
    out = []
    for spec in specs:
        if spec is None:
            out.append(None)
        else:
            uri, off, length = spec
            out.append(BlobDescriptor(uri, off, length).serialize())
    return pa.array(out, type=pa.large_binary())


@pytest.mark.skipif(not has_file_range_reads(), reason="daft >= 0.7.11 required for File range metadata")
class BlobColumnToFileArrayTest(unittest.TestCase):

    def _io_field(self, arr):
        return arr.field("io_config")

    def test_io_config_null_by_default(self):
        col = _descriptor_column([("oss://b/k", 0, 10), None])
        arr = blob_column_to_file_array(col)
        io = self._io_field(arr)
        self.assertEqual(io.null_count, 2)
        self.assertEqual(arr.field("url").to_pylist(), ["oss://b/k", None])
        self.assertEqual(arr.field(file_range_position_field()).to_pylist(), [0, None])
        self.assertEqual(arr.field(file_range_size_field()).to_pylist(), [10, None])

    def test_io_config_embedded_for_valid_rows_only(self):
        cfg = IOConfig(s3=S3Config(key_id="AK", access_key="SK", region_name="cn-hangzhou"))
        blob = serialize_io_config(cfg)
        col = _descriptor_column([("oss://b/k1", 0, 5), None, ("oss://b/k2", 8, 9)])
        arr = blob_column_to_file_array(col, blob)
        self.assertEqual(self._io_field(arr).to_pylist(), [blob, None, blob])

    def test_serialize_io_config_roundtrips(self):
        cfg = IOConfig(s3=S3Config(key_id="AK", access_key="SK", session_token="TOK"))
        restored = IOConfig._from_serialized(serialize_io_config(cfg))
        self.assertEqual(restored.s3.key_id, "AK")
        self.assertEqual(restored.s3.session_token, "TOK")

    def test_cast_to_file_reconstructs_io_config(self):
        # The crux of the fix: embedded bytes must survive cast to DataType.file() so a native
        # Daft File carries the credentials. Round-trip through daft and read the io_config back.
        cfg = IOConfig(s3=S3Config(key_id="AK", access_key="SK", region_name="cn-hangzhou"))
        blob = serialize_io_config(cfg)
        arr = blob_column_to_file_array(_descriptor_column([("s3://b/k", 0, 4)]), blob)
        df = daft.from_arrow(pa.table({"f": arr}))
        df = df.with_column("f", df["f"].cast(DataType.file()))
        back = df.to_arrow().column("f")[0].as_py()
        restored = IOConfig._from_serialized(back["io_config"])
        self.assertEqual(restored.s3.key_id, "AK")
        self.assertEqual(restored.s3.region_name, "cn-hangzhou")


if __name__ == "__main__":
    unittest.main()
