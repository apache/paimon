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
from pypaimon.daft.daft_io_config import (
    _convert_paimon_catalog_options_to_file_io_config,
    serialize_io_config,
)
from pypaimon.daft.daft_datasource import _PaimonPKSplitTask
from pypaimon.table.row.blob import BlobDescriptor


def _descriptor_column(specs):
    """large_binary column of serialized BlobDescriptors (None for gaps)."""
    out = [None if s is None else BlobDescriptor(*s).serialize() for s in specs]
    return pa.array(out, type=pa.large_binary())


class BlobColumnToFileArrayTest(unittest.TestCase):
    # Pure config/arrow tests run on any installed Daft; only File-cast needs file ranges.

    def test_blob_column_to_file_array(self):
        # io_config is null without creds; embedded only for valid (non-null) rows with creds.
        col = _descriptor_column([("oss://b/k1", 0, 5), None, ("oss://b/k2", 8, 9)])
        bare = blob_column_to_file_array(col)
        self.assertEqual(bare.field("url").to_pylist(), ["oss://b/k1", None, "oss://b/k2"])
        self.assertEqual(bare.field(file_range_position_field()).to_pylist(), [0, None, 8])
        self.assertEqual(bare.field(file_range_size_field()).to_pylist(), [5, None, 9])
        self.assertEqual(bare.field("io_config").null_count, 3)

        blob = serialize_io_config(IOConfig(s3=S3Config(key_id="AK", access_key="SK")))
        self.assertEqual(blob_column_to_file_array(col, blob).field("io_config").to_pylist(),
                         [blob, None, blob])

    def test_serialize_io_config_roundtrips(self):
        s3 = IOConfig(s3=S3Config(key_id="AK", access_key="SK", session_token="TOK"))
        self.assertEqual(IOConfig._from_serialized(serialize_io_config(s3)).s3.session_token, "TOK")
        # OSS uses Daft's OpenDAL backend, which serializes differently from S3Config.
        oss = {"access_key_id": "AK", "endpoint": "https://oss-test.example.com", "bucket": "b"}
        cfg = IOConfig(opendal_backends={"oss": oss})
        self.assertEqual(IOConfig._from_serialized(serialize_io_config(cfg)).opendal_backends["oss"], oss)

    def test_file_io_config_routes_oss_through_s3(self):
        # OSS -> Daft S3 client (oss:// aliased to s3, virtual-hosted); File.open() over
        # OpenDAL/OSS fails to issue the request on some Daft builds.
        cfg = _convert_paimon_catalog_options_to_file_io_config({
            "warehouse": "oss://b", "fs.oss.endpoint": "oss-test.example.com",
            "fs.oss.region": "test-region", "fs.oss.accessKeyId": "AK",
            "fs.oss.accessKeySecret": "SK", "fs.oss.securityToken": "TOK",
        })
        self.assertEqual(cfg.s3.key_id, "AK")
        self.assertEqual(cfg.s3.endpoint_url, "https://oss-test.example.com")
        self.assertTrue(cfg.s3.force_virtual_addressing)
        self.assertEqual(dict(cfg.protocol_aliases)["oss"], "s3")
        # No credentials: None when required; oss->s3 alias otherwise (env/instance creds).
        self.assertIsNone(_convert_paimon_catalog_options_to_file_io_config({"warehouse": "oss://b"}))
        env = _convert_paimon_catalog_options_to_file_io_config({"warehouse": "oss://b"}, require_credentials=False)
        self.assertEqual(dict(env.protocol_aliases)["oss"], "s3")

    def test_explicit_io_config_used_when_no_derivable_credentials(self):
        # The explicit io_config passed to read_paimon must reach blob File columns when no
        # complete credentials are derivable -- an endpoint or a half key pair is not enough.
        explicit = serialize_io_config(IOConfig(s3=S3Config(key_id="USERKEY", access_key="SK")))

        def blob_key(catalog_options):
            task = _PaimonPKSplitTask(catalog_options, None, None, {}, None, None,
                                      blob_column_names={"x"}, explicit_io_config_bytes=explicit)
            return IOConfig._from_serialized(task._blob_io_config_bytes(None)).s3.key_id

        self.assertEqual(blob_key({}), "USERKEY")
        for opts in (
            {"warehouse": "oss://b", "fs.oss.endpoint": "oss-test.example.com"},
            {"warehouse": "oss://b", "fs.oss.accessKeyId": "PARTIAL"},  # no secret
            {"warehouse": "s3://b", "fs.s3.endpoint": "https://s3.example.com"},
            {"warehouse": "s3a://b", "fs.s3.accessKeyId": "PARTIAL"},  # no secret
        ):
            self.assertEqual(blob_key(opts), "USERKEY")

    @pytest.mark.skipif(not has_file_range_reads(), reason="daft >= 0.7.11 required for File range metadata")
    def test_cast_to_file_reconstructs_io_config(self):
        # The crux: embedded bytes must survive the cast to DataType.file() so a native
        # Daft File carries the credentials.
        blob = serialize_io_config(IOConfig(s3=S3Config(key_id="AK", region_name="test-region")))
        arr = blob_column_to_file_array(_descriptor_column([("s3://b/k", 0, 4)]), blob)
        df = daft.from_arrow(pa.table({"f": arr}))
        df = df.with_column("f", df["f"].cast(DataType.file()))
        restored = IOConfig._from_serialized(df.to_arrow().column("f")[0].as_py()["io_config"])
        self.assertEqual(restored.s3.key_id, "AK")
        self.assertEqual(restored.s3.region_name, "test-region")


if __name__ == "__main__":
    unittest.main()
