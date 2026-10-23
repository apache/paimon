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

import struct
import unittest
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options
from pypaimon.common.options.config import OssOptions
from pypaimon.execution.python.read.reader.lance_utils import (to_lance_specified)


class LanceUtilsTest(unittest.TestCase):

    def test_oss_url_bucket_extraction_correctness(self):
        file_path = "oss://test-bucket/db-name.db/table-name/bucket-0/data.lance"

        properties = Options({
            OssOptions.OSS_ENDPOINT.key(): "oss-example-region.example.com",
            OssOptions.OSS_ACCESS_KEY_ID.key(): "test-key",
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): "test-secret",
        })

        file_io = FileIO.get(file_path, properties)
        file_path_for_lance, storage_options = to_lance_specified(file_io, file_path)

        self.assertEqual(
            storage_options['endpoint'],
            "https://test-bucket.oss-example-region.example.com"
        )

        self.assertTrue(file_path_for_lance.startswith("oss://test-bucket/"))

        self.assertEqual(storage_options.get('virtual_hosted_style_request'), 'true')

        self.assertTrue('fs.oss.endpoint' in storage_options)
        self.assertTrue('fs.oss.accessKeyId' in storage_options)
        self.assertTrue('fs.oss.accessKeySecret' in storage_options)

    def test_oss_url_with_security_token(self):
        file_path = "oss://my-bucket/path/to/file.lance"

        properties = Options({
            OssOptions.OSS_ENDPOINT.key(): "oss-example-region.example.com",
            OssOptions.OSS_ACCESS_KEY_ID.key(): "test-access-key",
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): "test-secret-key",
            OssOptions.OSS_SECURITY_TOKEN.key(): "test-token",
        })

        file_io = FileIO.get(file_path, properties)
        file_path_for_lance, storage_options = to_lance_specified(file_io, file_path)

        self.assertEqual(file_path_for_lance, "oss://my-bucket/path/to/file.lance")

        self.assertEqual(
            storage_options['endpoint'],
            "https://my-bucket.oss-example-region.example.com"
        )

        self.assertEqual(storage_options.get('virtual_hosted_style_request'), 'true')

        self.assertEqual(storage_options.get('access_key_id'), "test-access-key")
        self.assertEqual(storage_options.get('secret_access_key'), "test-secret-key")
        self.assertEqual(storage_options.get('session_token'), "test-token")
        self.assertEqual(storage_options.get('oss_session_token'), "test-token")

        self.assertTrue('fs.oss.securityToken' in storage_options)


@pytest.mark.parametrize("backend", ["local", "pyarrow", "hdfs"])
@pytest.mark.parametrize("version, expected_header", [(None, (0, 3)), ("2.1", (2, 1))])
def test_lance_file_writer_preserves_compatible_default_and_explicit_version(
        tmp_path, backend, version, expected_header):
    pytest.importorskip("lance", minversion="0.39.0")
    from lance.file import LanceFileReader
    from pypaimon.filesystem.hdfs_native_file_io import HdfsNativeFileIO
    from pypaimon.filesystem.local_file_io import LocalFileIO
    from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

    path = tmp_path / (backend + ".lance")
    file_io = LocalFileIO()
    # The remote FileIO constructors require remote filesystem configuration.
    # Reuse local storage while exercising each real Lance writer and reader.
    write = {"local": LocalFileIO.write_lance, "pyarrow": PyArrowFileIO.write_lance,
             "hdfs": HdfsNativeFileIO.write_lance}[backend]
    data = pa.table({"id": pa.array([1, 2, 3], type=pa.int32()), "text": ["first", None, "last"]})
    kwargs = {} if version is None else {"version": version}

    write(file_io, str(path), data, **kwargs)

    footer = path.read_bytes()[-8:]
    assert footer[-4:] == b"LANC"
    assert struct.unpack("<HH", footer[:4]) == expected_header
    assert LanceFileReader(str(path)).read_all().to_table().equals(data)


def test_lance_file_writer_options_keeps_old_dependency_without_version_argument():
    lance = pytest.importorskip("lance")
    from pypaimon.execution.python.read.reader.lance_utils import lance_file_writer_options

    options = {"storage_options": {"region": "test-region"}}
    with patch.object(lance, "__version__", "0.10.18"):
        result = lance_file_writer_options(options)

    assert result == options
    assert "version" not in result
    assert options == {"storage_options": {"region": "test-region"}}


if __name__ == '__main__':
    unittest.main()
