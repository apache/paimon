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

from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options
from pypaimon.common.options.config import OssOptions
from pypaimon.read.reader.vortex_utils import to_vortex_specified


class VortexUtilsTest(unittest.TestCase):

    def _kwargs(self, endpoint, with_token=False):
        opts = {
            OssOptions.OSS_ENDPOINT.key(): endpoint,
            OssOptions.OSS_ACCESS_KEY_ID.key(): "test-key",
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): "test-secret",
        }
        if with_token:
            opts[OssOptions.OSS_SECURITY_TOKEN.key()] = "test-token"
        file_path = "oss://test-bucket/db.db/t/bucket-0/data.vortex"
        file_io = FileIO.get(file_path, Options(opts))
        path, kwargs = to_vortex_specified(file_io, file_path)
        return path, kwargs

    def test_oss_endpoint_scheme_is_stripped(self):
        # A user-configured endpoint that carries a scheme must not produce
        # the malformed "https://<bucket>.https://<host>".
        _, kwargs = self._kwargs("https://oss-example-region.example.com")
        self.assertEqual(
            kwargs['endpoint'],
            "https://test-bucket.oss-example-region.example.com")

    def test_oss_plain_endpoint_unchanged(self):
        path, kwargs = self._kwargs("oss-example-region.example.com", with_token=True)
        self.assertEqual(
            kwargs['endpoint'],
            "https://test-bucket.oss-example-region.example.com")
        self.assertEqual(kwargs.get('virtual_hosted_style_request'), 'true')
        self.assertEqual(kwargs.get('session_token'), "test-token")
        # vortex reads oss paths through the s3 scheme.
        self.assertTrue(path.startswith("s3://test-bucket/"))


if __name__ == '__main__':
    unittest.main()
