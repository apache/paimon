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

from pypaimon.common.options import Options
from pypaimon.common.options.config import OssOptions
from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO
from pypaimon.read.reader.lance_utils import to_lance_specified


class LanceUtilsTest(unittest.TestCase):

    def test_oss_url_bucket_extraction_correctness(self):
        file_path = "oss://test-bucket/db-name.db/table-name/bucket-0/data.lance"

        properties = Options({
            OssOptions.OSS_ENDPOINT.key(): "oss-example-region.example.com",
            OssOptions.OSS_ACCESS_KEY_ID.key(): "test-key",
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): "test-secret",
        })

        file_io = PyArrowFileIO(file_path, properties)
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

        file_io = PyArrowFileIO(file_path, properties)
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


if __name__ == '__main__':
    unittest.main()
