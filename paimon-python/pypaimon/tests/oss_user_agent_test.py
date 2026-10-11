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

import os
import unittest
from unittest import mock

from pypaimon import build_info
from pypaimon.common.options import Options
from pypaimon.common.options.config import OssOptions
from pypaimon.filesystem import oss_user_agent
from pypaimon.filesystem import pyarrow_file_io
from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

IDENTITY = "pypaimon/2.2.dev0"


class OssUserAgentIdentityTest(unittest.TestCase):

    def setUp(self):
        oss_user_agent.identity.cache_clear()
        self.addCleanup(oss_user_agent.identity.cache_clear)

    def test_identity_uses_build_version(self):
        with mock.patch.object(build_info, "version", return_value="2.2.dev"):
            self.assertEqual("pypaimon/2.2.dev", oss_user_agent.identity())

    def test_identity_without_build_version(self):
        with mock.patch.object(build_info, "version", return_value=None):
            self.assertEqual("pypaimon", oss_user_agent.identity())

    def test_build_version_from_full_version(self):
        for full_version, expected in (("python-2.2.dev-abc123", "2.2.dev"),
                                       ("python-2.2.0-UNKNOWN", "2.2.0"),
                                       ("UNKNOWN", None)):
            with mock.patch.object(build_info, "_FULL_VERSION", full_version):
                self.assertEqual(expected, build_info.version())


class OssUserAgentTest(unittest.TestCase):

    def setUp(self):
        patcher = mock.patch.object(oss_user_agent, "identity", return_value=IDENTITY)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_features_default_to_identity(self):
        self.assertEqual(IDENTITY, oss_user_agent.features(Options({})))

    def test_user_features_follow_identity(self):
        options = Options({oss_user_agent.USER_AGENT_FEATURES: " Flink  Spark "})
        self.assertEqual(IDENTITY + " Flink Spark", oss_user_agent.features(options))

    def test_identity_is_not_duplicated(self):
        for user_features in ("pypaimon Flink", "Flink pypaimon/1.0"):
            options = Options({oss_user_agent.USER_AGENT_FEATURES: user_features})
            self.assertEqual(user_features, oss_user_agent.features(options))

    def test_access_tracking_is_appended_to_user_extended(self):
        options = Options({
            oss_user_agent.USER_AGENT_EXTENDED: "user/ext",
            oss_user_agent.DLF_ACCESS_TRACKING_EXTENDED_INFO: "acs/xxx k/v",
        })
        self.assertEqual("user/ext acs/xxx k/v", oss_user_agent.extended(options))

    def test_extended_from_either_side_alone(self):
        self.assertEqual("acs/xxx", oss_user_agent.extended(
            Options({oss_user_agent.DLF_ACCESS_TRACKING_EXTENDED_INFO: "acs/xxx"})))
        self.assertEqual("user/ext", oss_user_agent.extended(
            Options({oss_user_agent.USER_AGENT_EXTENDED: "user/ext"})))

    def test_common_keys_are_used_without_oss_keys(self):
        options = Options({
            oss_user_agent.COMMON_USER_AGENT_MODULE: "MyApp/1.0",
            oss_user_agent.COMMON_USER_AGENT_FEATURES: "Flink",
            oss_user_agent.COMMON_USER_AGENT_EXTENDED: "vvr",
            oss_user_agent.DLF_ACCESS_TRACKING_EXTENDED_INFO: "uid/123",
        })
        self.assertEqual("MyApp/1.0", oss_user_agent.module(options))
        self.assertEqual(IDENTITY + " Flink", oss_user_agent.features(options))
        self.assertEqual("vvr uid/123", oss_user_agent.extended(options))

    def test_oss_keys_override_common_keys_per_part(self):
        options = Options({
            oss_user_agent.COMMON_USER_AGENT_MODULE: "MyApp/1.0",
            oss_user_agent.COMMON_USER_AGENT_FEATURES: "Flink",
            oss_user_agent.COMMON_USER_AGENT_EXTENDED: "vvr",
            oss_user_agent.USER_AGENT_FEATURES: "Spark",
            oss_user_agent.USER_AGENT_EXTENDED: " ",
        })
        self.assertEqual("MyApp/1.0", oss_user_agent.module(options))
        self.assertEqual(IDENTITY + " Spark", oss_user_agent.features(options))
        self.assertEqual("vvr", oss_user_agent.extended(options))
        options = Options({oss_user_agent.COMMON_USER_AGENT_EXTENDED: "vvr",
                           oss_user_agent.USER_AGENT_EXTENDED: "oss/ext"})
        self.assertEqual("oss/ext", oss_user_agent.extended(options))
        self.assertIsNone(oss_user_agent.module(Options({})))

    def test_blank_extended_is_ignored(self):
        options = Options({
            oss_user_agent.USER_AGENT_EXTENDED: " ",
            oss_user_agent.DLF_ACCESS_TRACKING_EXTENDED_INFO: "",
        })
        self.assertIsNone(oss_user_agent.extended(options))
        self.assertIsNone(oss_user_agent.extended(Options({})))


class LegacyOssUserAgentTest(unittest.TestCase):

    def _new_legacy_file_io(self):
        options = Options({
            OssOptions.OSS_ACCESS_KEY_ID.key(): "ak",
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): "sk",
            OssOptions.OSS_ENDPOINT.key(): "oss-cn-test.example.com",
            OssOptions.OSS_REGION.key(): "cn-test",
            OssOptions.OSS_IMPL.key(): "legacy",
        })
        with mock.patch.object(pyarrow_file_io.pafs, "S3FileSystem") as s3_filesystem, \
                mock.patch.object(oss_user_agent, "identity", return_value=IDENTITY):
            PyArrowFileIO("oss://test-bucket/", options)
        s3_filesystem.assert_called_once()

    def test_sets_aws_app_id_when_absent(self):
        with mock.patch.dict(os.environ):
            os.environ.pop("AWS_SDK_UA_APP_ID", None)
            self._new_legacy_file_io()
            self.assertEqual(IDENTITY, os.environ["AWS_SDK_UA_APP_ID"])

    def test_keeps_user_aws_app_id(self):
        with mock.patch.dict(os.environ, {"AWS_SDK_UA_APP_ID": "my-app"}):
            self._new_legacy_file_io()
            self.assertEqual("my-app", os.environ["AWS_SDK_UA_APP_ID"])


if __name__ == "__main__":
    unittest.main()
