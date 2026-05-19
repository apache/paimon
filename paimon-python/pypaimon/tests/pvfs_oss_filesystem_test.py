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

"""Unit tests for OSS backend selection in PaimonVirtualFileSystem.

These run in CI with no external dependencies -- DLF, OSS and pyjindosdk are
all stubbed. They cover the ``fs.oss.impl`` dispatch and the ``_get_filesystem``
wiring that routes OSS reads/writes through the jindo or ossfs backend. The
end-to-end behavior of those backends is covered separately by the (DLF-gated)
``pvfs_oss_backend_alignment_test``.
"""

import unittest
from unittest import mock

from pypaimon.common.options import Options
from pypaimon.common.options.config import OssOptions
from pypaimon.filesystem import pvfs as pvfs_module
from pypaimon.filesystem.pvfs import (
    PaimonVirtualFileSystem,
    PVFSTableIdentifier,
    StorageType,
)


def _make_pvfs(extra_options=None):
    options = {OssOptions.OSS_ACCESS_KEY_ID.key(): "ak"}
    if extra_options:
        options.update(extra_options)
    return PaimonVirtualFileSystem(options)


class ExtractOssBucketTest(unittest.TestCase):

    def test_extracts_bucket_from_oss_path(self):
        self.assertEqual(
            PaimonVirtualFileSystem._extract_oss_bucket("oss://my-bucket/wh/db/tbl"),
            "my-bucket")

    def test_extracts_bucket_from_bucket_root(self):
        self.assertEqual(
            PaimonVirtualFileSystem._extract_oss_bucket("oss://my-bucket/"),
            "my-bucket")

    def test_rejects_non_oss_path(self):
        with self.assertRaises(Exception):
            PaimonVirtualFileSystem._extract_oss_bucket("s3://my-bucket/key")

    def test_rejects_path_without_bucket(self):
        with self.assertRaises(Exception):
            PaimonVirtualFileSystem._extract_oss_bucket("oss:///key")


class GetOssFilesystemDispatchTest(unittest.TestCase):
    """fs.oss.impl selects the backend; jindo falls back to ossfs when absent."""

    STORAGE_LOCATION = "oss://my-bucket/warehouse/db/tbl"

    def setUp(self):
        # Table-scoped OSS credentials handed to the backend builder.
        self.token_options = Options({
            OssOptions.OSS_ACCESS_KEY_ID.key(): "tk-ak",
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): "tk-sk",
            OssOptions.OSS_ENDPOINT.key(): "oss-cn-hangzhou.aliyuncs.com",
        })

    def _dispatch(self, oss_impl, jindo_available):
        extra = {OssOptions.OSS_IMPL.key(): oss_impl} if oss_impl is not None else None
        pvfs = _make_pvfs(extra)
        ossfs_sentinel = object()
        jindo_calls = []

        def fake_create_jindo(root_uri, options):
            jindo_calls.append((root_uri, options))
            return "JINDO_FS"

        with mock.patch.object(pvfs_module, "JINDO_AVAILABLE", jindo_available), \
             mock.patch.object(pvfs_module, "create_jindo_oss_filesystem", fake_create_jindo), \
             mock.patch.object(PaimonVirtualFileSystem, "_get_ossfs_filesystem",
                               staticmethod(lambda options: ossfs_sentinel)):
            fs = pvfs._get_oss_filesystem(self.token_options, self.STORAGE_LOCATION)
        return fs, ossfs_sentinel, jindo_calls

    def test_legacy_uses_ossfs(self):
        fs, ossfs_sentinel, jindo_calls = self._dispatch("legacy", jindo_available=True)
        self.assertIs(fs, ossfs_sentinel)
        self.assertEqual(jindo_calls, [])

    def test_jindo_uses_jindo_when_available(self):
        fs, _, jindo_calls = self._dispatch("jindo", jindo_available=True)
        self.assertEqual(fs, "JINDO_FS")
        self.assertEqual(len(jindo_calls), 1)
        root_uri, options = jindo_calls[0]
        self.assertEqual(root_uri, "oss://my-bucket/")
        self.assertIs(options, self.token_options)

    def test_default_impl_is_jindo(self):
        # No fs.oss.impl set -> OssOptions.OSS_IMPL default value ("jindo").
        fs, _, jindo_calls = self._dispatch(None, jindo_available=True)
        self.assertEqual(fs, "JINDO_FS")
        self.assertEqual(len(jindo_calls), 1)

    def test_jindo_falls_back_to_ossfs_when_unavailable(self):
        fs, ossfs_sentinel, jindo_calls = self._dispatch("jindo", jindo_available=False)
        self.assertIs(fs, ossfs_sentinel)
        self.assertEqual(jindo_calls, [])

    def test_invalid_impl_raises(self):
        with self.assertRaises(Exception) as ctx:
            self._dispatch("garbage", jindo_available=True)
        self.assertIn("Unsupported fs.oss.impl", str(ctx.exception))


class GetFilesystemOssWiringTest(unittest.TestCase):
    """_get_filesystem must feed the table's OSS storage path into the backend.

    Only the network calls are stubbed, so the real _get_filesystem runs --
    including its _table_cache_lock critical section. That makes this a
    regression guard against the self-deadlock: _get_filesystem holds that
    lock, so it must resolve the table path without calling a helper that
    re-acquires the same (non-reentrant) lock.
    """

    def test_oss_branch_forwards_table_storage_path(self):
        pvfs = _make_pvfs()
        identifier = PVFSTableIdentifier(
            catalog="cat", endpoint="http://rest", database="db", table="tbl")

        class FakeTokenResponse:
            token = {
                OssOptions.OSS_ACCESS_KEY_ID.key(): "tk-ak",
                OssOptions.OSS_ACCESS_KEY_SECRET.key(): "tk-sk",
                OssOptions.OSS_ENDPOINT.key(): "oss-cn-hangzhou.aliyuncs.com",
            }
            expires_at_millis = None

        class FakeTable:
            path = "oss://wired-bucket/warehouse/db/tbl"

        class FakeRestApi:
            def load_table_token(self, ident):
                return FakeTokenResponse()

            def get_table(self, ident):
                return FakeTable()

        captured = {}

        def fake_get_oss_filesystem(self, options, storage_location):
            captured["options"] = options
            captured["storage_location"] = storage_location
            return "FS"

        with mock.patch.object(PaimonVirtualFileSystem,
                               "_PaimonVirtualFileSystem__rest_api",
                               lambda self, ident: FakeRestApi()), \
             mock.patch.object(PaimonVirtualFileSystem, "_get_oss_filesystem",
                               fake_get_oss_filesystem):
            fs = pvfs._get_filesystem(identifier, StorageType.OSS)

        self.assertEqual(fs, "FS")
        # _get_filesystem must hand the backend the table's real OSS location,
        # from which _extract_oss_bucket then derives the jindo root uri.
        self.assertEqual(captured["storage_location"], "oss://wired-bucket/warehouse/db/tbl")
        self.assertEqual(
            PaimonVirtualFileSystem._extract_oss_bucket(captured["storage_location"]),
            "wired-bucket")
        self.assertEqual(captured["options"].get(OssOptions.OSS_ACCESS_KEY_ID), "tk-ak")


class StripStorageProtocolTest(unittest.TestCase):
    """OSS path form depends on the backend: jindo keeps oss://, ossfs strips it."""

    def _strip(self, oss_impl, jindo_available, path):
        pvfs = _make_pvfs({OssOptions.OSS_IMPL.key(): oss_impl})
        with mock.patch.object(pvfs_module, "JINDO_AVAILABLE", jindo_available):
            return pvfs._strip_storage_protocol(StorageType.OSS, path)

    def test_jindo_backend_keeps_oss_scheme(self):
        self.assertEqual(
            self._strip("jindo", True, "oss://b/db/tbl/f.bin"),
            "oss://b/db/tbl/f.bin")

    def test_legacy_backend_strips_oss_scheme(self):
        self.assertEqual(
            self._strip("legacy", True, "oss://b/db/tbl/f.bin"),
            "b/db/tbl/f.bin")

    def test_jindo_unavailable_falls_back_to_stripping(self):
        # fs.oss.impl=jindo but pyjindosdk missing -> ossfs backend -> strip.
        self.assertEqual(
            self._strip("jindo", False, "oss://b/db/tbl/f.bin"),
            "b/db/tbl/f.bin")


if __name__ == "__main__":
    unittest.main()
