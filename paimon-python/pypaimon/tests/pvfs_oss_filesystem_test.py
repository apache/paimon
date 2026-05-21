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
    PaimonRealStorage,
    PaimonVirtualFileSystem,
    PVFSTableIdentifier,
    StorageType,
)


def _make_pvfs(extra_options=None):
    options = {OssOptions.OSS_ACCESS_KEY_ID.key(): "ak"}
    if extra_options:
        options.update(extra_options)
    # skip_instance_cache so each test gets a fresh PVFS (fsspec's _Cached
    # metaclass would otherwise share _fs_cache state across tests).
    return PaimonVirtualFileSystem(options, skip_instance_cache=True)


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

    def _dispatch(self, oss_impl, jindo_ossfs_available):
        extra = {OssOptions.OSS_IMPL.key(): oss_impl} if oss_impl is not None else None
        pvfs = _make_pvfs(extra)
        ossfs_sentinel = object()
        jindo_calls = []

        def fake_create_jindo(root_uri, options):
            jindo_calls.append((root_uri, options))
            return "JINDO_FS"

        # Patch both surfaces. JINDO_AVAILABLE is held True so the test
        # behaves the same whether or not pyjindosdk is installed in the
        # CI image; backend selection is then driven solely by
        # JINDO_OSSFS_AVAILABLE -- the surface the PVFS jindo backend
        # actually needs.
        with mock.patch.object(pvfs_module, "JINDO_AVAILABLE", True), \
             mock.patch.object(pvfs_module, "JINDO_OSSFS_AVAILABLE", jindo_ossfs_available), \
             mock.patch.object(pvfs_module, "create_jindo_oss_filesystem", fake_create_jindo), \
             mock.patch.object(PaimonVirtualFileSystem, "_get_ossfs_filesystem",
                               staticmethod(lambda options: ossfs_sentinel)):
            fs = pvfs._get_oss_filesystem(self.token_options, self.STORAGE_LOCATION)
        return fs, ossfs_sentinel, jindo_calls

    def test_legacy_uses_ossfs(self):
        fs, ossfs_sentinel, jindo_calls = self._dispatch("legacy", jindo_ossfs_available=True)
        self.assertIs(fs, ossfs_sentinel)
        self.assertEqual(jindo_calls, [])

    def test_jindo_uses_jindo_when_available(self):
        fs, _, jindo_calls = self._dispatch("jindo", jindo_ossfs_available=True)
        self.assertEqual(fs, "JINDO_FS")
        self.assertEqual(len(jindo_calls), 1)
        root_uri, options = jindo_calls[0]
        self.assertEqual(root_uri, "oss://my-bucket/")
        self.assertIs(options, self.token_options)

    def test_default_impl_is_jindo(self):
        # No fs.oss.impl set -> OssOptions.OSS_IMPL default value ("jindo").
        fs, _, jindo_calls = self._dispatch(None, jindo_ossfs_available=True)
        self.assertEqual(fs, "JINDO_FS")
        self.assertEqual(len(jindo_calls), 1)

    def test_jindo_falls_back_to_ossfs_when_pyjindo_ossfs_missing(self):
        # fs.oss.impl=jindo but pyjindo.ossfs not importable (e.g. an older
        # pyjindosdk build that ships only fs/util). PyArrow jindo path stays
        # available; PVFS jindo backend falls back to ossfs.
        fs, ossfs_sentinel, jindo_calls = self._dispatch("jindo", jindo_ossfs_available=False)
        self.assertIs(fs, ossfs_sentinel)
        self.assertEqual(jindo_calls, [])

    def test_invalid_impl_raises(self):
        with self.assertRaises(Exception) as ctx:
            self._dispatch("garbage", jindo_ossfs_available=True)
        self.assertIn("Unsupported fs.oss.impl", str(ctx.exception))


class GetFilesystemOssWiringTest(unittest.TestCase):
    """_get_filesystem must forward the caller-supplied storage_location into
    the OSS backend factory. Callers always have the table path in hand (from
    _get_table_store) by the time they reach _get_filesystem, so threading it
    through avoids a redundant REST round-trip inside the write critical
    section."""

    def test_oss_branch_forwards_caller_storage_location(self):
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

        class FakeRestApi:
            def load_table_token(self, ident):
                return FakeTokenResponse()

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
            fs = pvfs._get_filesystem(
                identifier, StorageType.OSS, "oss://wired-bucket/warehouse/db/tbl")

        self.assertEqual(fs, "FS")
        self.assertEqual(captured["storage_location"], "oss://wired-bucket/warehouse/db/tbl")
        self.assertEqual(
            PaimonVirtualFileSystem._extract_oss_bucket(captured["storage_location"]),
            "wired-bucket")
        self.assertEqual(captured["options"].get(OssOptions.OSS_ACCESS_KEY_ID), "tk-ak")


class StripStorageProtocolTest(unittest.TestCase):
    """OSS path form depends on the backend: jindo keeps oss://, ossfs strips it."""

    def _strip(self, oss_impl, jindo_ossfs_available, path):
        pvfs = _make_pvfs({OssOptions.OSS_IMPL.key(): oss_impl})
        with mock.patch.object(pvfs_module, "JINDO_AVAILABLE", True), \
             mock.patch.object(pvfs_module, "JINDO_OSSFS_AVAILABLE", jindo_ossfs_available):
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
        # fs.oss.impl=jindo but pyjindo.ossfs missing -> ossfs backend -> strip.
        self.assertEqual(
            self._strip("jindo", False, "oss://b/db/tbl/f.bin"),
            "b/db/tbl/f.bin")


class CloseStaleFilesystemOnRefreshTest(unittest.TestCase):
    """When _get_filesystem rebuilds a cached fs (token near expiry), the old
    filesystem must be released. For the jindo backend this is the only way
    the underlying native JindoSDK connection gets reclaimed. The close must
    also happen outside _table_cache_lock -- JindoSDK close() can block on
    native teardown, and holding the write lock through it would stall every
    other OSS rebuild."""

    STORAGE_LOCATION = "oss://b/wh/db/tbl"

    @staticmethod
    def _stub_oss_rebuild():
        identifier = PVFSTableIdentifier(
            catalog="cat", endpoint="http://rest", database="db", table="tbl")

        class FakeTokenResponse:
            token = {
                OssOptions.OSS_ACCESS_KEY_ID.key(): "tk-ak",
                OssOptions.OSS_ACCESS_KEY_SECRET.key(): "tk-sk",
                OssOptions.OSS_ENDPOINT.key(): "oss-cn-hangzhou.aliyuncs.com",
            }
            expires_at_millis = None

        class FakeRestApi:
            def load_table_token(self, ident):
                return FakeTokenResponse()

        return identifier, mock.patch.object(
            PaimonVirtualFileSystem,
            "_PaimonVirtualFileSystem__rest_api",
            lambda self, ident: FakeRestApi(),
        ), mock.patch.object(
            PaimonVirtualFileSystem,
            "_get_oss_filesystem",
            lambda self, opts, loc: "NEW_FS",
        )

    def test_old_filesystem_close_called_when_token_expires(self):
        pvfs = _make_pvfs()
        identifier, patch_rest, patch_build = self._stub_oss_rebuild()

        close_log = []

        class StaleFs:
            def close(self_inner):
                close_log.append("closed")

        # expires_at_millis=0 -> need_refresh() is True -> _get_filesystem rebuilds.
        pvfs._fs_cache[identifier] = PaimonRealStorage(
            token={}, expires_at_millis=0, file_system=StaleFs())

        with patch_rest, patch_build:
            new_fs = pvfs._get_filesystem(identifier, StorageType.OSS, self.STORAGE_LOCATION)

        self.assertEqual(new_fs, "NEW_FS")
        self.assertEqual(close_log, ["closed"],
                         "stale filesystem must be closed on token refresh")

    def test_first_build_does_not_invoke_close(self):
        pvfs = _make_pvfs()
        identifier, patch_rest, patch_build = self._stub_oss_rebuild()

        with patch_rest, patch_build:
            new_fs = pvfs._get_filesystem(identifier, StorageType.OSS, self.STORAGE_LOCATION)

        # No prior cache entry -> nothing to close, must not raise.
        self.assertEqual(new_fs, "NEW_FS")

    def test_close_runs_after_write_lock_released(self):
        # Regression guard: close() of the stale fs runs after the write
        # lock is released. If close were still inside the critical section,
        # attempting to acquire the same non-reentrant write lock from
        # within close would fail (blocking=False -> False).
        pvfs = _make_pvfs()
        identifier, patch_rest, patch_build = self._stub_oss_rebuild()
        lock_was_free_during_close = []

        class StaleFs:
            def close(self_inner):
                probe = pvfs._table_cache_lock.gen_wlock()
                got = probe.acquire(blocking=False)
                lock_was_free_during_close.append(got)
                if got:
                    probe.release()

        pvfs._fs_cache[identifier] = PaimonRealStorage(
            token={}, expires_at_millis=0, file_system=StaleFs())

        with patch_rest, patch_build:
            pvfs._get_filesystem(identifier, StorageType.OSS, self.STORAGE_LOCATION)

        self.assertEqual(lock_was_free_during_close, [True],
                         "stale fs close must run after _table_cache_lock is released")

    def test_close_without_close_method_is_no_op(self):
        # _close_filesystem_quietly must tolerate filesystems that don't
        # implement close() (e.g. pyjindo 6.10.2's JindoOssFileSystem).
        PaimonVirtualFileSystem._close_filesystem_quietly(object())
        PaimonVirtualFileSystem._close_filesystem_quietly(None)


if __name__ == "__main__":
    unittest.main()
