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

import gc
import os
import pickle
import signal
import tempfile
import threading
import time
import unittest
import warnings
import weakref
from datetime import timedelta
from unittest.mock import patch, MagicMock

from cachetools import TTLCache

from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
from pypaimon.catalog.rest.rest_token import RESTToken

from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions, OssOptions
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.table.row.blob import BlobDescriptor

_FILE_IO_GET = 'pypaimon.catalog.rest.rest_token_file_io.FileIO.get'


class _NonOwningStream:
    """Like a pyjindo stream: does not keep the FileIO that opened it alive."""

    def __init__(self, file_io):
        self._file_io = weakref.ref(file_io)

    def read(self):
        if self._file_io() is None:
            raise RuntimeError('use-after-free: the FileIO of this stream was released')
        return b'x'


class _FakeFileIO:

    def __init__(self, path, options):
        self.path = path
        self.options = options

    def new_input_stream(self, path):
        return _NonOwningStream(self)

    def exists(self, path):
        return True


class RESTTokenFileIOTest(unittest.TestCase):
    """Test cases for RESTTokenFileIO."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp(prefix="rest_token_file_io_test_")
        self.warehouse_path = f"file://{self.temp_dir}"
        self.identifier = Identifier.from_string("default.test_table")
        self.catalog_options = Options({})
        RESTTokenFileIO._FILE_IO_CACHE.clear()

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        RESTTokenFileIO._FILE_IO_CACHE.clear()
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @staticmethod
    def _valid_token(name):
        return RESTToken({OssOptions.OSS_SECURITY_TOKEN.key(): name},
                         int(time.time() * 1000) + 7_200_000)

    def _file_io_with_token(self, path, options, token):
        file_io = RESTTokenFileIO(self.identifier, path, options)
        file_io.token = token
        return file_io

    def test_token_refresh_keeps_file_io_of_open_streams(self):
        table_path = "oss://bucket/db.db/table"
        file_io = RESTTokenFileIO(self.identifier, table_path, self.catalog_options)
        self.addCleanup(RESTTokenFileIO._TOKEN_CACHE.pop, 'refresh-test', None)
        now = [1700000000]
        issued = []

        def refresh():
            issued.append(RESTToken(
                {OssOptions.OSS_SECURITY_TOKEN.key(): "sts-{}".format(len(issued))},
                (now[0] + 2 * 3600) * 1000))
            file_io.token = issued[-1]

        with patch('pypaimon.catalog.rest.rest_token_file_io.time.time',
                   side_effect=lambda: now[0]), \
                patch.object(file_io, '_build_cache_key', return_value='refresh-test'), \
                patch.object(file_io, 'refresh_token', side_effect=refresh), \
                patch(_FILE_IO_GET, side_effect=_FakeFileIO):
            stream = file_io.new_input_stream(table_path + "/bucket-0/data.blob")
            # Less than an hour left: the next call refreshes the token.
            now[0] += 3600 + 1
            self.assertTrue(file_io.exists(table_path + "/snapshot/LATEST"))
            gc.collect()

            self.assertEqual(2, len(issued))
            self.assertEqual(b'x', stream.read())

    def test_file_io_cache_separates_paths_and_options_with_same_token(self):
        token = self._valid_token("shared")
        legacy = Options({OssOptions.OSS_IMPL.key(): "legacy"})
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'), \
                patch(_FILE_IO_GET, side_effect=_FakeFileIO):
            bucket_a = self._file_io_with_token(
                "oss://bucket-a/db.db/t", Options({}), token).file_io()
            bucket_b = self._file_io_with_token(
                "oss://bucket-b/db.db/t", Options({}), token).file_io()
            bucket_a_legacy = self._file_io_with_token(
                "oss://bucket-a/db.db/t", legacy, token).file_io()

        self.assertEqual("oss://bucket-a/db.db/t", bucket_a.path)
        self.assertEqual("oss://bucket-b/db.db/t", bucket_b.path)
        self.assertIsNot(bucket_a, bucket_a_legacy)
        self.assertEqual("legacy", bucket_a_legacy.options.get(OssOptions.OSS_IMPL))

    def test_same_token_on_other_bucket_keeps_its_own_oss_bucket(self):
        token = RESTToken({
            OssOptions.OSS_ENDPOINT.key(): "oss-cn-hangzhou.aliyuncs.com",
            OssOptions.OSS_ACCESS_KEY_ID.key(): "ak",
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): "sk",
        }, int(time.time() * 1000) + 7_200_000)
        options = Options({OssOptions.OSS_IMPL.key(): "legacy"})
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            first = self._file_io_with_token("oss://bucket-a/db.db/t", options, token)
            second = self._file_io_with_token("oss://bucket-b/db.db/t", options, token)
            self.assertEqual("bucket-a", first.file_io()._oss_bucket)
            self.assertEqual("bucket-b", second.file_io()._oss_bucket)

    def test_instances_of_one_table_share_file_io(self):
        token = self._valid_token("shared")
        table_path = "oss://bucket/db.db/t"
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'), \
                patch(_FILE_IO_GET, side_effect=_FakeFileIO) as get_io:
            first = self._file_io_with_token(table_path, Options({"k": "v"}), token)
            second = self._file_io_with_token(table_path, Options({"k": "v"}), token)
            restored = pickle.loads(pickle.dumps(first))

            self.assertIs(first.file_io(), second.file_io())
            self.assertIs(first.file_io(), restored.file_io())
            self.assertEqual(1, get_io.call_count)

    def test_file_io_cache_expires_idle_entries_only(self):
        clock = [0]
        hour = 3600
        cache = TTLCache(maxsize=RESTTokenFileIO._FILE_IO_CACHE_MAXSIZE,
                         ttl=RESTTokenFileIO._FILE_IO_CACHE_TTL, timer=lambda: clock[0])
        with patch.object(RESTTokenFileIO, '_FILE_IO_CACHE', cache), \
                patch.object(RESTTokenFileIO, 'try_to_refresh_token'), \
                patch(_FILE_IO_GET, side_effect=_FakeFileIO):
            file_io = self._file_io_with_token(
                "oss://bucket/db.db/t", self.catalog_options, self._valid_token("first"))
            first = file_io.file_io()
            for _ in range(3):
                clock[0] += 9 * hour
                self.assertIs(first, file_io.file_io())

            # The previous token's FileIO stays cached until it has been idle for the TTL.
            file_io.token = self._valid_token("second")
            second = file_io.file_io()
            self.assertIsNot(first, second)
            first_ref = weakref.ref(first)
            del first
            clock[0] += 9 * hour
            file_io.file_io()
            gc.collect()
            self.assertIsNotNone(first_ref())
            clock[0] += 2 * hour
            self.assertIs(second, file_io.file_io())
            gc.collect()
            self.assertIsNone(first_ref())

    def test_concurrent_lookups_create_one_file_io(self):
        token = self._valid_token("shared")
        threads_count = 8
        barrier = threading.Barrier(threads_count)
        results = []

        def slow_file_io(path, options):
            time.sleep(0.05)
            return _FakeFileIO(path, options)

        def lookup(file_io):
            barrier.wait()
            results.append(file_io.file_io())

        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'), \
                patch(_FILE_IO_GET, side_effect=slow_file_io) as get_io:
            instances = [self._file_io_with_token("oss://bucket/db.db/t", Options({}), token)
                         for _ in range(threads_count)]
            threads = [threading.Thread(target=lookup, args=(i,)) for i in instances]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        self.assertEqual(threads_count, len(results))
        self.assertTrue(all(r is results[0] for r in results))
        self.assertEqual(1, get_io.call_count)

    @staticmethod
    def _run_in_forked_child(action):
        with warnings.catch_warnings():
            # Python 3.12+ warns about forking with other threads alive, which these tests do on purpose.
            warnings.simplefilter("ignore", DeprecationWarning)
            pid = os.fork()
        if pid == 0:
            code = 1
            try:
                signal.alarm(10)
                action()
                code = 0
            finally:
                os._exit(code)
        return os.waitpid(pid, 0)[1]

    def _assert_child_succeeded(self, status):
        self.assertFalse(os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGALRM,
                         "the forked child blocked on a lock held by a parent thread")
        self.assertTrue(os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, status)

    @unittest.skipUnless(hasattr(os, "register_at_fork"), "needs os.register_at_fork")
    def test_fork_while_another_thread_creates_file_io(self):
        token = self._valid_token("shared")
        creating = threading.Event()

        def slow_file_io(path, options):
            if path.endswith("/slow"):
                creating.set()
                time.sleep(0.3)
            return _FakeFileIO(path, options)

        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'), \
                patch(_FILE_IO_GET, side_effect=slow_file_io) as get_io:
            other = self._file_io_with_token("oss://bucket/db.db/other", Options({}), token)
            slow = self._file_io_with_token("oss://bucket/db.db/slow", Options({}), token)

            def in_child():
                other.file_io()
                # The fork waited for the creation, so the child finds that FileIO cached.
                slow.file_io()
                assert get_io.call_count == 2, get_io.call_count

            creator = threading.Thread(target=slow.file_io)
            creator.start()
            self.assertTrue(creating.wait(5))
            status = self._run_in_forked_child(in_child)
            creator.join()
        self._assert_child_succeeded(status)

    @unittest.skipUnless(hasattr(os, "register_at_fork"), "needs os.register_at_fork")
    def test_fork_while_another_thread_refreshes_token(self):
        table_path = "oss://bucket/db.db/t"
        refreshing, release = threading.Event(), threading.Event()
        holders = []

        def refresh(file_io):
            if threading.current_thread() in holders:
                refreshing.set()
                release.wait(10)
            file_io.token = self._valid_token("refreshed")

        self.addCleanup(RESTTokenFileIO._TOKEN_CACHE.pop, 'fork-test', None)
        with patch.object(RESTTokenFileIO, '_build_cache_key', return_value='fork-test'), \
                patch.object(RESTTokenFileIO, 'refresh_token', autospec=True, side_effect=refresh), \
                patch(_FILE_IO_GET, side_effect=_FakeFileIO):
            file_io = RESTTokenFileIO(self.identifier, table_path, Options({}))
            holders.append(threading.Thread(target=file_io.file_io))
            holders[0].start()
            self.assertTrue(refreshing.wait(5))
            # The child uses the object whose locks are held, and another object of that table.
            status = self._run_in_forked_child(lambda: (
                file_io.file_io(),
                RESTTokenFileIO(self.identifier, table_path, Options({})).file_io()))
            release.set()
            holders[0].join()
        self._assert_child_succeeded(status)

    def test_existing_valid_token_does_not_refresh_or_read_shared_cache(self):
        file_io = RESTTokenFileIO(self.identifier, self.warehouse_path)
        now = 1700000000
        valid = RESTToken({'key': 'value'}, (now + 7200) * 1000)
        expiring = RESTToken({'key': 'value'}, (now + 1800) * 1000)
        with patch('pypaimon.catalog.rest.rest_token_file_io.time.time', return_value=now), \
                patch.object(file_io, 'try_to_refresh_token') as refresh, \
                patch.object(file_io, '_get_cached_token') as shared_cache:
            self.assertIsNone(file_io._existing_valid_token())
            file_io.token = valid
            self.assertIs(file_io._existing_valid_token(), valid)
            file_io.token = expiring
            self.assertIsNone(file_io._existing_valid_token())
            refresh.assert_not_called()
            shared_cache.assert_not_called()

    def test_blob_presigned_url_bound_table_root(self):
        root = "oss://bucket/table-a"
        file_io = RESTTokenFileIO(self.identifier, root, self.catalog_options)
        descriptor = BlobDescriptor(root + "/data/video.blob", 10, 20)
        validity = timedelta(minutes=30)
        delegate = MagicMock()
        delegate.create_blob_presigned_url.return_value = "https://signed-url"
        with patch.object(
                file_io, '_file_io_with_token',
                return_value=(delegate, None)):
            self.assertEqual(
                file_io.create_blob_presigned_url(root, descriptor, validity),
                "https://signed-url")
            delegate.create_blob_presigned_url.assert_called_once_with(
                root, descriptor, validity)

    def test_blob_presigned_url_rejects_other_table_before_resolving_io(self):
        file_io = RESTTokenFileIO(
            self.identifier, "oss://bucket/table-a", self.catalog_options)
        other_root = "oss://bucket/table-b"
        descriptor = BlobDescriptor(other_root + "/data/video.blob", 10, 20)
        with patch.object(file_io, '_file_io_with_token') as resolve:
            with self.assertRaisesRegex(ValueError, "bound table root"):
                file_io.create_blob_presigned_url(
                    other_root, descriptor, timedelta(minutes=30))
            resolve.assert_not_called()

    def test_presigned_url_refreshes_credentials_for_requested_lifetime(self):
        self._check_presigned_url_lifetime(3, 4, True)

    def test_presigned_url_reuses_credentials_for_short_validity(self):
        self._check_presigned_url_lifetime(0.5, 4, False)

    def test_presigned_url_accepts_when_one_second_exceeds_validity(self):
        self._check_presigned_url_lifetime(
            1.5, 3, False, current_token_extra_seconds=1)

    def test_presigned_url_rejects_insufficient_refreshed_lifetime(self):
        self._check_presigned_url_lifetime(3, 2, True, rejected=True)

    def _check_presigned_url_lifetime(
            self, requested_hours, refreshed_hours, should_refresh, rejected=False,
            current_token_extra_seconds=None):
        root = "oss://bucket/table"
        file_io = RESTTokenFileIO(self.identifier, root, self.catalog_options)
        now = 1700000000
        token_properties = {OssOptions.OSS_SECURITY_TOKEN.key(): "test-token"}
        if current_token_extra_seconds is None:
            old_expiry = now + 2 * 3600
        else:
            old_expiry = (
                now + requested_hours * 3600 + current_token_extra_seconds)
        old_token = RESTToken(token_properties, int(old_expiry * 1000))
        new_token = RESTToken(token_properties, (now + refreshed_hours * 3600) * 1000)
        file_io.token = old_token
        descriptor = BlobDescriptor(root + "/video.blob", 0, 10)
        validity = timedelta(hours=requested_hours)

        def refresh():
            file_io.token = new_token

        with patch('pypaimon.catalog.rest.rest_token_file_io.time.time', return_value=now), \
                patch.object(file_io, '_build_cache_key', return_value='presigning-test'), \
                patch.object(file_io, '_get_cached_token', return_value=old_token), \
                patch.object(file_io, '_set_cached_token') as cache_token, \
                patch.object(file_io, 'refresh_token', side_effect=refresh) as refresh_token, \
                patch('pypaimon.catalog.rest.rest_token_file_io.FileIO.get') as get_io:
            presign = get_io.return_value.create_blob_presigned_url
            presign.side_effect = ["https://first-url", "https://refreshed-url"]
            if rejected:
                with self.assertRaisesRegex(ValueError, "credential lifetime after refresh"):
                    file_io.create_blob_presigned_url(root, descriptor, validity)
                get_io.assert_not_called()
            else:
                self.assertEqual("https://first-url", file_io.create_blob_presigned_url(
                    root, descriptor, validity))
                self.assertEqual(1, presign.call_count)
                presign.assert_called_with(root, descriptor, validity)
            self.assertEqual(int(should_refresh), refresh_token.call_count)
            if should_refresh:
                cache_token.assert_called_once_with(file_io._build_cache_key(), new_token)
            else:
                cache_token.assert_not_called()

    def test_presigned_url_refreshes_and_resigns_after_materialization(self):
        root = "oss://bucket/table"
        file_io = RESTTokenFileIO(self.identifier, root, self.catalog_options)
        now = [1700000000]
        token_properties = {OssOptions.OSS_SECURITY_TOKEN.key(): "test-token"}
        old_token = RESTToken(token_properties, (now[0] + 2 * 3600) * 1000)
        new_token = RESTToken(token_properties, (now[0] + 4 * 3600) * 1000)
        file_io.token = old_token
        descriptor = BlobDescriptor(root + "/video.blob", 0, 10)
        validity = timedelta(minutes=30)

        def refresh():
            file_io.token = new_token

        def presign(*_):
            if now[0] == 1700000000:
                now[0] += 2 * 3600
                return "https://first-url"
            return "https://refreshed-url"

        with patch('pypaimon.catalog.rest.rest_token_file_io.time.time',
                   side_effect=lambda: now[0]), \
                patch.object(file_io, '_build_cache_key', return_value='presigning-test'), \
                patch.object(file_io, '_get_cached_token', return_value=old_token), \
                patch.object(file_io, '_set_cached_token'), \
                patch.object(file_io, 'refresh_token', side_effect=refresh) as refresh_token, \
                patch('pypaimon.catalog.rest.rest_token_file_io.FileIO.get') as get_io:
            get_io.return_value.create_blob_presigned_url.side_effect = presign
            self.assertEqual(
                "https://refreshed-url",
                file_io.create_blob_presigned_url(root, descriptor, validity))
            self.assertEqual(
                2, get_io.return_value.create_blob_presigned_url.call_count)
            refresh_token.assert_called_once_with()

    def test_new_output_stream_path_conversion_and_parent_creation(self):
        """Test new_output_stream correctly handles URI paths and creates parent directories."""
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )

            # Test with file:// URI path - should convert and create parent directory
            test_file_path = f"file://{self.temp_dir}/subdir/test.txt"
            test_content = b"test content"
            expected_path = f"{self.temp_dir}/subdir/test.txt"

            with file_io.new_output_stream(test_file_path) as stream:
                stream.write(test_content)

            self.assertTrue(os.path.exists(expected_path),
                            f"File should be created at {expected_path}")
            with open(expected_path, 'rb') as f:
                self.assertEqual(f.read(), test_content)

            # Test nested path - should create multiple parent directories
            nested_path = f"file://{self.temp_dir}/level1/level2/level3/nested.txt"
            parent_dir = f"{self.temp_dir}/level1/level2/level3"
            self.assertFalse(os.path.exists(parent_dir))

            with file_io.new_output_stream(nested_path) as stream:
                stream.write(b"nested content")

            self.assertTrue(os.path.exists(parent_dir),
                            f"Parent directory should be created at {parent_dir}")
            self.assertTrue(os.path.exists(f"{parent_dir}/nested.txt"))

            # Test relative path
            original_cwd = os.getcwd()
            try:
                os.chdir(self.temp_dir)
                relative_path = "relative_test.txt"
                with file_io.new_output_stream(relative_path) as stream:
                    stream.write(b"relative content")
                expected_relative = os.path.join(self.temp_dir, relative_path)
                self.assertTrue(os.path.exists(expected_relative))
            finally:
                os.chdir(original_cwd)

    def test_try_to_write_atomic_directory_check(self):
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )

            target_dir = os.path.join(self.temp_dir, "target_dir")
            os.makedirs(target_dir)

            result = file_io.try_to_write_atomic(f"file://{target_dir}", "test content")
            self.assertFalse(result, "try_to_write_atomic should return False when target is a directory")

            self.assertTrue(os.path.isdir(target_dir))
            self.assertEqual(len(os.listdir(target_dir)), 0, "No file should be created inside the directory")

            normal_file = os.path.join(self.temp_dir, "normal_file.txt")
            result = file_io.try_to_write_atomic(f"file://{normal_file}", "test content")
            self.assertTrue(result, "try_to_write_atomic should succeed for a normal file path")
            self.assertTrue(os.path.exists(normal_file))
            with open(normal_file, "r") as f:
                self.assertEqual(f.read(), "test content")

    def test_new_output_stream_behavior_matches_parent(self):
        """Test that RESTTokenFileIO.new_output_stream behaves like FileIO.new_output_stream."""
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            rest_file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )
            regular_file_io = LocalFileIO(self.warehouse_path, self.catalog_options)

            test_file_path = f"file://{self.temp_dir}/comparison/test.txt"
            test_content = b"comparison content"

            with rest_file_io.new_output_stream(test_file_path) as stream:
                stream.write(test_content)

            expected_path = f"{self.temp_dir}/comparison/test.txt"
            self.assertTrue(os.path.exists(expected_path))

            with regular_file_io.new_input_stream(test_file_path) as stream:
                read_content = stream.read()
                self.assertEqual(read_content, test_content)

    def test_pickle_serialization(self):
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            original_file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )
            original_file_io.token = RESTToken({}, 1)
            original_backend = original_file_io.file_io()

            pickled = pickle.dumps(original_file_io)

            deserialized_file_io = pickle.loads(pickled)

            self.assertEqual(deserialized_file_io.identifier, original_file_io.identifier)
            self.assertEqual(deserialized_file_io.path, original_file_io.path)
            self.assertEqual(deserialized_file_io.properties.data, original_file_io.properties.data)

            self.assertIsNone(deserialized_file_io.api_instance)
            self.assertIs(deserialized_file_io.file_io(), original_backend)

            test_file_path = f"file://{self.temp_dir}/pickle_test.txt"
            test_content = b"pickle test content"

            with deserialized_file_io.new_output_stream(test_file_path) as stream:
                stream.write(test_content)

            expected_path = f"{self.temp_dir}/pickle_test.txt"
            self.assertTrue(os.path.exists(expected_path))
            with open(expected_path, 'rb') as f:
                self.assertEqual(f.read(), test_content)

    def test_dlf_oss_endpoint_overrides_token_endpoint(self):
        """Test that DLF OSS endpoint overrides the standard OSS endpoint in token."""
        dlf_oss_endpoint = "https://dlf-custom-endpoint.oss-cn-hangzhou.aliyuncs.com"
        catalog_options = {
            CatalogOptions.DLF_OSS_ENDPOINT.key(): dlf_oss_endpoint
        }

        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                Options(catalog_options)
            )

            # Create a token with a standard OSS endpoint
            token = {
                OssOptions.OSS_ENDPOINT.key(): "https://standard-endpoint.oss-cn-beijing.aliyuncs.com",
                "fs.oss.accessKeyId": "test-access-key",
                "fs.oss.accessKeySecret": "test-secret-key"
            }

            # Merge token with catalog options
            merged_token = file_io._merge_token_with_catalog_options(token)

            # Verify DLF OSS endpoint overrides the standard endpoint
            self.assertEqual(
                merged_token[OssOptions.OSS_ENDPOINT.key()],
                dlf_oss_endpoint,
                "DLF OSS endpoint should override the standard OSS endpoint in token"
            )
            # Verify other token properties are preserved
            self.assertEqual(
                merged_token["fs.oss.accessKeyId"],
                "test-access-key",
                "Other token properties should be preserved"
            )
            self.assertEqual(
                merged_token["fs.oss.accessKeySecret"],
                "test-secret-key",
                "Other token properties should be preserved"
            )

    def test_catalog_options_not_modified(self):
        from pypaimon.api.rest_util import RESTUtil

        original_catalog_options = Options({
            CatalogOptions.URI.key(): "http://test-uri",
            "custom.key": "custom.value"
        })

        catalog_options_copy = Options(original_catalog_options.to_map())

        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                original_catalog_options
            )

            token_dict = {
                OssOptions.OSS_ACCESS_KEY_ID.key(): "token-access-key",
                OssOptions.OSS_ACCESS_KEY_SECRET.key(): "token-secret-key",
                OssOptions.OSS_ENDPOINT.key(): "token-endpoint"
            }

            merged_token = file_io._merge_token_with_catalog_options(token_dict)

            self.assertEqual(
                original_catalog_options.to_map(),
                catalog_options_copy.to_map(),
                "Original catalog_options should not be modified"
            )

            merged_properties = RESTUtil.merge(
                original_catalog_options.to_map(),
                merged_token
            )

            self.assertIn("custom.key", merged_properties)
            self.assertEqual(merged_properties["custom.key"], "custom.value")
            self.assertIn(OssOptions.OSS_ACCESS_KEY_ID.key(), merged_properties)
            self.assertEqual(merged_properties[OssOptions.OSS_ACCESS_KEY_ID.key()], "token-access-key")

    def test_filesystem_property(self):
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )

            self.assertTrue(hasattr(file_io, 'filesystem'), "RESTTokenFileIO should have filesystem property")
            filesystem = file_io.filesystem
            self.assertIsNotNone(filesystem, "filesystem should not be None")

            self.assertTrue(hasattr(filesystem, 'open_input_file'),
                            "filesystem should support open_input_file method")

    def test_uri_reader_factory_property(self):
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )

            self.assertTrue(hasattr(file_io, 'uri_reader_factory'),
                            "RESTTokenFileIO should have uri_reader_factory property")
            uri_reader_factory = file_io.uri_reader_factory
            self.assertIsNotNone(uri_reader_factory, "uri_reader_factory should not be None")

            self.assertTrue(hasattr(uri_reader_factory, 'create'),
                            "uri_reader_factory should support create method")

    def test_filesystem_and_uri_reader_factory_after_serialization(self):
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            original_file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )

            pickled = pickle.dumps(original_file_io)
            restored_file_io = pickle.loads(pickled)

            self.assertIsNotNone(restored_file_io.filesystem,
                                 "filesystem should work after deserialization")
            self.assertIsNotNone(restored_file_io.uri_reader_factory,
                                 "uri_reader_factory should work after deserialization")

    def test_build_cache_key_with_dlf_auth(self):
        """Test that _build_cache_key includes DLF access_key_id for cache isolation."""
        from pypaimon.api.auth.dlf_provider import DLFAuthProvider
        from pypaimon.api.token_loader import DLFToken

        catalog_options = Options({
            CatalogOptions.URI.key(): "http://test-uri",
            CatalogOptions.TOKEN_PROVIDER.key(): "dlf",
            CatalogOptions.DLF_REGION.key(): "cn-hangzhou",
            CatalogOptions.DLF_ACCESS_KEY_ID.key(): "test-ak-123",
            CatalogOptions.DLF_ACCESS_KEY_SECRET.key(): "test-sk-456",
        })

        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                catalog_options
            )

        mock_api = MagicMock()
        mock_dlf_provider = MagicMock(spec=DLFAuthProvider)
        mock_dlf_provider.get_token.return_value = DLFToken(
            access_key_id="test-ak-123",
            access_key_secret="test-sk-456",
            security_token=None
        )
        mock_api.rest_auth_function.auth_provider = mock_dlf_provider
        file_io.api_instance = mock_api

        cache_key = file_io._build_cache_key()
        self.assertTrue(cache_key.startswith("test-ak-123:"),
                        f"Cache key should start with access_key_id, got: {cache_key}")
        self.assertIn(str(self.identifier), cache_key)

    def test_build_cache_key_with_bear_auth(self):
        """Test that _build_cache_key includes bearer token for cache isolation."""
        from pypaimon.api.auth.bearer import BearTokenAuthProvider

        catalog_options = Options({
            CatalogOptions.URI.key(): "http://test-uri",
            CatalogOptions.TOKEN_PROVIDER.key(): "bear",
            CatalogOptions.TOKEN.key(): "my-bearer-token",
        })

        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                catalog_options
            )

        mock_api = MagicMock()
        mock_bear_provider = MagicMock(spec=BearTokenAuthProvider)
        mock_bear_provider.token = "my-bearer-token"
        mock_api.rest_auth_function.auth_provider = mock_bear_provider
        file_io.api_instance = mock_api

        cache_key = file_io._build_cache_key()
        self.assertTrue(cache_key.startswith("my-bearer-token:"),
                        f"Cache key should start with bearer token, got: {cache_key}")
        self.assertIn(str(self.identifier), cache_key)

    def test_different_ak_same_table_token_isolation(self):
        """Test that two RESTTokenFileIO instances with different AKs on the same table
        do not share cached tokens."""
        from pypaimon.api.auth.dlf_provider import DLFAuthProvider
        from pypaimon.api.token_loader import DLFToken

        # Clear class-level token cache before test
        RESTTokenFileIO._TOKEN_CACHE.clear()
        RESTTokenFileIO._TOKEN_LOCKS.clear()

        identifier = Identifier.from_string("db.same_table")
        future_expiry = int(time.time() * 1000) + 7_200_000  # 2 hours from now

        # Create file_io_1 with AK "read-ak"
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io_1 = RESTTokenFileIO(
                identifier, self.warehouse_path,
                Options({
                    CatalogOptions.URI.key(): "http://test-uri",
                    CatalogOptions.TOKEN_PROVIDER.key(): "dlf",
                    CatalogOptions.DLF_REGION.key(): "cn-hangzhou",
                    CatalogOptions.DLF_ACCESS_KEY_ID.key(): "read-ak",
                    CatalogOptions.DLF_ACCESS_KEY_SECRET.key(): "read-sk",
                })
            )

        mock_api_1 = MagicMock()
        mock_dlf_1 = MagicMock(spec=DLFAuthProvider)
        mock_dlf_1.get_token.return_value = DLFToken("read-ak", "read-sk", None)
        mock_api_1.rest_auth_function.auth_provider = mock_dlf_1
        file_io_1.api_instance = mock_api_1

        read_token = RESTToken({"fs.oss.accessKeyId": "read-data-ak"}, future_expiry)
        file_io_1.token = read_token
        cache_key_1 = file_io_1._build_cache_key()
        file_io_1._set_cached_token(cache_key_1, read_token)

        # Create file_io_2 with AK "write-ak"
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io_2 = RESTTokenFileIO(
                identifier, self.warehouse_path,
                Options({
                    CatalogOptions.URI.key(): "http://test-uri",
                    CatalogOptions.TOKEN_PROVIDER.key(): "dlf",
                    CatalogOptions.DLF_REGION.key(): "cn-hangzhou",
                    CatalogOptions.DLF_ACCESS_KEY_ID.key(): "write-ak",
                    CatalogOptions.DLF_ACCESS_KEY_SECRET.key(): "write-sk",
                })
            )

        mock_api_2 = MagicMock()
        mock_dlf_2 = MagicMock(spec=DLFAuthProvider)
        mock_dlf_2.get_token.return_value = DLFToken("write-ak", "write-sk", None)
        mock_api_2.rest_auth_function.auth_provider = mock_dlf_2
        file_io_2.api_instance = mock_api_2

        cache_key_2 = file_io_2._build_cache_key()

        # Cache keys should be different
        self.assertNotEqual(cache_key_1, cache_key_2,
                            "Different AKs on the same table should produce different cache keys")

        # file_io_2 should NOT get file_io_1's cached token
        cached_for_2 = file_io_2._get_cached_token(cache_key_2)
        self.assertIsNone(cached_for_2,
                          "file_io_2 should not see file_io_1's cached token")

    def test_same_ak_same_table_token_reuse(self):
        """Test that two RESTTokenFileIO instances with the same AK on the same table
        can share cached tokens."""
        from pypaimon.api.auth.dlf_provider import DLFAuthProvider
        from pypaimon.api.token_loader import DLFToken

        # Clear class-level token cache before test
        RESTTokenFileIO._TOKEN_CACHE.clear()
        RESTTokenFileIO._TOKEN_LOCKS.clear()

        identifier = Identifier.from_string("db.same_table")
        future_expiry = int(time.time() * 1000) + 7_200_000

        def make_file_io():
            with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
                fio = RESTTokenFileIO(
                    identifier, self.warehouse_path,
                    Options({
                        CatalogOptions.URI.key(): "http://test-uri",
                        CatalogOptions.TOKEN_PROVIDER.key(): "dlf",
                        CatalogOptions.DLF_REGION.key(): "cn-hangzhou",
                        CatalogOptions.DLF_ACCESS_KEY_ID.key(): "same-ak",
                        CatalogOptions.DLF_ACCESS_KEY_SECRET.key(): "same-sk",
                    })
                )
            mock_api = MagicMock()
            mock_dlf = MagicMock(spec=DLFAuthProvider)
            mock_dlf.get_token.return_value = DLFToken("same-ak", "same-sk", None)
            mock_api.rest_auth_function.auth_provider = mock_dlf
            fio.api_instance = mock_api
            return fio

        file_io_1 = make_file_io()
        file_io_2 = make_file_io()

        # Cache keys should be the same
        self.assertEqual(file_io_1._build_cache_key(), file_io_2._build_cache_key(),
                         "Same AK on the same table should produce the same cache key")

        # Set token via file_io_1
        shared_token = RESTToken({"fs.oss.accessKeyId": "shared-data-ak"}, future_expiry)
        file_io_1._set_cached_token(file_io_1._build_cache_key(), shared_token)

        # file_io_2 should see the same token
        cached = file_io_2._get_cached_token(file_io_2._build_cache_key())
        self.assertIsNotNone(cached, "file_io_2 should see file_io_1's cached token")
        self.assertEqual(cached.token.get("fs.oss.accessKeyId"), "shared-data-ak")

    def test_token_cache_drops_expired_tokens_of_rotated_identities(self):
        now = int(time.time() * 1000)
        live = RESTToken({"fs.oss.accessKeyId": "live"}, now + 7_200_000)
        expired = RESTToken({"fs.oss.accessKeyId": "expired"}, now - 1)
        file_io = RESTTokenFileIO(self.identifier, self.warehouse_path, self.catalog_options)
        with patch.object(RESTTokenFileIO, '_TOKEN_CACHE', {}), \
                patch.object(RESTTokenFileIO, '_TOKEN_LOCKS', {}), \
                patch.object(RESTTokenFileIO, '_TOKEN_CACHE_PRUNE_SIZE', 4):
            for sts_ak in ("STS.1", "STS.2"):
                key = sts_ak + ":" + str(self.identifier)
                file_io._get_global_token_lock(key)
                file_io._set_cached_token(key, expired)
            busy = file_io._get_global_token_lock("STS.3:" + str(self.identifier))
            busy.acquire()
            self.addCleanup(busy.release)
            file_io._set_cached_token("STS.3:other", live)
            self.assertEqual(3, len(RESTTokenFileIO._TOKEN_CACHE))

            file_io._set_cached_token("STS.4:" + str(self.identifier), live)

            self.assertEqual(["STS.3:other", "STS.4:" + str(self.identifier)],
                             sorted(RESTTokenFileIO._TOKEN_CACHE))
            # A lock that a refresh still holds stays; idle locks of dropped tokens go.
            self.assertEqual(["STS.3:" + str(self.identifier)], list(RESTTokenFileIO._TOKEN_LOCKS))
            self.assertEqual(1024, RESTTokenFileIO._TOKEN_CACHE_PRUNE_SIZE)

    def test_token_cache_prune_size_doubles_with_live_tokens(self):
        now = int(time.time() * 1000)
        live = RESTToken({"fs.oss.accessKeyId": "live"}, now + 7_200_000)
        expired = RESTToken({"fs.oss.accessKeyId": "expired"}, now - 1)
        cache = {"live-%d" % i: live for i in range(1023)}
        cache["expired-0"] = expired
        file_io = RESTTokenFileIO(self.identifier, self.warehouse_path, self.catalog_options)
        with patch.object(RESTTokenFileIO, '_TOKEN_CACHE', cache), \
                patch.object(RESTTokenFileIO, '_TOKEN_LOCKS', {}), \
                patch.object(RESTTokenFileIO, '_TOKEN_CACHE_PRUNE_SIZE', 1024):
            file_io._set_cached_token("live-1023", live)
            self.assertNotIn("expired-0", cache)
            self.assertEqual(2048, RESTTokenFileIO._TOKEN_CACHE_PRUNE_SIZE)

            # Below the doubled size, caching another token scans nothing.
            file_io._set_cached_token("expired-1", expired)
            self.assertIn("expired-1", cache)
            self.assertEqual(1025, len(cache))

    def test_token_cache_prune_size_doubles_with_held_locks(self):
        held = {"refreshing-%d" % i: threading.Lock() for i in range(1100)}
        for lock in held.values():
            lock.acquire()
        file_io = RESTTokenFileIO(self.identifier, self.warehouse_path, self.catalog_options)
        with patch.object(RESTTokenFileIO, '_TOKEN_CACHE', {}), \
                patch.object(RESTTokenFileIO, '_TOKEN_LOCKS', dict(held)), \
                patch.object(RESTTokenFileIO, '_TOKEN_CACHE_PRUNE_SIZE', 1024):
            file_io._get_global_token_lock("new")
            # Locks of refreshes in flight stay, so the next prune waits until the lock count doubles.
            self.assertEqual(1101, len(RESTTokenFileIO._TOKEN_LOCKS))
            self.assertEqual(2200, RESTTokenFileIO._TOKEN_CACHE_PRUNE_SIZE)

    def test_failed_refreshes_drop_idle_locks(self):
        keys = ["STS.%d:%s" % (i, self.identifier) for i in range(6)]
        file_io = RESTTokenFileIO(self.identifier, self.warehouse_path, self.catalog_options)
        with patch.object(RESTTokenFileIO, '_TOKEN_CACHE', {}), \
                patch.object(RESTTokenFileIO, '_TOKEN_LOCKS', {}), \
                patch.object(RESTTokenFileIO, '_TOKEN_CACHE_PRUNE_SIZE', 4), \
                patch.object(RESTTokenFileIO, '_build_cache_key', side_effect=keys), \
                patch.object(RESTTokenFileIO, 'refresh_token', side_effect=RuntimeError("forbidden")):
            for _ in keys:
                with self.assertRaises(RuntimeError):
                    file_io.try_to_refresh_token()

            # The fifth lock found four idle ones without tokens and dropped them first.
            self.assertEqual(keys[4:], sorted(RESTTokenFileIO._TOKEN_LOCKS))
            self.assertEqual({}, RESTTokenFileIO._TOKEN_CACHE)
            self.assertEqual(1024, RESTTokenFileIO._TOKEN_CACHE_PRUNE_SIZE)

    def test_refresh_token_strips_system_table_suffix(self):
        """refresh_token() strips $snapshots suffix before requesting token."""
        system_identifier = Identifier.create("db", "my_table", system_table="snapshots")
        file_io = RESTTokenFileIO(
            system_identifier, self.warehouse_path, self.catalog_options)

        mock_response = MagicMock()
        mock_response.token = {'ak': 'test-ak'}
        mock_response.expires_at_millis = int(time.time() * 1000) + 7_200_000

        mock_api = MagicMock()
        mock_api.load_table_token.return_value = mock_response
        file_io.api_instance = mock_api

        file_io.refresh_token()

        called_identifier = mock_api.load_table_token.call_args[0][0]
        self.assertEqual(called_identifier.get_database_name(), "db")
        self.assertEqual(called_identifier.get_object_name(), "my_table")

    def test_refresh_token_keeps_normal_identifier(self):
        """refresh_token() does not modify normal (non-system) identifiers."""
        normal_identifier = Identifier.create("db", "my_table")
        file_io = RESTTokenFileIO(
            normal_identifier, self.warehouse_path, self.catalog_options)

        mock_response = MagicMock()
        mock_response.token = {'ak': 'test-ak'}
        mock_response.expires_at_millis = int(time.time() * 1000) + 7_200_000

        mock_api = MagicMock()
        mock_api.load_table_token.return_value = mock_response
        file_io.api_instance = mock_api

        file_io.refresh_token()

        called_identifier = mock_api.load_table_token.call_args[0][0]
        self.assertEqual(called_identifier.get_object_name(), "my_table")


if __name__ == '__main__':
    unittest.main()
