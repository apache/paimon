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
import pickle
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

import pyarrow.fs as pafs

from pypaimon.common.file_io import FileIO, create_temp_path
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions, OssOptions
from pypaimon.filesystem.local_file_io import LocalFileIO, _file_uri_path
from pypaimon.filesystem.oss_file_io import OssFileIO
from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO, _pyarrow_lt_7


class FileIOFactoryTest(unittest.TestCase):
    def test_windows_drive_paths_use_local_file_io(self):
        options = Options({})
        paths = [r"C:\warehouse", "C:/warehouse", r"d:\warehouse", "D:/warehouse",
                 "C:warehouse", "C:", r"C:\data set\episode%23#copy.txt"]
        with patch.object(os, "name", "nt"), \
                patch('pypaimon.filesystem.local_file_io.LocalFileIO') as local_io:
            for location in paths:
                with self.subTest(location=location):
                    self.assertIs(FileIO.get(location, options), local_io.return_value)
                    local_io.assert_called_with(location, options)

    def test_local_paths_keep_their_original_spelling(self):
        options = Options({})
        paths = ["warehouse", "./warehouse", "/tmp/warehouse", "file:///tmp/warehouse",
                 "file:///C:/warehouse", r"\\server\share\warehouse", "//server/share/warehouse"]
        with patch('pypaimon.filesystem.local_file_io.LocalFileIO') as local_io:
            for location in paths:
                with self.subTest(location=location):
                    self.assertIs(FileIO.get(location, options), local_io.return_value)
                    local_io.assert_called_with(location, options)

    def test_drive_letters_are_not_local_schemes_on_posix(self):
        options = Options({})
        with patch.object(os, "name", "posix"), \
                patch('pypaimon.filesystem.pyarrow_file_io.PyArrowFileIO') as remote_io:
            for location in [r"C:\warehouse", "C:/warehouse", "x:relative"]:
                with self.subTest(location=location):
                    self.assertIs(FileIO.get(location, options), remote_io.return_value)
                    remote_io.assert_called_with(location, options)

    def test_remote_uris_are_not_windows_drive_paths(self):
        options = Options({})
        with patch.object(os, "name", "nt"), \
                patch('pypaimon.filesystem.pyarrow_file_io.PyArrowFileIO') as remote_io:
            for location in ["c://server/warehouse", "s3://bucket/warehouse",
                             "gs://bucket/warehouse", "custom://server/warehouse"]:
                with self.subTest(location=location):
                    self.assertIs(FileIO.get(location, options), remote_io.return_value)
                    remote_io.assert_called_with(location, options)

    @unittest.skipUnless(os.name == "nt", "Requires native Windows paths")
    def test_windows_native_paths_read_and_write(self):
        with tempfile.TemporaryDirectory(prefix="file io # ") as directory:
            for resolving in [False, True]:
                with self.subTest(resolving=resolving):
                    options = Options({CatalogOptions.RESOLVING_FILE_IO_ENABLED.key(): resolving})
                    file_io = FileIO.get(directory, options)
                    location = os.path.join(directory, "episode%23-{}.txt".format(resolving))
                    try:
                        file_io.write_file(location, "native Windows path")
                        self.assertEqual(file_io.read_file_utf8(location), "native Windows path")
                        self.assertEqual(file_io.read_file_utf8(location.replace("\\", "/")),
                                         "native Windows path")
                        self.assertEqual(file_io.read_file_utf8(Path(location).as_uri()),
                                         "native Windows path")
                        self.assertTrue(Path(location).is_file())
                    finally:
                        file_io.close()


class FileIOTest(unittest.TestCase):
    """Test cases for FileIO.to_filesystem_path method."""

    @patch('pypaimon.common.file_io.uuid.uuid4', return_value='test-uuid')
    def test_create_temp_path(self, _):
        self.assertEqual(
            create_temp_path("oss://bucket/table/snapshot/snapshot-1"),
            "oss://bucket/table/snapshot/.snapshot-1.test-uuid.tmp")
        self.assertEqual(
            create_temp_path("snapshot-1"),
            ".snapshot-1.test-uuid.tmp")
        self.assertEqual(
            create_temp_path(r"C:\table\snapshot\snapshot-1"),
            r"C:\table\snapshot\.snapshot-1.test-uuid.tmp")

    def test_filesystem_path_conversion(self):
        """Test S3FileSystem path conversion with various formats."""
        file_io = PyArrowFileIO("s3://bucket/warehouse", Options({}))
        self.assertIsInstance(file_io.filesystem, pafs.S3FileSystem)

        # Test bucket and path
        self.assertEqual(file_io.to_filesystem_path("s3://my-bucket/path/to/file.txt"),
                         "my-bucket/path/to/file.txt")
        self.assertEqual(
            file_io.to_filesystem_path(
                "s3://my-bucket/path/episode%23copy%3F1.h5"),
            "my-bucket/path/episode%23copy%3F1.h5",
        )
        self.assertEqual(
            file_io.to_filesystem_path(
                "s3://my-bucket/path/episode%2523copy.h5"),
            "my-bucket/path/episode%2523copy.h5",
        )
        self.assertEqual(
            file_io.to_filesystem_path(
                "s3://my-bucket/t/p=a%2Fb/data.parquet"),
            "my-bucket/t/p=a%2Fb/data.parquet",
        )

        self.assertEqual(file_io.to_filesystem_path("oss://my-bucket/path/to/file.txt"),
                         "my-bucket/path/to/file.txt")

        # Test bucket only
        self.assertEqual(file_io.to_filesystem_path("s3://my-bucket"), "my-bucket")
        self.assertEqual(file_io.to_filesystem_path("oss://my-bucket"), "my-bucket")

        # Test scheme but no netloc
        self.assertEqual(file_io.to_filesystem_path("oss:///path/to/file.txt"), "path/to/file.txt")
        self.assertEqual(file_io.to_filesystem_path("s3:///path/to/file.txt"), "path/to/file.txt")

        # Test empty path
        self.assertEqual(file_io.to_filesystem_path("oss:///"), ".")

        # Test path without scheme
        self.assertEqual(file_io.to_filesystem_path("bucket/path/to/file.txt"),
                         "bucket/path/to/file.txt")

        # Test idempotency
        converted_path = "my-bucket/path/to/file.txt"
        self.assertEqual(file_io.to_filesystem_path(converted_path), converted_path)
        parent_str = str(Path(converted_path).parent)
        self.assertEqual(file_io.to_filesystem_path(parent_str), parent_str)

        oss_io = OssFileIO("oss://test-bucket/warehouse", Options({
            OssOptions.OSS_ENDPOINT.key(): 'oss-cn-hangzhou.aliyuncs.com',
            OssOptions.OSS_ACCESS_KEY_ID.key(): 'test-key',
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): 'test-secret',
            OssOptions.OSS_IMPL.key(): 'legacy',
        }))
        # PyArrow <16 bakes the bucket into endpoint_override, so keys omit it.
        bucket_stripped = oss_io._oss_bucket_in_endpoint
        got = oss_io.to_filesystem_path("oss://test-bucket/path/to/file.txt")
        self.assertEqual(got, "path/to/file.txt" if bucket_stripped else "test-bucket/path/to/file.txt")
        if bucket_stripped:
            self.assertEqual(oss_io.to_filesystem_path("db-xxx.db/tbl-xxx/data.parquet"),
                             "db-xxx.db/tbl-xxx/data.parquet")
            self.assertEqual(oss_io.to_filesystem_path("db-xxx.db/tbl-xxx"), "db-xxx.db/tbl-xxx")
            manifest_uri = "oss://test-bucket/warehouse/db.db/table/manifest/manifest-list-abc-0"
            manifest_key = oss_io.to_filesystem_path(manifest_uri)
            self.assertEqual(manifest_key, "warehouse/db.db/table/manifest/manifest-list-abc-0",
                             "OSS+PyArrow<16 must pass key only so manifest lands in the right bucket")
            self.assertFalse(manifest_key.startswith("test-bucket/"),
                             "path must not start with bucket or PyArrow <16 writes to wrong bucket")
        nf = MagicMock(type=pafs.FileType.NotFound)
        get_file_info_calls = []

        def record_get_file_info(paths):
            get_file_info_calls.append(list(paths))
            return [MagicMock(type=pafs.FileType.NotFound) for _ in paths]

        mock_fs = MagicMock()
        mock_fs.get_file_info.side_effect = record_get_file_info if bucket_stripped else [[nf], [nf]]
        mock_fs.create_dir = MagicMock()
        mock_fs.open_output_stream.return_value = MagicMock()
        oss_io.filesystem = mock_fs
        # Bucket-in-endpoint mode (PyArrow < 16) mkdirs probes the real bucket
        # over HTTP; keep the test offline.
        with patch("requests.get", return_value=MagicMock(status_code=403)):
            oss_io.new_output_stream("oss://test-bucket/path/to/file.txt")
        if bucket_stripped:
            # Legacy mkdirs must not create_dir (it would CreateBucket the
            # first key segment and corrupt the parent directory).
            mock_fs.create_dir.assert_not_called()
        else:
            mock_fs.create_dir.assert_called_once()
            path_str = oss_io.to_filesystem_path("oss://test-bucket/path/to/file.txt")
            expected_parent = "/".join(path_str.split("/")[:-1]) if "/" in path_str else str(Path(path_str).parent)
            self.assertEqual(mock_fs.create_dir.call_args[0][0], expected_parent)
        if bucket_stripped:
            for call_paths in get_file_info_calls:
                for p in call_paths:
                    self.assertFalse(
                        p.startswith("test-bucket/"),
                        "OSS+PyArrow<16 must pass key only to get_file_info, not bucket/key. Got: %r" % (p,)
                    )

    def test_legacy_oss_url_endpoint_passes_scheme(self):
        options = {
            OssOptions.OSS_ACCESS_KEY_ID.key(): 'test-key',
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): 'test-secret',
            OssOptions.OSS_IMPL.key(): 'legacy',
        }
        cases = (('http://127.0.0.1:9000/', 'http', '127.0.0.1:9000'),
                 ('HTTPS://oss-cn-hangzhou.aliyuncs.com', 'https', 'oss-cn-hangzhou.aliyuncs.com'),
                 ('oss-cn-hangzhou.aliyuncs.com', None, 'oss-cn-hangzhou.aliyuncs.com'))
        for endpoint, scheme, host in cases:
            with patch('pypaimon.filesystem.pyarrow_file_io.pafs.S3FileSystem') as s3_file_system:
                oss_io = OssFileIO("oss://test-bucket/warehouse", Options(
                    {**options, OssOptions.OSS_ENDPOINT.key(): endpoint}))
            kwargs = s3_file_system.call_args[1]
            self.assertEqual(scheme, kwargs.get('scheme'))
            expected = 'test-bucket.' + host if oss_io._oss_bucket_in_endpoint else host
            self.assertEqual(expected, kwargs['endpoint_override'])

    def test_legacy_oss_path_style_keeps_bucket_out_of_endpoint(self):
        options = Options({
            OssOptions.OSS_ENDPOINT.key(): 'http://10.0.0.1:8080',
            OssOptions.OSS_ACCESS_KEY_ID.key(): 'test-key',
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): 'test-secret',
            OssOptions.OSS_IMPL.key(): 'legacy',
            OssOptions.OSS_SECOND_LEVEL_DOMAIN_ENABLE.key(): 'true',
        })
        for version, virtual in (('15.0.0', None), ('16.0.0', False)):
            # the retry strategy is missing from the old pyarrow some CI jobs install
            with patch('pyarrow.__version__', version), \
                    patch('pypaimon.filesystem.pyarrow_file_io.pafs.AwsStandardS3RetryStrategy', create=True), \
                    patch('pypaimon.filesystem.pyarrow_file_io.pafs.S3FileSystem') as s3_file_system:
                oss_io = OssFileIO("oss://test-bucket/warehouse", options)
                kwargs = s3_file_system.call_args[1]
                # unpickling (e.g. in a Ray worker) must keep path-style addressing
                restored = pickle.loads(pickle.dumps(oss_io))
            self.assertEqual('10.0.0.1:8080', kwargs['endpoint_override'])
            self.assertEqual(virtual, kwargs.get('force_virtual_addressing'))
            self.assertFalse(oss_io._oss_bucket_in_endpoint)
            self.assertFalse(restored._oss_bucket_in_endpoint)
        oss_io = OssFileIO("oss://test-bucket/warehouse", options)
        self.assertEqual("test-bucket/warehouse/data-1.parquet",
                         oss_io.to_filesystem_path("oss://test-bucket/warehouse/data-1.parquet"))

    def test_exists(self):
        lt7 = _pyarrow_lt_7()
        with tempfile.TemporaryDirectory(prefix="file_io_nonexistent_") as tmpdir:
            file_io = LocalFileIO("file://" + tmpdir, Options({}))
            missing_uri = "file://" + os.path.join(tmpdir, "nonexistent_xyz")
            path_str = file_io.to_filesystem_path(missing_uri)
            raised = None
            infos = None
            try:
                infos = file_io.filesystem.get_file_info([path_str])
            except OSError as e:
                raised = e
            if lt7:
                if raised is not None:
                    err = str(raised).lower()
                    self.assertTrue("133" in err or "does not exist" in err or "not exist" in err, str(raised))
                else:
                    self.assertEqual(len(infos), 1)
                    self.assertEqual(infos[0].type, pafs.FileType.NotFound)
            else:
                self.assertIsNone(raised)
                self.assertEqual(len(infos), 1)
                self.assertEqual(infos[0].type, pafs.FileType.NotFound)
            self.assertFalse(file_io.exists(missing_uri))
            with self.assertRaises(FileNotFoundError):
                file_io.get_file_status(missing_uri)

    def test_local_filesystem_path_conversion(self):
        file_io = LocalFileIO("file:///tmp/warehouse", Options({}))
        self.assertIsInstance(file_io, LocalFileIO)

        # Test file:// scheme
        self.assertEqual(file_io.to_filesystem_path("file:///tmp/path/to/file.txt"),
                         os.path.normpath("/tmp/path/to/file.txt"))
        self.assertEqual(file_io.to_filesystem_path("file:///path/to/file.txt"),
                         os.path.normpath("/path/to/file.txt"))
        self.assertEqual(
            file_io.to_filesystem_path(
                "file:///tmp/episode%20%E4%B8%AD%E6%96%87.h5"),
            os.path.normpath("/tmp/episode 中文.h5"),
        )

        # Test empty paths
        self.assertEqual(file_io.to_filesystem_path("file://"), ".")
        self.assertEqual(file_io.to_filesystem_path("file:///"), os.path.normpath("/"))

        # Test paths without scheme
        self.assertEqual(file_io.to_filesystem_path("/tmp/path/to/file.txt"),
                         os.path.normpath("/tmp/path/to/file.txt"))
        self.assertEqual(file_io.to_filesystem_path("relative/path/to/file.txt"),
                         os.path.normpath("relative/path/to/file.txt"))
        self.assertEqual(file_io.to_filesystem_path("./relative/path/to/file.txt"),
                         "./" + os.path.normpath("relative/path/to/file.txt"))

        # Test idempotency
        converted_path = os.path.normpath("/tmp/path/to/file.txt")
        self.assertEqual(file_io.to_filesystem_path(converted_path), converted_path)
        parent_str = str(Path(converted_path).parent)
        self.assertEqual(file_io.to_filesystem_path(parent_str), parent_str)

    def test_windows_path_handling(self):
        file_io = LocalFileIO("file:///tmp/warehouse", Options({}))
        self.assertIsInstance(file_io, LocalFileIO)

        self.assertEqual(file_io.to_filesystem_path("C:\\path\\to\\file.txt"),
                         "C:\\path\\to\\file.txt")
        self.assertEqual(file_io.to_filesystem_path("C:/path/to/file.txt"),
                         os.path.normpath("C:/path/to/file.txt"))
        self.assertEqual(file_io.to_filesystem_path("C:"), "C:")

        # file:// scheme with Windows drive
        self.assertEqual(file_io.to_filesystem_path("file://C:/path/to/file.txt"),
                         os.path.normpath("C:/path/to/file.txt"))
        self.assertEqual(file_io.to_filesystem_path("file://C:/path"), os.path.normpath("C:/path"))
        self.assertEqual(file_io.to_filesystem_path("file://C:"), "C:")
        self.assertEqual(file_io.to_filesystem_path("file:///C:/path/to/file.txt"),
                         os.path.normpath("C:/path/to/file.txt") if os.name == "nt"
                         else "/C:/path/to/file.txt")

        # Windows path with S3FileSystem (should preserve)
        s3_file_io = PyArrowFileIO("s3://bucket/warehouse", Options({}))
        self.assertEqual(s3_file_io.to_filesystem_path("C:\\path\\to\\file.txt"),
                         "C:\\path\\to\\file.txt")

    def test_standard_windows_and_unc_file_uris_round_trip(self):
        self.assertEqual(
            r"C:\data set\episode.h5",
            _file_uri_path(
                urlparse("file:///C:/data%20set/episode.h5"), windows=True),
        )
        self.assertEqual(
            r"\\server\share\episode.h5",
            _file_uri_path(
                urlparse("file://server/share/episode.h5"), windows=True),
        )

    def test_local_file_status_uses_portable_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            location = Path(directory) / "schema-0"
            location.write_text("{}", encoding="utf-8")
            file_io = LocalFileIO(directory, Options({}))
            expected = location.absolute().as_posix()
            for path in [str(location), location.as_uri()]:
                with self.subTest(path=path):
                    status = file_io.get_file_status(path)
                    self.assertEqual(status.path, expected)
                    self.assertEqual(status.path.split('/')[-1], "schema-0")
                    self.assertEqual(file_io.filesystem.get_file_info([expected])[0].path,
                                     status.path)
            self.assertEqual([entry.path for entry in file_io.list_status(directory)], [expected])
            self.assertEqual(file_io.read_file_utf8(expected), "{}")

    def test_copy_files_from_local_status(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            target = Path(directory) / "target"
            source.mkdir()
            (source / "data.txt").write_text("content", encoding="utf-8")
            file_io = LocalFileIO(directory, Options({}))
            file_io.copy_files(source.as_uri(), target.as_uri())
            self.assertEqual((target / "data.txt").read_text(encoding="utf-8"), "content")
            self.assertEqual([entry.name for entry in target.iterdir()], ["data.txt"])

    def test_local_directory_listing_propagates_permission_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            file_io = LocalFileIO(directory, Options({}))
            with patch.object(Path, "iterdir", side_effect=PermissionError):
                with self.assertRaises(PermissionError):
                    file_io.list_status(directory)

    def test_path_normalization(self):
        """Test path normalization (multiple slashes)."""
        file_io = LocalFileIO("file:///tmp/warehouse", Options({}))
        self.assertEqual(file_io.to_filesystem_path("file://///tmp///path///file.txt"),
                         os.path.normpath("/tmp/path/file.txt"))

        s3_file_io = PyArrowFileIO("s3://bucket/warehouse", Options({}))
        self.assertEqual(s3_file_io.to_filesystem_path("s3://my-bucket///path///to///file.txt"),
                         "my-bucket/path/to/file.txt")

    def test_write_file_with_overwrite_flag(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_write_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = LocalFileIO(warehouse_path, Options({}))

            test_file_uri = f"file://{temp_dir}/overwrite_test.txt"
            expected_path = os.path.join(temp_dir, "overwrite_test.txt")

            # 1) Write to a new file with default overwrite=False
            file_io.write_file(test_file_uri, "first content")
            self.assertTrue(os.path.exists(expected_path))
            with open(expected_path, "r", encoding="utf-8") as f:
                self.assertEqual(f.read(), "first content")

            # 2) Attempt to write again with overwrite=False should raise FileExistsError
            with self.assertRaises(FileExistsError):
                file_io.write_file(test_file_uri, "second content", overwrite=False)

            # Ensure content is unchanged
            with open(expected_path, "r", encoding="utf-8") as f:
                self.assertEqual(f.read(), "first content")

            # 3) Write with overwrite=True should replace the content
            file_io.write_file(test_file_uri, "overwritten content", overwrite=True)
            with open(expected_path, "r", encoding="utf-8") as f:
                self.assertEqual(f.read(), "overwritten content")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_exists_does_not_catch_exception(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_exists_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = LocalFileIO(warehouse_path, Options({}))

            test_file = os.path.join(temp_dir, "test_file.txt")
            with open(test_file, "w") as f:
                f.write("test")
            self.assertTrue(file_io.exists(f"file://{test_file}"))
            self.assertFalse(file_io.exists(f"file://{temp_dir}/nonexistent.txt"))

            mock_path = MagicMock(spec=Path)
            mock_path.exists.side_effect = OSError("Permission denied")
            with patch.object(file_io, '_to_file', return_value=mock_path):
                with self.assertRaises(OSError) as context:
                    file_io.exists("file:///some/path")
                self.assertIn("Permission denied", str(context.exception))

            with patch('builtins.open', side_effect=OSError("Permission denied")):
                with self.assertRaises(OSError):
                    file_io.new_output_stream("file:///some/path/file.txt")

            mock_path = MagicMock(spec=Path)
            mock_path.is_dir.side_effect = OSError("Permission denied")
            with patch.object(file_io, '_to_file', return_value=mock_path):
                with self.assertRaises(OSError):
                    file_io.check_or_mkdirs("file:///some/path")

            with patch('builtins.open', side_effect=OSError("Permission denied")):
                with self.assertRaises(OSError):
                    file_io.write_file("file:///some/path", "content", overwrite=False)

            with patch('builtins.open', side_effect=OSError("Permission denied")):
                with self.assertRaises(OSError):
                    file_io.copy_file("file:///src", "file:///dst", overwrite=False)

            with patch.object(file_io, 'exists', return_value=True):
                with patch.object(file_io, 'read_file_utf8', side_effect=OSError("Read error")):
                    with self.assertRaises(OSError) as context:
                        file_io.read_overwritten_file_utf8("file:///some/path")
                    self.assertIn("Read error", str(context.exception))

            # rename() catches OSError and returns False (consistent with Java implementation)
            mock_src_path = MagicMock(spec=Path)
            mock_dst_path = MagicMock(spec=Path)
            mock_dst_path.parent = MagicMock()
            mock_dst_path.parent.exists.return_value = True
            mock_dst_path.exists.return_value = False
            mock_src_path.rename.side_effect = OSError("Network error")
            with patch.object(file_io, '_to_file', side_effect=[mock_src_path, mock_dst_path]):
                # rename() catches OSError and returns False, doesn't raise
                result = file_io.rename("file:///src", "file:///dst")
                self.assertFalse(result, "rename() should return False when OSError occurs")

            file_io.delete_quietly("file:///some/path")
            file_io.delete_directory_quietly("file:///some/path")

            oss_io = OssFileIO("oss://test-bucket/warehouse", Options({
                OssOptions.OSS_ENDPOINT.key(): 'oss-cn-hangzhou.aliyuncs.com',
                OssOptions.OSS_ACCESS_KEY_ID.key(): 'test-key',
                OssOptions.OSS_ACCESS_KEY_SECRET.key(): 'test-secret',
                OssOptions.OSS_IMPL.key(): 'legacy',
            }))
            mock_fs = MagicMock()
            mock_fs.get_file_info.return_value = [
                MagicMock(type=pafs.FileType.NotFound)]
            oss_io.filesystem = mock_fs
            self.assertFalse(oss_io.exists("oss://test-bucket/path/to/file.txt"))
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_delete_non_empty_directory_raises_error(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_delete_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = LocalFileIO(warehouse_path, Options({}))

            test_dir = os.path.join(temp_dir, "test_dir")
            os.makedirs(test_dir)
            test_file = os.path.join(test_dir, "test_file.txt")
            with open(test_file, "w") as f:
                f.write("test")

            with self.assertRaises(OSError) as context:
                file_io.delete(f"file://{test_dir}", recursive=False)
            self.assertIn("is not empty", str(context.exception))
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_delete_returns_false_when_file_not_exists(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_delete_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = LocalFileIO(warehouse_path, Options({}))

            result = file_io.delete(f"file://{temp_dir}/nonexistent.txt")
            self.assertFalse(result, "delete() should return False when file does not exist")

            result = file_io.delete(f"file://{temp_dir}/nonexistent_dir", recursive=False)
            self.assertFalse(result, "delete() should return False when directory does not exist")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_mkdirs_raises_error_when_path_is_file(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_mkdirs_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = LocalFileIO(warehouse_path, Options({}))

            test_file = os.path.join(temp_dir, "test_file.txt")
            with open(test_file, "w") as f:
                f.write("test")

            with self.assertRaises(FileExistsError) as context:
                file_io.mkdirs(f"file://{test_file}")
            self.assertIn("is not a directory", str(context.exception))
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_rename_returns_false_when_dst_exists(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_rename_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = LocalFileIO(warehouse_path, Options({}))

            src_file = os.path.join(temp_dir, "src.txt")
            dst_file = os.path.join(temp_dir, "dst.txt")
            with open(src_file, "w") as f:
                f.write("src")
            with open(dst_file, "w") as f:
                f.write("dst")

            result = file_io.rename(f"file://{src_file}", f"file://{dst_file}")
            self.assertFalse(result)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_get_file_status_raises_error_when_file_not_exists(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_get_file_status_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = LocalFileIO(warehouse_path, Options({}))

            with self.assertRaises(FileNotFoundError) as context:
                file_io.get_file_status(f"file://{temp_dir}/nonexistent.txt")
            self.assertIn("does not exist", str(context.exception))

            test_file = os.path.join(temp_dir, "test_file.txt")
            with open(test_file, "w") as f:
                f.write("test content")
            
            file_info = file_io.get_file_status(f"file://{test_file}")
            self.assertEqual(file_info.type, pafs.FileType.File)
            self.assertIsNotNone(file_info.size)

            with self.assertRaises(FileNotFoundError) as context:
                file_io.get_file_size(f"file://{temp_dir}/nonexistent.txt")
            self.assertIn("does not exist", str(context.exception))

            with self.assertRaises(FileNotFoundError) as context:
                file_io.is_dir(f"file://{temp_dir}/nonexistent_dir")
            self.assertIn("does not exist", str(context.exception))
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_copy_file(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_copy_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = LocalFileIO(warehouse_path, Options({}))

            source_file = os.path.join(temp_dir, "source.txt")
            target_file = os.path.join(temp_dir, "target.txt")
            
            with open(source_file, "w") as f:
                f.write("source content")
            
            # Test 1: Raises FileExistsError when target exists and overwrite=False
            with open(target_file, "w") as f:
                f.write("target content")
            
            with self.assertRaises(FileExistsError) as context:
                file_io.copy_file(f"file://{source_file}", f"file://{target_file}", overwrite=False)
            self.assertIn("already exists", str(context.exception))
            
            with open(target_file, "r") as f:
                self.assertEqual(f.read(), "target content")
            
            # Test 2: Overwrites when overwrite=True
            file_io.copy_file(f"file://{source_file}", f"file://{target_file}", overwrite=True)
            with open(target_file, "r") as f:
                self.assertEqual(f.read(), "source content")
            
            # Test 3: Creates parent directory if it doesn't exist
            target_file_in_subdir = os.path.join(temp_dir, "subdir", "target.txt")
            file_io.copy_file(f"file://{source_file}", f"file://{target_file_in_subdir}", overwrite=False)
            self.assertTrue(os.path.exists(target_file_in_subdir))
            with open(target_file_in_subdir, "r") as f:
                self.assertEqual(f.read(), "source content")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_try_to_write_atomic(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_try_write_atomic_test_")
        try:
            target_dir = os.path.join(temp_dir, "target_dir")
            normal_file = os.path.join(temp_dir, "normal_file.txt")
            
            from pypaimon.filesystem.local_file_io import LocalFileIO
            local_file_io = LocalFileIO(f"file://{temp_dir}", Options({}))
            os.makedirs(target_dir)
            self.assertFalse(
                local_file_io.try_to_write_atomic(f"file://{target_dir}", "test content"),
                "LocalFileIO should return False when target is a directory")
            self.assertEqual(len(os.listdir(target_dir)), 0, "No file should be created inside the directory")
            
            self.assertTrue(local_file_io.try_to_write_atomic(f"file://{normal_file}", "test content"))
            with open(normal_file, "r") as f:
                self.assertEqual(f.read(), "test content")
            
            os.remove(normal_file)
            local_file_io = LocalFileIO(f"file://{temp_dir}", Options({}))
            self.assertFalse(
                local_file_io.try_to_write_atomic(f"file://{target_dir}", "test content"),
                "LocalFileIO should return False when target is a directory")
            self.assertEqual(len(os.listdir(target_dir)), 0, "No file should be created inside the directory")
            
            self.assertTrue(local_file_io.try_to_write_atomic(f"file://{normal_file}", "test content"))
            with open(normal_file, "r") as f:
                self.assertEqual(f.read(), "test content")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_path_on_windows(self):
        oss_io = OssFileIO("oss://test-bucket/warehouse", Options({
            OssOptions.OSS_ENDPOINT.key(): 'oss-cn-hangzhou.aliyuncs.com',
            OssOptions.OSS_ACCESS_KEY_ID.key(): 'test-key',
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): 'test-secret',
            OssOptions.OSS_IMPL.key(): 'legacy',
        }))
        mock_fs = MagicMock()
        mock_fs.get_file_info.return_value = [MagicMock(type=pafs.FileType.NotFound)]
        mock_fs.create_dir = MagicMock()
        mock_fs.open_output_stream.return_value = MagicMock()
        mock_fs.move = MagicMock()
        mock_fs.copy_file = MagicMock()
        oss_io.filesystem = mock_fs

        # PyArrow < 16 mkdirs probes the real bucket over HTTP; keep the test offline.
        with patch("requests.get", return_value=MagicMock(status_code=403)):
            oss_io.new_output_stream("oss://test-bucket/db.db/tbl/bucket-0/data.parquet")
            oss_io.rename("oss://test-bucket/db.db/tbl/old.parquet",
                          "oss://test-bucket/db.db/tbl/new.parquet")
            oss_io.copy_file("oss://test-bucket/db.db/tbl/src.parquet",
                             "oss://test-bucket/db.db/tbl/dst.parquet")

        for call in mock_fs.create_dir.call_args_list:
            self.assertNotIn("\\", call[0][0], f"backslash in path: {call[0][0]}")


class HdfsFileIOTest(unittest.TestCase):
    """Cases for HDFS / ViewFS URI handling in PyArrowFileIO._initialize_hdfs_fs."""

    def _make_hdfs_env(self, env_patch):
        env_patch.setdefault('HADOOP_HOME', '/opt/hadoop')
        env_patch.setdefault('HADOOP_CONF_DIR', '/opt/hadoop/etc/hadoop')
        env_patch.setdefault('CLASSPATH', '')
        env_patch.setdefault('LD_LIBRARY_PATH', '')
        return env_patch

    def _make_file_io(self):
        file_io = PyArrowFileIO.__new__(PyArrowFileIO)
        file_io.properties = Options({})
        return file_io

    @patch('pypaimon.filesystem.pyarrow_file_io.subprocess.run')
    @patch('pypaimon.filesystem.pyarrow_file_io.pafs.HadoopFileSystem')
    def test_viewfs_uses_default_host(self, mock_hadoop_fs, mock_run):
        mock_run.return_value = MagicMock(stdout='/opt/hadoop/share/hadoop/common/*')
        mock_hadoop_fs.return_value = MagicMock()
        with patch.dict(os.environ, self._make_hdfs_env({}), clear=True):
            self._make_file_io()._initialize_hdfs_fs('viewfs', 'clusterName')
        mock_hadoop_fs.assert_called_once_with(host='default', port=0, user='hadoop')

    @patch('pypaimon.filesystem.pyarrow_file_io.subprocess.run')
    @patch('pypaimon.filesystem.pyarrow_file_io.pafs.HadoopFileSystem')
    def test_viewfs_without_netloc_uses_default_host(self, mock_hadoop_fs, mock_run):
        mock_run.return_value = MagicMock(stdout='/opt/hadoop/share/hadoop/common/*')
        mock_hadoop_fs.return_value = MagicMock()
        with patch.dict(os.environ, self._make_hdfs_env({}), clear=True):
            self._make_file_io()._initialize_hdfs_fs('viewfs', '')
        mock_hadoop_fs.assert_called_once_with(host='default', port=0, user='hadoop')

    @patch('pypaimon.filesystem.pyarrow_file_io.subprocess.run')
    @patch('pypaimon.filesystem.pyarrow_file_io.pafs.HadoopFileSystem')
    def test_hdfs_with_port_uses_explicit_host(self, mock_hadoop_fs, mock_run):
        mock_run.return_value = MagicMock(stdout='/opt/hadoop/share/hadoop/common/*')
        mock_hadoop_fs.return_value = MagicMock()
        with patch.dict(os.environ, self._make_hdfs_env({}), clear=True):
            self._make_file_io()._initialize_hdfs_fs('hdfs', 'namenode:8020')
        mock_hadoop_fs.assert_called_once_with(host='namenode', port=8020, user='hadoop')

    @patch('pypaimon.filesystem.pyarrow_file_io.subprocess.run')
    @patch('pypaimon.filesystem.pyarrow_file_io.pafs.HadoopFileSystem')
    def test_hdfs_ha_nameservice_without_port_uses_default_host(self, mock_hadoop_fs, mock_run):
        mock_run.return_value = MagicMock(stdout='/opt/hadoop/share/hadoop/common/*')
        mock_hadoop_fs.return_value = MagicMock()
        with patch.dict(os.environ, self._make_hdfs_env({}), clear=True):
            self._make_file_io()._initialize_hdfs_fs('hdfs', 'nameservice1')
        mock_hadoop_fs.assert_called_once_with(host='default', port=0, user='hadoop')

    @patch('pypaimon.filesystem.pyarrow_file_io.subprocess.run')
    @patch('pypaimon.filesystem.pyarrow_file_io.pafs.HadoopFileSystem')
    def test_hdfs_without_netloc_uses_default_host(self, mock_hadoop_fs, mock_run):
        mock_run.return_value = MagicMock(stdout='/opt/hadoop/share/hadoop/common/*')
        mock_hadoop_fs.return_value = MagicMock()
        with patch.dict(os.environ, self._make_hdfs_env({}), clear=True):
            self._make_file_io()._initialize_hdfs_fs('hdfs', '')
        mock_hadoop_fs.assert_called_once_with(host='default', port=0, user='hadoop')

    def test_hdfs_missing_hadoop_home_raises(self):
        with patch.dict(os.environ, {'HADOOP_CONF_DIR': '/opt/hadoop/etc/hadoop'}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                self._make_file_io()._initialize_hdfs_fs('hdfs', 'namenode:8020')
            self.assertIn('HADOOP_HOME', str(ctx.exception))

    def test_hdfs_missing_hadoop_conf_dir_raises(self):
        with patch.dict(os.environ, {'HADOOP_HOME': '/opt/hadoop'}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                self._make_file_io()._initialize_hdfs_fs('hdfs', 'namenode:8020')
            self.assertIn('HADOOP_CONF_DIR', str(ctx.exception))


@unittest.skipUnless(hasattr(pafs, 'GcsFileSystem'), "PyArrow GCS support not available")
class GCSFileIOPathTest(unittest.TestCase):
    """Unit tests for PyArrowFileIO.to_filesystem_path with GCS (no credentials required)."""

    def setUp(self):
        self.file_io = PyArrowFileIO("gs://my-bucket/warehouse", Options({}))

    def test_gcs_filesystem_type(self):
        """GCS warehouse path should produce a GcsFileSystem."""
        self.assertIsInstance(self.file_io.filesystem, pafs.GcsFileSystem)

    def test_gcs_path_conversion(self):
        """gs://bucket/key should map to bucket/key (no leading slash)."""
        self.assertEqual(
            self.file_io.to_filesystem_path("gs://my-bucket/path/to/file.parquet"),
            "my-bucket/path/to/file.parquet"
        )
        self.assertEqual(
            self.file_io.to_filesystem_path("gs://my-bucket/warehouse/db.db/tbl/data-0.parquet"),
            "my-bucket/warehouse/db.db/tbl/data-0.parquet"
        )

    def test_gcs_path_bucket_only(self):
        """gs://bucket with no path should return just the bucket name."""
        self.assertEqual(
            self.file_io.to_filesystem_path("gs://my-bucket"),
            "my-bucket"
        )

    def test_gcs_path_normalization(self):
        """Multiple consecutive slashes in the path should be collapsed."""
        self.assertEqual(
            self.file_io.to_filesystem_path("gs://my-bucket///path///to///file.parquet"),
            "my-bucket/path/to/file.parquet"
        )

    def test_gcs_path_idempotency(self):
        """Already-converted bucket/key paths should pass through unchanged."""
        converted = "my-bucket/path/to/file.parquet"
        self.assertEqual(self.file_io.to_filesystem_path(converted), converted)
        parent = str(Path(converted).parent)
        self.assertEqual(self.file_io.to_filesystem_path(parent), parent)

    def test_gcs_path_no_leading_slash(self):
        """to_filesystem_path must never return a path starting with '/'."""
        cases = [
            "gs://my-bucket/path/to/file.parquet",
            "gs://my-bucket",
            "gs://my-bucket///path",
        ]
        for uri in cases:
            result = self.file_io.to_filesystem_path(uri)
            self.assertFalse(
                result.startswith("/"),
                f"Path must not start with '/' for GcsFileSystem, got: {result!r} (input: {uri!r})"
            )


if __name__ == '__main__':
    unittest.main()
