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

"""Unit tests for PyArrow-backed OSS and S3-compatible storage.

No real OSS access is required.
"""

import base64
import hashlib
import os
import socketserver
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock
from urllib.parse import parse_qs, quote, unquote, urlsplit
from xml.etree import ElementTree
from xml.sax.saxutils import escape

import pyarrow
import pyarrow.fs as pafs
from packaging.version import parse

from pypaimon import CatalogFactory, Schema
from pypaimon.common.json_util import JSON
from pypaimon.common.options import Options
from pypaimon.common.options.config import OssOptions, S3Options
from pypaimon.filesystem.oss_file_io import OssFileIO
from pypaimon.filesystem.pyarrow_file_io import (
    PyArrowFileIO,
)
from pypaimon.schema.table_schema import TableSchema

TABLE_PATH = "oss://test-bucket/db-uuid.db/tbl-uuid"


def _file_info(path, file_type):
    return pafs.FileInfo(path, file_type)


def _set_listed_keys(file_io, *passes):
    file_io._s3_delete_client.list_objects_v2.side_effect = [
        {"Contents": [{"Key": key} for key in keys]}
        for keys in passes
    ] + [{"Contents": []}]


def _all_deleted_keys(client):
    keys = []
    for call in client.method_calls:
        if call[0] == "delete_objects":
            keys.extend(item["Key"] for item in call[2]["Delete"]["Objects"])
        elif call[0] == "delete_object":
            keys.append(call[2]["Key"])
    return keys


class _ThreadingHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads = True


class _DeleteRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _target(self):
        path = unquote(urlsplit(self.path).path).lstrip("/")
        bucket, _, key = path.partition("/")
        return bucket, key

    def _respond(self, status, body=b""):
        self.send_response(status)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_HEAD(self):
        self.server.requests.append((self.command, self.path))
        bucket, key = self._target()
        objects = (self.server.bucket_objects.get(bucket, ())
                   if hasattr(self.server, "bucket_objects") else self.server.objects)
        exists = not key or key in objects
        self._respond(200 if exists else 404)

    def do_GET(self):
        self.server.requests.append((self.command, self.path))
        required_region = getattr(self.server, "required_region", None)
        if required_region and "/{}/s3/aws4_request".format(required_region) not in \
                self.headers.get("Authorization", ""):
            return self._respond(
                400, b"<Error><Code>AuthorizationHeaderMalformed</Code></Error>")
        query = parse_qs(urlsplit(self.path).query)
        prefix = query.get("prefix", [""])[0]
        delimiter = query.get("delimiter", [""])[0]
        max_keys = int(query.get("max-keys", ["1000"])[0])
        bucket, _ = self._target()
        objects = (
            self.server.bucket_objects.get(bucket, ())
            if hasattr(self.server, "bucket_objects")
            else self.server.objects
        )
        keys = sorted(
            key for key in objects if key.startswith(prefix)
        )
        if max_keys == 1000 and hasattr(self.server, "forced_list_keys"):
            keys = self.server.forced_list_keys
        contents = []
        common_prefixes = set()
        for key in keys:
            suffix = key[len(prefix):]
            if delimiter and delimiter in suffix:
                common_prefixes.add(
                    prefix + suffix.split(delimiter, 1)[0] + delimiter)
            else:
                contents.append(key)
        contents = contents[:max_keys]
        encode_keys = getattr(self.server, "encode_list_keys", False)
        response_prefix = quote(prefix, safe="/") if encode_keys else prefix
        response_delimiter = quote(delimiter, safe="/") if encode_keys else delimiter
        response_contents = (
            [quote(key, safe="/") for key in contents]
            if encode_keys else contents)
        response_common_prefixes = (
            [quote(key, safe="/") for key in sorted(common_prefixes)]
            if encode_keys else sorted(common_prefixes))
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            '<Name>{}</Name><Prefix>{}</Prefix><Delimiter>{}</Delimiter>'
            '<KeyCount>{}</KeyCount><MaxKeys>{}</MaxKeys>{}'
            '<IsTruncated>false</IsTruncated>{}{}</ListBucketResult>'.format(
                escape(bucket), escape(response_prefix), escape(response_delimiter),
                len(contents) + len(common_prefixes), max_keys,
                '<EncodingType>url</EncodingType>' if encode_keys else '',
                "".join(
                    "<Contents><Key>{}</Key>"
                    "<LastModified>2026-01-01T00:00:00Z</LastModified>"
                    "<Size>{}</Size><StorageClass>STANDARD</StorageClass>"
                    "</Contents>".format(
                        escape(key), 0 if key.endswith("/") else 1)
                    for key in response_contents),
                "".join(
                    "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>".format(
                        escape(child))
                    for child in response_common_prefixes)))
        if (not hasattr(self.server, "bucket_objects")
                and keys == [self.server.prefix]
                and not self.server.late_object_added):
            self.server.objects.add(self.server.prefix + "late.parquet")
            self.server.late_object_added = True
        first = getattr(self.server, "prefix", "") + "first.parquet"
        if (not hasattr(self.server, "bucket_objects")
                and first in keys and not self.server.missing_object_removed):
            self.server.objects.discard(first)
            self.server.missing_object_removed = True
        encoded = body.encode("utf-8")
        self._respond(200, encoded)

    def do_DELETE(self):
        self.server.requests.append((self.command, self.path))
        bucket, key = self._target()
        if hasattr(self.server, "bucket_objects"):
            self.server.bucket_objects.setdefault(bucket, set()).discard(key)
        else:
            self.server.objects.discard(key)
            if key == getattr(self.server, "inject_late_after_delete", None):
                self.server.objects.add(self.server.prefix + "late.parquet")
        self._respond(204)

    def do_PUT(self):
        self.server.requests.append((self.command, self.path))
        if hasattr(self.server, "put_headers"):
            self.server.put_headers.append((self.path, dict(self.headers)))
        bucket, key = self._target()
        if hasattr(self.server, "bucket_objects"):
            self.server.bucket_objects.setdefault(bucket, set()).add(key)
        else:
            self.server.objects.add(key)
        self._respond(200)

    def _unexpected(self):
        self.server.requests.append((self.command, self.path))
        self.send_response(501)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self):
        self.server.requests.append((self.command, self.path))
        if "delete" not in urlsplit(self.path).query:
            return self._respond(501)
        body = self.rfile.read(int(self.headers["Content-Length"]))
        if any(name.lower().startswith("x-amz-checksum-")
               for name in self.headers):
            return self._respond(
                400, b"<Error><Code>InvalidRequest</Code></Error>")
        content_md5 = base64.b64encode(hashlib.md5(body).digest()).decode()
        if self.headers.get("Content-MD5") != content_md5:
            return self._respond(
                400, b"<Error><Code>MissingArgument</Code></Error>")
        if b"\x01" in body:
            return self._respond(400, b"<Error><Code>MalformedXML</Code></Error>")
        bucket, _ = self._target()
        keys = [item.text for item in ElementTree.fromstring(body).iter()
                if item.tag.rsplit("}", 1)[-1] == "Key"]
        objects = (self.server.bucket_objects.setdefault(bucket, set())
                   if hasattr(self.server, "bucket_objects") else self.server.objects)
        failed = getattr(self.server, "fail_delete_key_once", None)
        if failed in keys:
            self.server.fail_delete_key_once = None
        deleted = []
        for key in keys:
            if key != failed:
                objects.discard(key)
                deleted.append(key)
                if key == getattr(self.server, "inject_late_after_delete", None):
                    objects.add(self.server.prefix + "late.parquet")
        result = "<DeleteResult>{}{}</DeleteResult>".format(
            "".join("<Deleted><Key>{}</Key></Deleted>".format(escape(key))
                    for key in deleted),
            "<Error><Key>{}</Key><Code>AccessDenied</Code></Error>".format(
                escape(failed)) if failed in keys else "")
        self._respond(200, result.encode())

    def log_message(self, *args):
        pass


def _successful_batch_delete(**kwargs):
    return {"Deleted": kwargs["Delete"]["Objects"]}


def _missing_marker(**kwargs):
    from botocore.exceptions import ClientError
    raise ClientError({"Error": {"Code": "404"},
                       "ResponseMetadata": {"HTTPStatusCode": 404}}, "HeadObject")


class OssDeleteCompatibilityTest(unittest.TestCase):
    def _new_file_io(self, legacy):
        options = Options({
            OssOptions.OSS_ACCESS_KEY_ID.key(): "ak",
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): "sk",
            OssOptions.OSS_ENDPOINT.key(): "oss-cn-test.example.com",
            OssOptions.OSS_REGION.key(): "cn-test",
            OssOptions.OSS_IMPL.key(): "legacy",
        })
        with mock.patch.object(
                OssFileIO, "_initialize_oss_fs", return_value=mock.Mock()):
            file_io = OssFileIO("oss://test-bucket/", options)
        # _legacy_oss_mode() keys off the bucket-in-endpoint flag (PyArrow < 16).
        file_io._oss_bucket_in_endpoint = legacy
        file_io.filesystem = mock.Mock(spec=pafs.S3FileSystem)
        file_io._s3_delete_client = mock.Mock()
        file_io._s3_delete_client.delete_objects.side_effect = \
            _successful_batch_delete
        file_io._s3_delete_client.head_object.side_effect = _missing_marker
        file_io._s3_delete_client.get_bucket_versioning.return_value = {}
        return file_io

    def test_delete_normalizes_oss_uri_before_selecting_bucket(self):
        for path in ("oss://other-bucket/table", "oss://other-bucket.oss-cn-test.example.com/table",
                     "oss://AK:SK@endpoint/other-bucket/table"):
            with self.subTest(path=path):
                file_io = self._new_file_io(legacy=False)
                file_io._pyarrow_gte_22 = True
                file_io.filesystem.get_file_info.return_value = [
                    _file_info("other-bucket/table", pafs.FileType.Directory)]
                _set_listed_keys(file_io, ["table/data"], [])
                self.assertTrue(file_io.delete(path, recursive=True))
                file_io.filesystem.get_file_info.assert_called_once_with(["other-bucket/table"])
                client = file_io._s3_delete_client
                self.assertTrue(all(call[1]["Bucket"] == "other-bucket"
                                    for call in client.delete_objects.call_args_list))
                client.delete_object.assert_called_once_with(Bucket="other-bucket", Key="table/")

    def test_pyarrow_22_recursive_delete_batches_objects(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        data_dir = directory.rstrip("/") + "/data"
        data_file = directory.rstrip("/") + "/data/data.parquet"
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        _set_listed_keys(
            file_io, [data_dir.split("/", 1)[1] + "/", data_file.split("/", 1)[1]], [])

        self.assertTrue(file_io.delete(TABLE_PATH, recursive=True))

        client = file_io._s3_delete_client
        self.assertCountEqual([
            "db-uuid.db/tbl-uuid/data/data.parquet",
            "db-uuid.db/tbl-uuid/data/",
        ], [item["Key"] for item in
            client.delete_objects.call_args[1]["Delete"]["Objects"]])
        client.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="db-uuid.db/tbl-uuid/")
        client.put_object.assert_called_once_with(
            Bucket="test-bucket", Key="db-uuid.db/", Body=b"",
            ContentType="application/x-directory")
        file_io.filesystem.delete_file.assert_not_called()
        file_io.filesystem.delete_dir_contents.assert_not_called()
        file_io.filesystem.delete_dir.assert_not_called()

    def test_recursive_delete_preserves_existing_parent_marker(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        _set_listed_keys(file_io, ["db-uuid.db/tbl-uuid/data.parquet"], [])
        client = file_io._s3_delete_client
        client.head_object.side_effect = None
        client.head_object.return_value = {"ContentLength": 15,
                                           "Metadata": {"probe": "retained"}}

        self.assertTrue(file_io.delete(TABLE_PATH, recursive=True))

        self.assertEqual(
            [mock.call(Bucket="test-bucket", Key="db-uuid.db/")] * 2,
            client.head_object.call_args_list)
        client.put_object.assert_not_called()

    def test_versioned_oss_rejects_missing_parent_before_deletion(self):
        for status in ("Enabled", "Suspended"):
            for recursive in (False, True):
                with self.subTest(status=status, recursive=recursive):
                    file_io = self._new_file_io(legacy=False)
                    file_io._pyarrow_gte_22 = True
                    directory = file_io.to_filesystem_path(TABLE_PATH)
                    file_io.filesystem.get_file_info.side_effect = [
                        [_file_info(directory, pafs.FileType.Directory)], []]
                    if recursive:
                        _set_listed_keys(
                            file_io, ["db-uuid.db/tbl-uuid/data.parquet"], [])
                    client = file_io._s3_delete_client
                    client.get_bucket_versioning.return_value = {"Status": status}

                    with self.assertRaisesRegex(OSError, "versioning status " + status):
                        file_io.delete(TABLE_PATH, recursive=recursive)

                    client.delete_objects.assert_not_called()
                    client.delete_object.assert_not_called()
                    client.put_object.assert_not_called()

    def test_pyarrow_22_recursive_delete_preserves_late_objects(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        first = directory.rstrip("/") + "/first.parquet"
        late = directory.rstrip("/") + "/late.parquet"
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        _set_listed_keys(file_io, [first.split("/", 1)[1]], [late.split("/", 1)[1]], [])

        with self.assertRaisesRegex(OSError, "changed during deletion"):
            file_io.delete(TABLE_PATH, recursive=True)

        self.assertEqual(["db-uuid.db/tbl-uuid/first.parquet"],
                         _all_deleted_keys(file_io._s3_delete_client))
        file_io._s3_delete_client.put_object.assert_not_called()

    def test_recursive_delete_lists_all_pages_before_deleting(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        client = file_io._s3_delete_client

        def list_page(**kwargs):
            if kwargs.get("ContinuationToken") == "next":
                self.assertEqual(0, client.delete_objects.call_count)
                return {"Contents": [{"Key": "db-uuid.db/tbl-uuid/b"}]}
            if client.delete_objects.call_count:
                return {"Contents": []}
            return {
                "Contents": [{"Key": "db-uuid.db/tbl-uuid/a"}],
                "IsTruncated": True,
                "NextContinuationToken": "next",
            }

        client.list_objects_v2.side_effect = list_page
        self.assertTrue(file_io.delete(TABLE_PATH, recursive=True))
        self.assertCountEqual(
            ["db-uuid.db/tbl-uuid/a", "db-uuid.db/tbl-uuid/b"],
            _all_deleted_keys(client)[:-1])
        self.assertEqual("next", client.list_objects_v2.call_args_list[1][1][
            "ContinuationToken"])

    def test_recursive_delete_keeps_schema_zero_until_last(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        prefix = "db-uuid.db/tbl-uuid/"
        _set_listed_keys(file_io, [
            prefix + "schema/schema-0",
            prefix + "snapshot/snapshot-1",
            prefix + "schema/schema-1",
            prefix + "data/file.parquet",
        ], [])

        self.assertTrue(file_io.delete(TABLE_PATH, recursive=True))

        calls = _all_deleted_keys(file_io._s3_delete_client)
        self.assertCountEqual(
            [prefix + "snapshot/snapshot-1", prefix + "data/file.parquet"],
            calls[:2])
        self.assertEqual([
            prefix, prefix + "schema/schema-1", prefix + "schema/schema-0",
        ], calls[2:])

    def test_recursive_delete_preserves_schema_zero_when_new_table_appears(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        prefix = "db-uuid.db/tbl-uuid/"
        schema_zero = prefix + "schema/schema-0"
        data = prefix + "data/file.parquet"
        new_schema_zero = prefix + "new-table/schema/schema-0"
        _set_listed_keys(
            file_io, [schema_zero, data], [schema_zero, new_schema_zero])

        with self.assertRaisesRegex(OSError, "changed during deletion"):
            file_io.delete(TABLE_PATH, recursive=True)

        self.assertEqual([data], _all_deleted_keys(file_io._s3_delete_client))

    def test_recursive_delete_preserves_only_schema_one_for_retry(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        prefix = "db-uuid.db/tbl-uuid/"
        schema_one = prefix + "schema/schema-1"
        data = prefix + "data/file.parquet"
        late = prefix + "data/late.parquet"
        _set_listed_keys(file_io,
                         [schema_one, data], [schema_one, late],
                         [schema_one, late], [schema_one])

        with self.assertRaisesRegex(OSError, "changed during deletion"):
            file_io.delete(TABLE_PATH, recursive=True)

        client = file_io._s3_delete_client
        self.assertEqual([data], _all_deleted_keys(client))
        client.put_object.assert_not_called()

        self.assertTrue(file_io.delete(TABLE_PATH, recursive=True))
        self.assertEqual([data, late, prefix, schema_one],
                         _all_deleted_keys(client))

    def test_recursive_delete_preserves_only_schema_one_on_marker_error(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        prefix = "db-uuid.db/tbl-uuid/"
        schema_one = prefix + "schema/schema-1"
        _set_listed_keys(file_io, [schema_one], [schema_one])
        client = file_io._s3_delete_client
        client.delete_object.side_effect = OSError("marker deletion failed")

        with self.assertRaisesRegex(OSError, "marker deletion failed"):
            file_io.delete(TABLE_PATH, recursive=True)

        client.delete_objects.assert_not_called()
        client.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key=prefix)

    def test_recursive_delete_allows_known_schema_zeros(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        path = "oss://test-bucket/db-uuid.db"
        directory = file_io.to_filesystem_path(path)
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        first = "db-uuid.db/a/schema/schema-0"
        second = "db-uuid.db/b/schema/schema-0"
        _set_listed_keys(file_io, [first, second], [second])

        self.assertTrue(file_io.delete(path, recursive=True))

        calls = _all_deleted_keys(file_io._s3_delete_client)
        self.assertEqual("db-uuid.db/", calls[0])
        self.assertCountEqual([first, second], calls[1:])

    def test_recursive_delete_times_out_when_directory_keeps_changing(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        file_io._s3_delete_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "db-uuid.db/tbl-uuid/data.parquet"}],
            "IsTruncated": True,
            "NextContinuationToken": "next",
        }
        clock = mock.Mock()
        clock.monotonic.side_effect = [0, 0, 2]

        with mock.patch("pypaimon.filesystem.pyarrow_file_io.time", clock), \
                mock.patch(
                    "pypaimon.filesystem.pyarrow_file_io._S3_DELETE_TIMEOUT_SECONDS", 1):
            with self.assertRaisesRegex(TimeoutError, "deleting S3 directory"):
                file_io.delete(TABLE_PATH, recursive=True)

        file_io._s3_delete_client.list_objects_v2.assert_called_once()
        file_io._s3_delete_client.delete_objects.assert_not_called()
        file_io._s3_delete_client.delete_object.assert_not_called()
        file_io._s3_delete_client.put_object.assert_not_called()

    def test_recursive_delete_stops_before_next_batch_after_deadline(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        prefix = "db-uuid.db/tbl-uuid/"
        keys = [prefix + "data/file-{}".format(i) for i in range(1001)]
        schema_zero = prefix + "schema/schema-0"
        client = file_io._s3_delete_client
        client.list_objects_v2.return_value = {
            "Contents": [{"Key": key} for key in keys + [schema_zero]]}
        clock = mock.Mock()
        clock.monotonic.side_effect = \
            lambda: 2 if client.delete_objects.call_count >= 1 else 0

        with mock.patch("pypaimon.filesystem.pyarrow_file_io.time", clock), \
                mock.patch(
                    "pypaimon.filesystem.pyarrow_file_io._S3_DELETE_TIMEOUT_SECONDS", 1):
            with self.assertRaisesRegex(TimeoutError, "deleting S3 directory"):
                file_io.delete(TABLE_PATH, recursive=True)

        self.assertEqual(1, client.delete_objects.call_count)
        self.assertEqual(1000, len(
            client.delete_objects.call_args[1]["Delete"]["Objects"]))
        client.delete_object.assert_not_called()
        client.put_object.assert_not_called()

    def test_mixed_delete_stops_before_batch_after_deadline(self):
        client = mock.Mock()
        ordinary = "db-uuid.db/tbl-uuid/data/file.parquet"
        special = "db-uuid.db/tbl-uuid/data/part\rfile.parquet"
        client.delete_objects.return_value = {"Deleted": [{"Key": ordinary}]}
        clock = mock.Mock()
        clock.monotonic.side_effect = \
            lambda: 2 if client.delete_object.called else 0

        with mock.patch("pypaimon.filesystem.pyarrow_file_io.time", clock):
            with self.assertRaisesRegex(TimeoutError, "deleting S3 directory"):
                PyArrowFileIO._delete_s3_objects(
                    client, "test-bucket", [ordinary, special], 1,
                    TABLE_PATH)

        client.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key=special)
        client.delete_objects.assert_not_called()

    def test_recursive_delete_batches_at_most_1000_keys(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        prefix = "db-uuid.db/tbl-uuid/"
        keys = [prefix + "file-{}".format(i) for i in range(1001)]
        _set_listed_keys(file_io, keys, [])

        self.assertTrue(file_io.delete(TABLE_PATH, recursive=True))

        calls = file_io._s3_delete_client.delete_objects.call_args_list
        self.assertEqual([1000, 1], [
            len(call[1]["Delete"]["Objects"]) for call in calls])
        self.assertCountEqual(keys, [
            item["Key"] for call in calls
            for item in call[1]["Delete"]["Objects"]])

    def test_recursive_delete_preserves_schema_zero_on_batch_error(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        file_io.filesystem.get_file_info.return_value = [
            _file_info(directory, pafs.FileType.Directory)]
        prefix = "db-uuid.db/tbl-uuid/"
        data = prefix + "data/file.parquet"
        schema_zero = prefix + "schema/schema-0"
        _set_listed_keys(file_io, [data, schema_zero])
        client = file_io._s3_delete_client
        client.delete_objects.side_effect = None
        client.delete_objects.return_value = {
            "Deleted": [], "Errors": [{"Key": data, "Code": "AccessDenied"}]}

        with self.assertRaisesRegex(OSError, "batch delete incomplete") as error:
            file_io.delete(TABLE_PATH, recursive=True)
        self.assertIn("AccessDenied", str(error.exception))
        self.assertIn(data, str(error.exception))

        client.delete_object.assert_not_called()
        client.put_object.assert_not_called()
        self.assertEqual([data], [
            item["Key"] for item in
            client.delete_objects.call_args[1]["Delete"]["Objects"]])

    def test_pre_pyarrow_22_recursive_delete_keeps_native_batch(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = False
        directory = file_io.to_filesystem_path(TABLE_PATH)
        file_io.filesystem.get_file_info.side_effect = [
            [_file_info(directory, pafs.FileType.Directory)],
        ]

        self.assertTrue(file_io.delete(TABLE_PATH, recursive=True))

        file_io.filesystem.delete_dir_contents.assert_called_once_with(directory)
        file_io.filesystem.delete_dir.assert_called_once_with(directory)
        file_io.filesystem.delete_file.assert_not_called()

    def test_modern_non_recursive_delete_removes_empty_directory(self):
        file_io = self._new_file_io(legacy=False)
        file_io._pyarrow_gte_22 = True
        directory = file_io.to_filesystem_path(TABLE_PATH)
        file_io.filesystem.get_file_info.side_effect = [
            [_file_info(directory, pafs.FileType.Directory)],
            [],
        ]

        self.assertTrue(file_io.delete(TABLE_PATH))

        file_io.filesystem.delete_file.assert_not_called()
        file_io.filesystem.delete_dir.assert_not_called()
        file_io._s3_delete_client.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="db-uuid.db/tbl-uuid/")
        file_io._s3_delete_client.put_object.assert_called_once_with(
            Bucket="test-bucket", Key="db-uuid.db/", Body=b"",
            ContentType="application/x-directory")

    def test_delete_rejects_bucket_root(self):
        for legacy, jindo in ((False, False), (True, False), (False, True)):
            file_io = self._new_file_io(legacy=legacy)
            file_io._use_jindo = jindo
            file_io._pyarrow_gte_22 = True
            for recursive in (False, True):
                with self.subTest(legacy=legacy, jindo=jindo,
                                  recursive=recursive):
                    with self.assertRaisesRegex(OSError, "bucket root"):
                        file_io.delete("oss://test-bucket/", recursive)

            file_io.filesystem.get_file_info.assert_not_called()
            file_io._s3_delete_client.delete_object.assert_not_called()
            file_io._s3_delete_client.put_object.assert_not_called()

    @unittest.skipUnless(
        parse(pyarrow.__version__) >= parse("22.0.0"),
        "requires PyArrow 22+ and boto3",
    )
    def test_delete_client_uses_pyarrow_resolved_region(self):
        server = _ThreadingHTTPServer(
            ("127.0.0.1", 0), _DeleteRequestHandler)
        server.requests = []
        server.bucket_objects = {
            "test-bucket": {"table/", "table/data.parquet"}}
        server.required_region = "eu-west-1"
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.start()
        try:
            options = Options({
                S3Options.S3_ACCESS_KEY_ID.key(): "ak",
                S3Options.S3_ACCESS_KEY_SECRET.key(): "sk",
                S3Options.S3_ENDPOINT.key():
                    "http://127.0.0.1:{}".format(server.server_port),
                "fs.s3.path.style.access": "true",
            })
            with mock.patch.dict(os.environ, {
                    "AWS_REGION": "eu-west-1",
                    "NO_PROXY": "127.0.0.1,localhost",
                    "no_proxy": "127.0.0.1,localhost",
            }, clear=True):
                file_io = PyArrowFileIO("s3://test-bucket/table", options)
                self.assertEqual("eu-west-1", file_io.filesystem.region)
                self.assertTrue(file_io.exists("s3://test-bucket/table"))
                self.assertTrue(file_io.delete(
                    "s3://test-bucket/table", recursive=True))
                file_io._s3_delete_client.close()
            self.assertEqual(set(), server.bucket_objects["test-bucket"])
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join()

    def test_modern_non_recursive_delete_rejects_non_empty_directory(self):
        file_io = self._new_file_io(legacy=False)
        directory = file_io.to_filesystem_path(TABLE_PATH)
        data_file = directory.rstrip("/") + "/data.parquet"
        file_io.filesystem.get_file_info.side_effect = [
            [_file_info(directory, pafs.FileType.Directory)],
            [_file_info(data_file, pafs.FileType.File)],
        ]

        with self.assertRaisesRegex(OSError, "is not empty"):
            file_io.delete(TABLE_PATH)

        file_io.filesystem.delete_file.assert_not_called()


class CustomS3EndpointTest(unittest.TestCase):
    def _new_file_io(self, scheme="s3"):
        options = Options({
            S3Options.S3_ACCESS_KEY_ID.key(): "ak",
            S3Options.S3_ACCESS_KEY_SECRET.key(): "sk",
            S3Options.S3_ENDPOINT.key(): "http://minio:9000",
            S3Options.S3_REGION.key(): "us-east-1",
        })
        with mock.patch.object(
                PyArrowFileIO, "_initialize_s3_fs", return_value=mock.Mock()):
            file_io = PyArrowFileIO(
                "{}://test-bucket/warehouse".format(scheme), options)
        file_io.filesystem = mock.Mock(spec=pafs.S3FileSystem)
        file_io._s3_delete_client = mock.Mock()
        file_io._s3_delete_client.delete_objects.side_effect = \
            _successful_batch_delete
        file_io._s3_delete_client.head_object.side_effect = _missing_marker
        return file_io

    def test_pickle_discards_cached_delete_client(self):
        file_io = self._new_file_io()
        state = file_io.__getstate__()
        self.assertNotIn("_s3_delete_client", state)
        restored = object.__new__(PyArrowFileIO)
        with mock.patch.object(PyArrowFileIO, "_initialize_s3_fs", return_value=mock.Mock()):
            restored.__setstate__(state)
        self.assertIsNone(restored._s3_delete_client)

    def test_jindo_and_native_s3_do_not_use_delete_fallback(self):
        file_io = self._new_file_io()
        file_io._pyarrow_gte_22 = True
        file_io._use_jindo = True
        self.assertFalse(file_io._uses_s3_delete_fallback())
        file_io._use_jindo = False
        file_io._s3_endpoint = None
        self.assertFalse(file_io._uses_s3_delete_fallback())

    def test_delete_rejects_bucket_root_with_native_or_fallback(self):
        file_io = self._new_file_io()
        for gte_22 in (False, True):
            file_io._pyarrow_gte_22 = gte_22
            for recursive in (False, True):
                with self.subTest(gte_22=gte_22, recursive=recursive):
                    with self.assertRaisesRegex(OSError, "bucket root"):
                        file_io.delete("s3://test-bucket/", recursive)

        file_io.filesystem.get_file_info.assert_not_called()
        file_io.filesystem.delete_dir_contents.assert_not_called()
        file_io._s3_delete_client.delete_object.assert_not_called()

    def test_recursive_delete_rejects_listed_keys_outside_prefix(self):
        file_io = self._new_file_io()
        file_io._pyarrow_gte_22 = True
        file_io.filesystem.get_file_info.return_value = [
            _file_info("test-bucket/table", pafs.FileType.Directory)]
        _set_listed_keys(file_io, [
            "table/data.parquet", "table-other/keep.parquet"])

        with self.assertRaisesRegex(OSError, "outside prefix"):
            file_io.delete("s3://test-bucket/table", recursive=True)

        file_io._s3_delete_client.delete_object.assert_not_called()
        file_io._s3_delete_client.put_object.assert_not_called()

    def test_recursive_delete_rejects_mismatched_list_scope(self):
        for scope in ({"Name": "other-bucket"},
                      {"Prefix": "table-other/"}):
            file_io = self._new_file_io()
            file_io._pyarrow_gte_22 = True
            file_io.filesystem.get_file_info.return_value = [
                _file_info("test-bucket/table", pafs.FileType.Directory)]
            file_io._s3_delete_client.list_objects_v2.return_value = {
                "Contents": [{"Key": "table/data.parquet"}], **scope}

            with self.subTest(scope=scope):
                with self.assertRaisesRegex(OSError, "different bucket or prefix"):
                    file_io.delete("s3://test-bucket/table", recursive=True)

            file_io._s3_delete_client.delete_object.assert_not_called()
            file_io._s3_delete_client.put_object.assert_not_called()

    def test_pyarrow_22_recursive_delete_uses_batch(self):
        for scheme in ("s3", "s3a", "s3n"):
            with self.subTest(scheme=scheme):
                file_io = self._new_file_io(scheme)
                file_io._pyarrow_gte_22 = True
                path = "{}://test-bucket/table".format(scheme)
                directory = file_io.to_filesystem_path(path)
                data_file = directory + "/data.parquet"
                file_io.filesystem.get_file_info.return_value = [
                    _file_info(directory, pafs.FileType.Directory)]
                _set_listed_keys(file_io, [data_file.split("/", 1)[1]], [])

                self.assertTrue(file_io.delete(path, recursive=True))

                file_io.filesystem.delete_file.assert_not_called()
                file_io.filesystem.delete_dir_contents.assert_not_called()
                file_io._s3_delete_client.delete_objects.assert_called_once_with(
                    Bucket="test-bucket", Delete={
                        "Objects": [{"Key": "table/data.parquet"}],
                        "Quiet": False})
                file_io._s3_delete_client.delete_object.assert_called_once_with(
                    Bucket="test-bucket", Key="table/")

    def test_recursive_delete_uses_bucket_from_target_uri(self):
        file_io = self._new_file_io()
        file_io._pyarrow_gte_22 = True
        file_io.to_filesystem_path = mock.Mock(
            return_value="target-bucket/table")
        file_io.filesystem.get_file_info.return_value = [
            _file_info("target-bucket/table", pafs.FileType.Directory)]
        _set_listed_keys(file_io, ["table/data.parquet"], [])

        self.assertTrue(file_io.delete(
            "s3://target-bucket/table", recursive=True))

        file_io._s3_delete_client.delete_objects.assert_called_once_with(
            Bucket="target-bucket", Delete={
                "Objects": [{"Key": "table/data.parquet"}], "Quiet": False})
        file_io._s3_delete_client.delete_object.assert_called_once_with(
            Bucket="target-bucket", Key="table/")

    def test_recursive_delete_uses_bucket_from_target_filesystem_path(self):
        file_io = self._new_file_io()
        file_io._pyarrow_gte_22 = True
        file_io.filesystem.get_file_info.return_value = [
            _file_info("target-bucket/table", pafs.FileType.Directory)]
        _set_listed_keys(file_io, ["table/data.parquet"], [])

        self.assertTrue(file_io.delete(
            "target-bucket/table", recursive=True))

        file_io._s3_delete_client.delete_objects.assert_called_once_with(
            Bucket="target-bucket", Delete={
                "Objects": [{"Key": "table/data.parquet"}], "Quiet": False})
        file_io._s3_delete_client.delete_object.assert_called_once_with(
            Bucket="target-bucket", Key="table/")

    def test_pre_pyarrow_22_cross_bucket_delete_keeps_native_path(self):
        file_io = self._new_file_io()
        file_io._pyarrow_gte_22 = False
        file_io.to_filesystem_path = mock.Mock(
            return_value="target-bucket/table")
        file_io.filesystem.get_file_info.return_value = [
            _file_info("target-bucket/table", pafs.FileType.Directory)]

        self.assertTrue(file_io.delete(
            "s3://target-bucket/table", recursive=True))

        file_io.filesystem.delete_dir_contents.assert_called_once_with(
            "target-bucket/table")
        file_io.filesystem.delete_dir.assert_called_once_with(
            "target-bucket/table")
        file_io._s3_delete_client.delete_object.assert_not_called()

    def test_non_recursive_delete_uses_bucket_from_target_uri(self):
        file_io = self._new_file_io()
        file_io._pyarrow_gte_22 = True
        file_io.to_filesystem_path = mock.Mock(
            return_value="target-bucket/table")
        file_io.filesystem.get_file_info.side_effect = [
            [_file_info("target-bucket/table", pafs.FileType.Directory)],
            [],
        ]

        self.assertTrue(file_io.delete("s3://target-bucket/table"))

        file_io._s3_delete_client.delete_object.assert_called_once_with(
            Bucket="target-bucket", Key="table/")

    @unittest.skipUnless(
        parse(pyarrow.__version__) >= parse("22.0.0"),
        "requires PyArrow 22+ and boto3",
    )
    def test_delete_uses_cross_bucket_list_status_path(self):
        server = _ThreadingHTTPServer(
            ("127.0.0.1", 0), _DeleteRequestHandler)
        server.requests = []
        server.put_headers = []
        source_objects = {
            "parent/child/", "parent/child/keep.parquet"}
        server.bucket_objects = {
            "source-bucket": set(source_objects),
            "target-bucket": {
                "parent/child/", "parent/child/delete.parquet",
                "parent-other/keep.parquet"},
        }
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.start()
        try:
            options = Options({
                S3Options.S3_ACCESS_KEY_ID.key(): "ak",
                S3Options.S3_ACCESS_KEY_SECRET.key(): "sk",
                S3Options.S3_ENDPOINT.key():
                    "http://127.0.0.1:{}".format(server.server_port),
                S3Options.S3_REGION.key(): "us-east-1",
                "fs.s3.path.style.access": "true",
            })
            with mock.patch.dict(os.environ, {
                    "AWS_REQUEST_CHECKSUM_CALCULATION": "WHEN_SUPPORTED",
                    "NO_PROXY": "127.0.0.1,localhost",
                    "no_proxy": "127.0.0.1,localhost",
            }):
                file_io = PyArrowFileIO(
                    "s3://source-bucket/warehouse", options)
                for bucket in ("source-bucket", "target-bucket"):
                    with self.assertRaisesRegex(OSError, "bucket root"):
                        file_io.delete(
                            "s3://{}/".format(bucket), recursive=True)
                self.assertFalse(any(
                    method in ("DELETE", "PUT", "POST")
                    for method, _ in server.requests))
                before = {
                    bucket: set(keys)
                    for bucket, keys in server.bucket_objects.items()}
                server.forced_list_keys = [
                    "parent/child/delete.parquet",
                    "parent-other/keep.parquet"]
                with self.assertRaisesRegex(OSError, "outside prefix"):
                    file_io.delete(
                        "s3://target-bucket/parent/child", recursive=True)
                self.assertEqual(before, server.bucket_objects)
                self.assertFalse(any(
                    method in ("DELETE", "PUT", "POST")
                    for method, _ in server.requests))
                del server.forced_list_keys
                statuses = file_io.list_status(
                    "s3://target-bucket/parent")
                target = next(
                    status for status in statuses
                    if status.type == pafs.FileType.Directory)

                self.assertEqual(
                    "target-bucket/parent/child", target.path)
                for path in (
                        target.path,
                        "s3://target-bucket/parent/child",
                        "s3:/target-bucket/parent/child",
                        "s3:target-bucket/parent/child"):
                    for recursive in (False, True):
                        with self.subTest(path=path, recursive=recursive):
                            server.bucket_objects["target-bucket"] = {
                                "parent/child/"}
                            if recursive:
                                server.bucket_objects["target-bucket"].add(
                                    "parent/child/delete.parquet")
                            self.assertTrue(file_io.exists(path))
                            self.assertTrue(file_io.delete(path, recursive))
                            self.assertEqual(
                                {"parent/"}, server.bucket_objects["target-bucket"])
                            self.assertEqual(
                                pafs.FileType.Directory,
                                file_io.filesystem.get_file_info(
                                    "target-bucket/parent").type)
                            self.assertEqual(
                                source_objects,
                                server.bucket_objects["source-bucket"])
                file_io._s3_delete_client.close()

            self.assertEqual(
                source_objects, server.bucket_objects["source-bucket"])
            self.assertEqual({"parent/"}, server.bucket_objects["target-bucket"])
            self.assertTrue(all(
                urlsplit(path).path.startswith("/target-bucket/")
                for method, path in server.requests
                if method == "DELETE"))
            self.assertEqual(
                ["/target-bucket"] * 4,
                [urlsplit(path).path for method, path in server.requests
                 if method == "POST"])
            self.assertTrue(all(
                path == "/target-bucket/parent/"
                and not any(key.lower().startswith("x-amz-checksum-")
                            for key in headers)
                for path, headers in server.put_headers))
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join()

    @unittest.skipUnless(
        parse(pyarrow.__version__) >= parse("22.0.0"),
        "requires PyArrow 22+ and boto3",
    )
    def test_recursive_delete_control_character_keys(self):
        server = _ThreadingHTTPServer(
            ("127.0.0.1", 0), _DeleteRequestHandler)
        server.requests = []
        server.prefix = "parent/table/"
        server.encode_list_keys = True
        carriage_return = server.prefix + "data/part\rfile.parquet"
        control = server.prefix + "data/part\x01file.parquet"
        server.objects = {
            server.prefix, carriage_return, control,
            server.prefix + "schema/schema-0",
        }
        server.late_object_added = False
        server.missing_object_removed = False
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.start()
        try:
            options = Options({
                S3Options.S3_ACCESS_KEY_ID.key(): "ak",
                S3Options.S3_ACCESS_KEY_SECRET.key(): "sk",
                S3Options.S3_ENDPOINT.key():
                    "http://127.0.0.1:{}".format(server.server_port),
                S3Options.S3_REGION.key(): "us-east-1",
                "fs.s3.path.style.access": "true",
            })
            with mock.patch.object(
                    PyArrowFileIO, "_initialize_s3_fs", return_value=mock.Mock()), \
                    mock.patch.dict(os.environ, {
                        "NO_PROXY": "127.0.0.1,localhost",
                        "no_proxy": "127.0.0.1,localhost",
                    }):
                file_io = PyArrowFileIO("s3://test-bucket/parent/table", options)
                file_io.filesystem = mock.Mock(spec=pafs.S3FileSystem)
                file_io.filesystem.get_file_info.return_value = [
                    _file_info("test-bucket/parent/table",
                               pafs.FileType.Directory)]
                self.assertTrue(file_io.delete(
                    "s3://test-bucket/parent/table", recursive=True))
                file_io._s3_delete_client.close()

            self.assertEqual({"parent/"}, server.objects)
            individual = [unquote(urlsplit(path).path)
                          for method, path in server.requests
                          if method == "DELETE"]
            self.assertIn("/test-bucket/" + carriage_return, individual)
            self.assertIn("/test-bucket/" + control, individual)
            self.assertEqual(1, sum(
                method == "POST" for method, _ in server.requests))
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join()

    @unittest.skipUnless(
        parse(pyarrow.__version__) >= parse("22.0.0"),
        "requires PyArrow 22+ and boto3",
    )
    def test_recursive_delete_retries_partial_batch_failure(self):
        server = _ThreadingHTTPServer(
            ("127.0.0.1", 0), _DeleteRequestHandler)
        server.requests = []
        server.prefix = "parent/table/"
        deleted = server.prefix + "data/deleted.parquet"
        failed = server.prefix + "data/failed.parquet"
        schema_zero = server.prefix + "schema/schema-0"
        server.objects = {server.prefix, deleted, failed, schema_zero}
        server.fail_delete_key_once = failed
        server.late_object_added = False
        server.missing_object_removed = False
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.start()
        try:
            options = Options({
                S3Options.S3_ACCESS_KEY_ID.key(): "ak",
                S3Options.S3_ACCESS_KEY_SECRET.key(): "sk",
                S3Options.S3_ENDPOINT.key():
                    "http://127.0.0.1:{}".format(server.server_port),
                S3Options.S3_REGION.key(): "us-east-1",
                "fs.s3.path.style.access": "true",
            })
            with mock.patch.object(
                    PyArrowFileIO, "_initialize_s3_fs", return_value=mock.Mock()), \
                    mock.patch.dict(os.environ, {
                        "NO_PROXY": "127.0.0.1,localhost",
                        "no_proxy": "127.0.0.1,localhost",
                    }):
                file_io = PyArrowFileIO("s3://test-bucket/parent/table", options)
                file_io.filesystem = mock.Mock(spec=pafs.S3FileSystem)
                file_io.filesystem.get_file_info.return_value = [
                    _file_info("test-bucket/parent/table",
                               pafs.FileType.Directory)]
                with self.assertRaisesRegex(OSError, "AccessDenied") as error:
                    file_io.delete(
                        "s3://test-bucket/parent/table", recursive=True)
                self.assertIn(failed, str(error.exception))
                self.assertEqual(
                    {server.prefix, failed, schema_zero}, server.objects)
                self.assertTrue(file_io.delete(
                    "s3://test-bucket/parent/table", recursive=True))
                file_io._s3_delete_client.close()

            self.assertEqual({"parent/"}, server.objects)
            self.assertEqual(3, sum(
                method == "POST" for method, _ in server.requests))
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join()

    @unittest.skipUnless(
        parse(pyarrow.__version__) >= parse("22.0.0"),
        "requires PyArrow 22+ and boto3",
    )
    def test_catalog_can_retry_delete_with_only_schema_one(self):
        server = _ThreadingHTTPServer(
            ("127.0.0.1", 0), _DeleteRequestHandler)
        server.requests = []
        server.prefix = "db.db/t/"
        schema_one = server.prefix + "schema/schema-1"
        first = server.prefix + "first.parquet"
        late = server.prefix + "late.parquet"
        server.objects = {server.prefix, schema_one, first}
        server.inject_late_after_delete = first
        server.late_object_added = False
        server.missing_object_removed = False
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.start()
        try:
            options = {
                "warehouse": "s3://test-bucket/",
                S3Options.S3_ACCESS_KEY_ID.key(): "ak",
                S3Options.S3_ACCESS_KEY_SECRET.key(): "sk",
                S3Options.S3_ENDPOINT.key():
                    "http://127.0.0.1:{}".format(server.server_port),
                S3Options.S3_REGION.key(): "us-east-1",
                "fs.s3.path.style.access": "true",
            }
            schema = Schema.from_pyarrow_schema(
                pyarrow.schema([("value", pyarrow.int32())]))
            schema_json = JSON.to_json(TableSchema.from_schema(1, schema))
            with mock.patch.object(
                    PyArrowFileIO, "_initialize_s3_fs", return_value=mock.Mock()), \
                    mock.patch.dict(os.environ, {
                        "NO_PROXY": "127.0.0.1,localhost",
                        "no_proxy": "127.0.0.1,localhost",
                    }):
                catalog = CatalogFactory.create(options)
                file_io = catalog.file_io
                file_io.filesystem = mock.Mock(spec=pafs.S3FileSystem)
                file_io.filesystem.get_file_info.return_value = [
                    _file_info("test-bucket/db.db/t", pafs.FileType.Directory)]

                def exists(path):
                    key = file_io.to_filesystem_path(path).partition("/")[2]
                    return any(item == key or item.startswith(key.rstrip("/") + "/")
                               for item in server.objects)

                def list_status(path):
                    return [_file_info("test-bucket/" + schema_one,
                                       pafs.FileType.File)] \
                        if schema_one in server.objects else []

                with mock.patch.object(file_io, "exists", side_effect=exists), \
                        mock.patch.object(file_io, "list_status",
                                          side_effect=list_status), \
                        mock.patch.object(file_io, "read_file_utf8",
                                          return_value=schema_json):
                    self.assertEqual(1, catalog.get_table("db.t").table_schema.id)
                    with self.assertRaisesRegex(OSError, "changed during deletion"):
                        catalog.drop_table("db.t", ignore_if_not_exists=True)
                    self.assertEqual(1, catalog.get_table("db.t").table_schema.id)
                    self.assertEqual({server.prefix, schema_one, late}, server.objects)

                    catalog.drop_table("db.t", ignore_if_not_exists=True)
                    self.assertNotIn(schema_one, server.objects)
                    self.assertNotIn(late, server.objects)
                file_io._s3_delete_client.close()
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join()

    @unittest.skipUnless(
        parse(pyarrow.__version__) >= parse("22.0.0"),
        "requires PyArrow 22+ and boto3",
    )
    def test_recursive_delete_preserves_late_objects_during_races(self):
        server = _ThreadingHTTPServer(
            ("127.0.0.1", 0), _DeleteRequestHandler)
        server.requests = []
        server.prefix = "ta/ble/"
        decoy = "ta/ble-other/keep.parquet"
        server.objects = {
            server.prefix, server.prefix + "first.parquet", decoy}
        server.late_object_added = False
        server.missing_object_removed = False
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.start()
        try:
            options = Options({
                S3Options.S3_ACCESS_KEY_ID.key(): "ak",
                S3Options.S3_ACCESS_KEY_SECRET.key(): "sk",
                S3Options.S3_ENDPOINT.key():
                    "http://127.0.0.1:{}".format(server.server_port),
                S3Options.S3_REGION.key(): "us-east-1",
                "fs.s3.path.style.access": "true",
            })
            with mock.patch.object(
                    PyArrowFileIO, "_initialize_s3_fs", return_value=mock.Mock()), \
                    mock.patch.dict(os.environ, {
                        "NO_PROXY": "127.0.0.1,localhost",
                        "no_proxy": "127.0.0.1,localhost",
                    }):
                file_io = PyArrowFileIO(
                    "s3://source-bucket/warehouse", options)
                file_io.filesystem = mock.Mock(spec=pafs.S3FileSystem)
                file_io.filesystem.get_file_info.return_value = [
                    _file_info("/ta/ble", pafs.FileType.Directory)]

                self.assertTrue(file_io.delete(
                    "s3://target-bucket/ta//ble", recursive=True))
                file_io._s3_delete_client.close()

            self.assertTrue(server.late_object_added)
            self.assertTrue(server.missing_object_removed)
            self.assertEqual({decoy, "ta/", server.prefix + "late.parquet"},
                             server.objects)
            self.assertEqual(
                {"GET", "HEAD", "DELETE", "POST", "PUT"},
                {method for method, _ in server.requests})
            self.assertEqual([
                "/target-bucket/ta/ble/",
            ], [path for method, path in server.requests
                if method == "DELETE"])
            self.assertEqual(
                ["/target-bucket"],
                [urlsplit(path).path for method, path in server.requests
                 if method == "POST"])
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join()
