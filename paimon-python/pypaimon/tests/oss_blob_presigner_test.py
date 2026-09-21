# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import hashlib
from datetime import timedelta
from types import SimpleNamespace
from unittest import mock

import pytest

from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options
from pypaimon.filesystem.caching_file_io import CachingFileIO
from pypaimon.filesystem.oss_blob_presigner import create_presigned_url
from pypaimon.filesystem.oss_file_io import OssFileIO
from pypaimon.filesystem.resolving_file_io import ResolvingFileIO
from pypaimon.table.row.blob import Blob, BlobDescriptor

oss2 = pytest.importorskip("oss2")


_FINGERPRINT = (
    "cdba7930b06372d607da30924e956ec0"
    "c1e5f0a27eac8155b2d6a2973022f1a8"
)
_TARGET_KEY = "table/bucket-0/_bloburl_" + _FINGERPRINT


def _descriptor(offset=10, length=20):
    return BlobDescriptor(
        "oss://bucket/table/bucket-0/source.blob", offset, length)


def _metadata(length=20, fingerprint=_FINGERPRINT):
    return SimpleNamespace(
        content_length=length,
        content_type="application/octet-stream",
        headers={"x-oss-meta-paimon-blob-descriptor-sha256": fingerprint},
    )


def _bucket(endpoint="https://oss-cn-hangzhou.aliyuncs.com"):
    bucket = mock.MagicMock()
    bucket.bucket_name = "bucket"
    bucket.endpoint = endpoint
    bucket.sign_url.return_value = (
        endpoint.replace("oss-", "bucket.oss-")
        + "/"
        + _TARGET_KEY
        + "?x-oss-signature=test"
    )
    return bucket


def _missing():
    return oss2.exceptions.ServerError(
        404, {}, b"", {"Code": "NoSuchKey", "Message": "missing"})


def test_blob_api_delegates_to_file_io():
    descriptor = _descriptor()
    blob = Blob.from_descriptor(mock.MagicMock(), descriptor)
    file_io = mock.MagicMock()
    validity = timedelta(minutes=5)
    file_io.create_blob_presigned_url.return_value = "https://example"

    assert blob.to_presigned_url(
        file_io, "oss://bucket/table", validity) == "https://example"
    file_io.create_blob_presigned_url.assert_called_once_with(
        "oss://bucket/table", descriptor, validity)


def test_inline_blob_cannot_create_presigned_url():
    file_io = mock.MagicMock()
    with pytest.raises(RuntimeError, match="can not convert to descriptor"):
        Blob.from_data(b"inline").to_presigned_url(
            file_io, "oss://bucket/table", timedelta(minutes=5))
    file_io.create_blob_presigned_url.assert_not_called()


def test_oss_file_io_uses_descriptor_bucket_and_closes_session():
    descriptor = _descriptor()
    validity = timedelta(minutes=5)
    bucket = _bucket()
    session = mock.MagicMock()
    file_io = OssFileIO.__new__(OssFileIO)
    file_io._create_oss_bucket = mock.MagicMock(return_value=bucket)

    with mock.patch('oss2.Session', return_value=session), mock.patch(
            'pypaimon.filesystem.oss_blob_presigner.create_presigned_url',
            return_value='https://example') as create:
        assert file_io.create_blob_presigned_url(
            'oss://bucket/table', descriptor, validity) == 'https://example'

    file_io._create_oss_bucket.assert_called_once_with(session, 'bucket')
    create.assert_called_once_with(
        bucket, 'oss://bucket/table', descriptor, validity)
    session.session.close.assert_called_once_with()


def test_cache_hit_only_creates_fresh_url():
    bucket = _bucket()
    bucket.head_object.return_value = _metadata()

    url = create_presigned_url(
        bucket,
        "oss://bucket/table",
        _descriptor(),
        timedelta(minutes=5),
    )

    assert url.startswith("https://bucket.oss-cn-hangzhou.aliyuncs.com/")
    bucket.head_object.assert_called_once_with(_TARGET_KEY)
    bucket.sign_url.assert_called_once_with(
        'GET', _TARGET_KEY, 300, slash_safe=True)
    bucket.init_multipart_upload.assert_not_called()


def test_materializes_descriptor_range_and_reuses_java_object_key():
    bucket = _bucket()
    bucket.head_object.side_effect = [
        _missing(),
        SimpleNamespace(content_length=100),
        _metadata(),
    ]
    bucket.init_multipart_upload.return_value = SimpleNamespace(
        upload_id="upload-id")
    bucket.upload_part_copy.return_value = SimpleNamespace(etag="etag")

    create_presigned_url(
        bucket,
        "oss://bucket/table",
        _descriptor(),
        timedelta(seconds=30),
    )

    assert bucket.head_object.call_args_list == [
        mock.call(_TARGET_KEY),
        mock.call("table/bucket-0/source.blob"),
        mock.call(_TARGET_KEY),
    ]
    bucket.init_multipart_upload.assert_called_once_with(
        _TARGET_KEY,
        headers={
            "Content-Type": "application/octet-stream",
            "x-oss-meta-paimon-blob-descriptor-sha256": _FINGERPRINT,
        },
    )
    bucket.upload_part_copy.assert_called_once_with(
        "bucket",
        "table/bucket-0/source.blob",
        (10, 29),
        _TARGET_KEY,
        "upload-id",
        1,
    )
    parts = bucket.complete_multipart_upload.call_args[0][2]
    assert [(part.part_number, part.etag) for part in parts] == [(1, "etag")]


def test_zero_length_descriptor_uses_put_object():
    descriptor = _descriptor(offset=100, length=0)
    fingerprint = hashlib.sha256(descriptor.serialize()).hexdigest()
    target_key = "table/bucket-0/_bloburl_" + fingerprint
    bucket = _bucket()
    bucket.sign_url.return_value = (
        "https://bucket.oss-cn-hangzhou.aliyuncs.com/"
        + target_key
        + "?x-oss-signature=test"
    )
    bucket.head_object.side_effect = [
        _missing(),
        SimpleNamespace(content_length=100),
        _metadata(length=0, fingerprint=fingerprint),
    ]

    create_presigned_url(
        bucket,
        "oss://bucket/table",
        descriptor,
        timedelta(minutes=5),
    )

    bucket.put_object.assert_called_once_with(
        target_key,
        b'',
        headers={
            "Content-Type": "application/octet-stream",
            "x-oss-meta-paimon-blob-descriptor-sha256": fingerprint,
        },
    )
    bucket.init_multipart_upload.assert_not_called()


@pytest.mark.parametrize("descriptor", [
    _descriptor(offset=-1, length=20),
    _descriptor(offset=10, length=-1),
    _descriptor(offset=90, length=20),
])
def test_rejects_invalid_source_range(descriptor):
    bucket = _bucket()
    bucket.head_object.side_effect = [
        _missing(), SimpleNamespace(content_length=100)]
    with pytest.raises(ValueError, match="range is outside"):
        create_presigned_url(
            bucket,
            "oss://bucket/table",
            descriptor,
            timedelta(minutes=5),
        )
    bucket.init_multipart_upload.assert_not_called()


@pytest.mark.parametrize("table_root", [
    "s3://bucket/table",
    "oss://other/table",
    "oss://bucket/other",
    "oss://bucket/table-sibling",
])
def test_rejects_descriptor_outside_table_root(table_root):
    with pytest.raises(
            ValueError, match="scheme and authority|under table root"):
        create_presigned_url(
            _bucket(), table_root, _descriptor(), timedelta(minutes=5))


@pytest.mark.parametrize("validity", [
    timedelta(0),
    timedelta(seconds=-1),
    timedelta(microseconds=1),
])
def test_validity_must_be_positive_whole_seconds(validity):
    with pytest.raises(ValueError, match="positive whole seconds"):
        create_presigned_url(
            _bucket(), "oss://bucket/table", _descriptor(), validity)


def test_internal_endpoint_is_rewritten_to_public_endpoint():
    bucket = _bucket("https://oss-cn-hangzhou-internal.aliyuncs.com")
    bucket.head_object.return_value = _metadata()

    url = create_presigned_url(
        bucket,
        "oss://bucket/table",
        _descriptor(),
        timedelta(minutes=5),
    )

    assert url.startswith("https://bucket.oss-cn-hangzhou.aliyuncs.com/")
    assert "-internal" not in url


def test_rejects_non_https_endpoint_after_cache_hit():
    bucket = _bucket("http://oss-cn-hangzhou.aliyuncs.com")
    bucket.head_object.return_value = _metadata()
    with pytest.raises(OSError, match="invalid target"):
        create_presigned_url(
            bucket,
            "oss://bucket/table",
            _descriptor(),
            timedelta(minutes=5),
        )


def test_failed_materialization_aborts_upload_and_wraps_error():
    bucket = _bucket()
    bucket.head_object.side_effect = [
        _missing(), SimpleNamespace(content_length=100)]
    bucket.init_multipart_upload.return_value = SimpleNamespace(
        upload_id="upload-id")
    bucket.upload_part_copy.side_effect = RuntimeError("copy failed")

    with pytest.raises(OSError, match="Failed to create") as caught:
        create_presigned_url(
            bucket,
            "oss://bucket/table",
            _descriptor(),
            timedelta(minutes=5),
        )

    assert isinstance(caught.value.__cause__, RuntimeError)
    bucket.abort_multipart_upload.assert_called_once_with(
        _TARGET_KEY, "upload-id")


def test_file_io_wrappers_delegate_presigning():
    descriptor = _descriptor()
    validity = timedelta(minutes=5)
    delegate = mock.MagicMock(spec=FileIO)
    delegate.create_blob_presigned_url.return_value = "https://example"

    caching = CachingFileIO(delegate, None)
    assert caching.create_blob_presigned_url(
        "oss://bucket/table", descriptor, validity) == "https://example"

    resolving = ResolvingFileIO(Options({}))
    with mock.patch.object(
            resolving, '_get_fileio', return_value=delegate) as get_file_io:
        assert resolving.create_blob_presigned_url(
            "oss://bucket/table", descriptor, validity) == "https://example"
        get_file_io.assert_called_once_with(descriptor.uri)

    rest = RESTTokenFileIO.__new__(RESTTokenFileIO)
    with mock.patch.object(rest, 'file_io', return_value=delegate):
        assert rest.create_blob_presigned_url(
            "oss://bucket/table", descriptor, validity) == "https://example"
