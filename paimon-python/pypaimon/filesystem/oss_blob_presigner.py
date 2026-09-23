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

"""Materialize OSS BLOB ranges and create temporary GET URLs."""

import hashlib
import logging
import posixpath
import re
from datetime import timedelta
from urllib.parse import unquote, urlparse, urlsplit


_LOGGER = logging.getLogger(__name__)

_BLOB_FINGERPRINT_METADATA = "paimon-blob-descriptor-sha256"
_BLOB_FINGERPRINT_HEADER = "x-oss-meta-" + _BLOB_FINGERPRINT_METADATA
_BLOB_CONTENT_TYPE = "application/octet-stream"
_BLOB_COPY_MIN_PART_SIZE = 100 * 1024 * 1024
_MAX_MULTIPART_UPLOAD_PARTS = 10_000


def create_presigned_url(
        bucket, table_root, descriptor, validity, sse_headers=None,
        has_security_token=False) -> str:
    """Create a presigned URL with the same object layout as Java Paimon."""
    endpoint = urlsplit(bucket.endpoint)
    if endpoint.scheme.lower() != "https" or not endpoint.hostname:
        raise ValueError("OSS BLOB presigning requires an HTTPS endpoint.")
    validity_seconds = _validity_seconds(validity, has_security_token)
    source = _validate_table_root(table_root, descriptor)
    # Keep the compatibility URI rules in OssFileIO, which also owns bucket
    # extraction for oss://AK:SK@endpoint/bucket/object-key URIs.
    from pypaimon.filesystem.oss_file_io import OssFileIO
    source_key = OssFileIO._extract_oss_object_key(source.geturl())
    if not source_key:
        raise ValueError("Blob descriptor URI must contain an OSS object key.")
    sse_headers = dict(sse_headers or {})
    fingerprint = hashlib.sha256(descriptor.serialize()).hexdigest()
    parent_end = source_key.rfind('/') + 1
    target_key = source_key[:parent_end] + "_bloburl_" + fingerprint

    try:
        target = _head_object_if_exists(bucket, target_key)
        if not _matches(
                target, descriptor.length, fingerprint, sse_headers):
            source_metadata = bucket.head_object(source_key)
            _validate_range(descriptor, source_metadata.content_length)
            _materialize(
                bucket,
                source_key,
                target_key,
                descriptor,
                fingerprint,
                sse_headers,
            )
            target = bucket.head_object(target_key)
            if not _matches(
                    target, descriptor.length, fingerprint, sse_headers):
                raise OSError(
                    "Materialized blob object metadata does not match "
                    "descriptor.")

        url = bucket.sign_url(
            'GET', target_key, validity_seconds, slash_safe=True)
        _validate_presigned_url(bucket, url, target_key)
        return url
    except (OSError, ValueError):
        raise
    except Exception as error:
        raise OSError("Failed to create blob presigned URL.") from error


def _validity_seconds(validity, has_security_token=False) -> int:
    if not isinstance(validity, timedelta):
        raise TypeError(
            "Blob presigned URL validity must be datetime.timedelta.")
    if validity <= timedelta(0) or validity.microseconds != 0:
        raise ValueError(
            "Blob presigned URL validity must be positive whole seconds.")
    seconds = validity.days * 24 * 60 * 60 + validity.seconds
    maximum = 43200 if has_security_token else 604800
    if seconds > maximum:
        raise ValueError(
            "OSS V4 presigned URL validity must not exceed {} seconds."
            .format(maximum))
    return seconds


def _validate_table_root(table_root, descriptor):
    root = urlparse(str(table_root))
    blob = urlparse(descriptor.uri)
    root_path = _normalize_path(root.path)
    blob_path = _normalize_path(blob.path)
    if ((root.scheme or '').lower() != (blob.scheme or '').lower()
            or root.netloc != blob.netloc):
        raise ValueError(
            "Blob descriptor URI must use the same scheme and authority "
            "as table root.")
    child_prefix = root_path if root_path.endswith('/') else root_path + '/'
    if not blob_path.startswith(child_prefix):
        raise ValueError("Blob descriptor URI must be under table root.")
    return blob._replace(path=blob_path)


def _normalize_path(path):
    normalized = posixpath.normpath(re.sub('/+', '/', path or '/'))
    if path.startswith('/') and not normalized.startswith('/'):
        normalized = '/' + normalized
    return normalized


def _head_object_if_exists(bucket, key):
    try:
        return bucket.head_object(key)
    except Exception as error:
        if getattr(error, 'code', None) in ('NoSuchKey', 'NoSuchObject'):
            return None
        raise


def _matches(metadata, length, fingerprint, sse_headers) -> bool:
    if metadata is None:
        return False
    headers = getattr(metadata, 'headers', {}) or {}
    metadata_fingerprint = next(
        (value for key, value in headers.items()
         if key.lower() == _BLOB_FINGERPRINT_HEADER),
        None,
    )
    return (
        metadata.content_length == length
        and metadata.content_type == _BLOB_CONTENT_TYPE
        and metadata_fingerprint == fingerprint
        and _matches_sse_headers(headers, sse_headers)
    )


def _matches_sse_headers(actual_headers, expected_headers) -> bool:
    if not expected_headers:
        return True
    actual = {
        str(key).lower(): value
        for key, value in actual_headers.items()
    }
    case_insensitive_values = {
        'x-oss-server-side-encryption',
        'x-oss-server-side-data-encryption',
    }
    for key, expected in expected_headers.items():
        normalized_key = str(key).lower()
        found = actual.get(normalized_key)
        if found is None:
            return False
        if normalized_key in case_insensitive_values:
            if str(found).upper() != str(expected).upper():
                return False
        elif found != expected:
            return False
    return True


def _validate_range(descriptor, source_length):
    if (descriptor.offset < 0
            or descriptor.length < 0
            or descriptor.offset > source_length
            or descriptor.length > source_length - descriptor.offset):
        raise ValueError("Blob descriptor range is outside the source object.")


def _materialize(
        bucket,
        source_key,
        target_key,
        descriptor,
        fingerprint,
        sse_headers):
    import oss2

    headers = dict(sse_headers)
    headers.update({
        'Content-Type': _BLOB_CONTENT_TYPE,
        _BLOB_FINGERPRINT_HEADER: fingerprint,
    })
    if descriptor.length == 0:
        bucket.put_object(target_key, b'', headers=headers)
        return

    upload_id = None
    try:
        initiated = bucket.init_multipart_upload(target_key, headers=headers)
        upload_id = initiated.upload_id
        part_size = max(
            _BLOB_COPY_MIN_PART_SIZE,
            descriptor.length // _MAX_MULTIPART_UPLOAD_PARTS + 1,
        )
        parts = []
        copied = 0
        part_number = 1
        while copied < descriptor.length:
            size = min(part_size, descriptor.length - copied)
            result = bucket.upload_part_copy(
                bucket.bucket_name,
                source_key,
                (descriptor.offset + copied,
                 descriptor.offset + copied + size - 1),
                target_key,
                upload_id,
                part_number,
            )
            parts.append(oss2.models.PartInfo(part_number, result.etag))
            copied += size
            part_number += 1
        bucket.complete_multipart_upload(target_key, upload_id, parts)
        upload_id = None
    except Exception:
        if upload_id is not None:
            try:
                bucket.abort_multipart_upload(target_key, upload_id)
            except Exception:
                _LOGGER.warning(
                    "Failed to abort OSS BLOB multipart upload %s for %s",
                    upload_id, target_key, exc_info=True)
        raise


def _validate_presigned_url(bucket, url, target_key):
    endpoint = urlsplit(bucket.endpoint)
    actual = urlsplit(url)
    expected_host = bucket.bucket_name + '.' + (endpoint.hostname or '')
    if (endpoint.scheme.lower() != 'https'
            or actual.scheme.lower() != 'https'
            or (actual.hostname or '').lower() != expected_host.lower()
            or unquote(actual.path) != '/' + target_key):
        raise OSError(
            "OSS client generated a presigned URL for an invalid target.")
