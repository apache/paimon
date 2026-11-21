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

"""OSS-specific operations, reusing the existing Arrow/Jindo FileIO."""

import logging
import os
import re
from contextlib import closing
from urllib.parse import urlparse

from pypaimon.common.options.config import OssOptions
from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO


_LOGGER = logging.getLogger(__name__)


class OssFileIO(PyArrowFileIO):
    """Override atomic metadata creation; inherit all ordinary file operations."""

    # Intentionally no lock: stable bucket configuration makes duplicate first
    # queries acceptable. Without a lock, no additional __getstate__/__setstate__
    # hooks are needed; the parent's pickle support already handles this None/bool
    # cache, preserving its value under the same stability assumption.
    _atomic_write_supported = None

    def try_to_write_atomic(self, path: str, content: str) -> bool:
        uri = urlparse(path)
        if uri.scheme:
            if uri.scheme != 'oss' or self._extract_oss_bucket(path) != self._oss_bucket:
                raise ValueError("Atomic write must target the configured OSS bucket")
            key = self._extract_oss_object_key(path)
            path = 'oss://{}/{}'.format(self._oss_bucket, key)
        else:
            key = path
            if not self._use_jindo and not self._oss_bucket_in_endpoint:
                bucket, _, key = path.partition('/')
                if bucket != self._oss_bucket:
                    raise ValueError("Atomic write must target the configured OSS bucket")
        if not key or key == '.' or key.endswith('/'):
            return False

        try:
            import oss2
        except ImportError as error:
            raise ImportError(
                "OSS atomic writes require oss2. Install pypaimon[oss] or pypaimon[jindo]."
            ) from error

        session = oss2.Session()
        try:
            bucket = self._create_oss_bucket(session)
            headers = self._sse_headers()
            headers['x-oss-forbid-overwrite'] = 'true'
            if not self._supports_atomic_write(bucket):
                return super().try_to_write_atomic(path, content)
            try:
                bucket.put_object(key, content.encode('utf-8'), headers=headers)
                return True
            except oss2.exceptions.ServerError as error:
                if error.code == 'FileAlreadyExists':
                    return False
                raise
        except oss2.exceptions.OssError as error:
            raise OSError("Failed to atomically write oss://{}/{}".format(self._oss_bucket, key)) from error
        finally:
            session.session.close()

    def try_to_write_exclusive(self, path: str, content: str) -> bool:
        """Create an OSS object only if absent; never use the rename fallback."""
        key = self._exclusive_object_key(path)

        try:
            import oss2
        except ImportError as error:
            raise ImportError(
                "OSS exclusive writes require oss2. Install pypaimon[oss] "
                "or pypaimon[jindo].") from error

        session = oss2.Session()
        try:
            bucket = self._create_oss_bucket(session)
            self._check_exclusive_bucket(bucket)
            headers = self._sse_headers()
            headers['x-oss-forbid-overwrite'] = 'true'
            payload = content.encode('utf-8')
            try:
                bucket.put_object(key, payload, headers=headers)
                return True
            except oss2.exceptions.ServerError as error:
                if error.code == 'FileAlreadyExists':
                    return False
                raise
            except oss2.exceptions.RequestError as error:
                # The server may have created the object before the response
                # was lost. The caller supplies a unique claim ID as content.
                try:
                    with closing(bucket.get_object(key)) as result:
                        recorded = result.read()
                except oss2.exceptions.OssError:
                    raise error
                return recorded == payload
        except oss2.exceptions.OssError as error:
            raise OSError("Failed to exclusively write {}".format(path)) from error
        finally:
            session.session.close()

    def upload_file_exclusive(self, source: str, path: str) -> bool:
        """Upload a local file with an OSS no-overwrite condition."""
        key = self._exclusive_object_key(path)
        try:
            import oss2
        except ImportError as error:
            raise ImportError(
                "OSS exclusive uploads require oss2. Install pypaimon[oss] "
                "or pypaimon[jindo].") from error

        session = oss2.Session()
        try:
            bucket = self._create_oss_bucket(session)
            self._check_exclusive_bucket(bucket)
            headers = self._sse_headers()
            headers['x-oss-forbid-overwrite'] = 'true'
            try:
                if os.path.getsize(source) <= 5 * 1024 ** 3:
                    bucket.put_object_from_file(key, source, headers=headers)
                else:
                    self._upload_multipart_exclusive(bucket, key, source, headers)
                return True
            except oss2.exceptions.ServerError as error:
                if error.code == 'FileAlreadyExists':
                    return False
                raise
        except oss2.exceptions.OssError as error:
            raise OSError("Failed to exclusively upload {}".format(path)) from error
        finally:
            session.session.close()

    @staticmethod
    def _upload_multipart_exclusive(bucket, key, source, headers):
        import oss2

        size = os.path.getsize(source)
        part_size = max(64 * 1024 ** 2, (size + 9999) // 10000)
        upload_id = None
        try:
            upload_id = bucket.init_multipart_upload(
                key, headers={name: value for name, value in headers.items()
                              if name != 'x-oss-forbid-overwrite'}).upload_id
            parts = []
            with open(source, 'rb') as stream:
                for number in range(1, (size + part_size - 1) // part_size + 1):
                    result = bucket.upload_part(
                        key, upload_id, number, stream.read(part_size))
                    parts.append(oss2.models.PartInfo(number, result.etag))
            bucket.complete_multipart_upload(
                key, upload_id, parts,
                headers={'x-oss-forbid-overwrite': 'true'})
            upload_id = None
        finally:
            if upload_id is not None:
                try:
                    bucket.abort_multipart_upload(key, upload_id)
                except oss2.exceptions.OssError:
                    _LOGGER.warning(
                        "Failed to abort exclusive OSS upload %s", key,
                        exc_info=True)

    def check_exclusive_write_supported(self, path: str):
        """Reject unsupported OSS buckets before preparing an export."""
        self._exclusive_object_key(path)
        try:
            import oss2
        except ImportError as error:
            raise ImportError(
                "OSS exclusive writes require oss2. Install pypaimon[oss] "
                "or pypaimon[jindo].") from error
        session = oss2.Session()
        try:
            self._check_exclusive_bucket(self._create_oss_bucket(session))
        except oss2.exceptions.OssError as error:
            raise OSError("Failed to check exclusive OSS write support") from error
        finally:
            session.session.close()

    def _exclusive_object_key(self, path):
        uri = urlparse(path)
        if uri.scheme != 'oss' or self._extract_oss_bucket(path) != self._oss_bucket:
            raise ValueError("Exclusive write must target the configured OSS bucket")
        key = self._extract_oss_object_key(path)
        if not key or key == '.' or key.endswith('/'):
            raise ValueError("Exclusive write requires an OSS object path")
        return key

    @staticmethod
    def _check_exclusive_bucket(bucket):
        import oss2

        # Check afresh: the cached metadata-write capability may predate a
        # bucket versioning change. Unknown status must fail closed.
        try:
            versioning = bucket.get_bucket_versioning().status
        except oss2.exceptions.ServerError as error:
            if error.status == 403 and error.code == 'AccessDenied':
                raise NotImplementedError(
                    "OSS exclusive writes require permission to query "
                    "bucket versioning.") from error
            raise
        if versioning is not None:
            raise NotImplementedError(
                "OSS exclusive writes require bucket versioning to be disabled.")

    def create_blob_presigned_url(self, table_root, descriptor, validity) -> str:
        try:
            import oss2
        except ImportError as error:
            raise ImportError(
                "OSS BLOB presigning requires oss2. Install pypaimon[oss] "
                "or pypaimon[jindo]."
            ) from error

        from pypaimon.filesystem.oss_blob_presigner import create_presigned_url

        session = oss2.Session()
        try:
            bucket = self._create_oss_bucket(
                session, self._extract_oss_bucket(descriptor.uri))
            return create_presigned_url(
                bucket,
                table_root,
                descriptor,
                validity,
                sse_headers=self._sse_headers(),
                has_security_token=bool(
                    self.properties.get(OssOptions.OSS_SECURITY_TOKEN)),
            )
        finally:
            session.session.close()

    def _supports_atomic_write(self, bucket):
        """Cache the publication mode for this instance; failed queries remain retryable."""
        import oss2

        # Intentionally lock-free: duplicate first queries are acceptable with stable bucket configuration.
        if self._atomic_write_supported is None:
            try:
                versioning = bucket.get_bucket_versioning().status
            except oss2.exceptions.ServerError as error:
                if error.status != 403 or error.code != 'AccessDenied':
                    raise
                versioning = 'unknown (GetBucketVersioning denied)'
            self._atomic_write_supported = versioning is None
            if not self._atomic_write_supported:
                self.logger.warning(
                    "Using legacy temporary-file-and-rename writes for OSS bucket %s "
                    "(versioning: %s). Concurrent commits are not protected against overwrites.",
                    self._oss_bucket, versioning)
        return self._atomic_write_supported

    @staticmethod
    def _extract_oss_object_key(location) -> str:
        """Return an OSS object key for standard and credential URI forms."""
        uri = urlparse(location)
        if uri.scheme and uri.scheme.lower() != 'oss':
            raise ValueError("Not an OSS URI: {}".format(location))
        key = re.sub(r'/+', '/', uri.path).lstrip('/')
        netloc = uri.netloc or ''
        if ((getattr(uri, 'username', None)
             or getattr(uri, 'password', None))
                or '@' in netloc):
            # Legacy URI: oss://AK:SK@endpoint/bucket/object-key
            key = key.partition('/')[2]
        return key

    def _create_oss_bucket(self, session, bucket_name=None):
        """Build the metadata client with one V4 credential path for both AK and STS."""
        import oss2
        from oss2.credentials import StaticCredentialsProvider

        access_key = self.properties.get(OssOptions.OSS_ACCESS_KEY_ID)
        secret_key = self.properties.get(OssOptions.OSS_ACCESS_KEY_SECRET)
        token = self.properties.get(OssOptions.OSS_SECURITY_TOKEN)
        endpoint = self.properties.get(OssOptions.OSS_ENDPOINT)
        if not access_key or not secret_key or not endpoint:
            raise ValueError(
                "OSS atomic writes require fs.oss.accessKeyId, fs.oss.accessKeySecret "
                "and fs.oss.endpoint; pass fs.oss.securityToken for STS credentials.")
        if '://' not in endpoint:
            endpoint = 'https://' + endpoint
        region = (self.properties.get(OssOptions.OSS_REGION) or '').strip()
        if not region:
            match = re.fullmatch(
                r'oss-(?!accelerate(?:[.-]))([a-z0-9-]+?)(?:-internal)?\.aliyuncs\.com',
                urlparse(endpoint).hostname or '')
            region = match.group(1) if match else None
        if not region:
            raise ValueError("Set fs.oss.region for OSS V4 signing when the endpoint is not regional")
        provider = StaticCredentialsProvider(access_key, secret_key, token)
        auth = oss2.ProviderAuthV4(provider)
        return oss2.Bucket(
            auth,
            endpoint,
            bucket_name or self._oss_bucket,
            session=session,
            region=region,
        )

    def _sse_headers(self):
        """Match Java OSSFileIO's SSE resolution, including the native option fallback."""
        settings = (OssOptions.OSS_SSE_METHOD, OssOptions.OSS_SSE_KMS_KEY_ID,
                    OssOptions.OSS_SSE_DATA_ENCRYPTION)
        values = [self.properties.get(setting) for setting in settings]
        for setting, value in zip(settings, values):
            if value is not None and not value.strip():
                raise ValueError("'{}' is set but blank".format(setting.key()))
        method, key_id, data_encryption = [value.strip() if value is not None else None for value in values]
        if method is None and key_id is None and data_encryption is None:
            # Java forwards a nonempty native value as-is; the OSS service validates it.
            algorithm = self.properties.get(OssOptions.OSS_SSE_ALGORITHM)
            return {'x-oss-server-side-encryption': algorithm} if algorithm else {}

        method = (method or 'KMS').upper()
        if method not in ('AES256', 'KMS', 'SM4'):
            raise ValueError("'{}' must be one of AES256/KMS/SM4".format(OssOptions.OSS_SSE_METHOD.key()))
        headers = {'x-oss-server-side-encryption': method}
        if key_id is not None:
            if any(character.isspace() for character in key_id):
                raise ValueError("'{}' must not contain whitespace".format(OssOptions.OSS_SSE_KMS_KEY_ID.key()))
            if method != 'KMS':
                raise ValueError("'{}' requires KMS".format(OssOptions.OSS_SSE_KMS_KEY_ID.key()))
            headers['x-oss-server-side-encryption-key-id'] = key_id
        if data_encryption is not None:
            if method != 'KMS':
                raise ValueError("'{}' requires KMS".format(OssOptions.OSS_SSE_DATA_ENCRYPTION.key()))
            if data_encryption.upper() != 'SM4':
                raise ValueError("'{}' only supports SM4".format(OssOptions.OSS_SSE_DATA_ENCRYPTION.key()))
            headers['x-oss-server-side-data-encryption'] = 'SM4'
        return headers
