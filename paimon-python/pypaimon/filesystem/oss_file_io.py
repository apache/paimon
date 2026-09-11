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

"""OSS conditional metadata creation, reusing the existing Arrow/Jindo FileIO."""

import re
from urllib.parse import urlparse

from pypaimon.common.options.config import OssOptions
from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO


class OssFileIO(PyArrowFileIO):
    """Override atomic metadata creation; inherit all ordinary file operations."""

    def try_to_write_atomic(self, path: str, content: str) -> bool:
        uri = urlparse(path)
        if uri.scheme:
            if uri.scheme != 'oss' or self._extract_oss_bucket(path) != self._oss_bucket:
                raise ValueError("Atomic write must target the configured OSS bucket")
            key = re.sub(r'/+', '/', uri.path).lstrip('/')
            if '@' in uri.netloc:
                key = key.partition('/')[2]
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
        headers = self._sse_headers()
        headers['x-oss-forbid-overwrite'] = 'true'
        auth = (oss2.StsAuth(access_key, secret_key, token, auth_version='v4')
                if token else oss2.AuthV4(access_key, secret_key))

        session = oss2.Session()
        try:
            bucket = oss2.Bucket(auth, endpoint, self._oss_bucket, session=session, region=region)
            try:
                versioning = bucket.get_bucket_versioning().status
            except oss2.exceptions.ServerError as error:
                if error.status != 403 or error.code != 'AccessDenied':
                    raise
                versioning = 'unknown (GetBucketVersioning denied)'
            if versioning is not None:
                self.logger.warning(
                    "Using legacy temporary-file-and-rename writes for OSS bucket %s "
                    "(versioning: %s). Concurrent commits are not protected against overwrites.",
                    self._oss_bucket, versioning)
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
