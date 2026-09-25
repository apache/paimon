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

import base64
import hashlib
import json
import logging
import os
import re
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timezone
from itertools import islice
from pathlib import PurePosixPath
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import splitport, urlparse

import pyarrow
import pyarrow.fs as pafs
from packaging.version import parse
from pyarrow._fs import FileSystem

from pypaimon.common.file_io import FileIO, create_temp_path
from pypaimon.common.options import Options
from pypaimon.common.options.config import OssOptions, S3Options, SecurityOptions
from pypaimon.common.options.options_utils import OptionsUtils
from pypaimon.common.uri_reader import UriReaderFactory
from pypaimon.filesystem.jindo_file_system_handler import JindoFileSystemHandler, JINDO_AVAILABLE
from pypaimon.schema.data_types import (AtomicType, DataField,
                                        PyarrowFieldParser)
from pypaimon.write.blob_format_writer import BlobFormatWriter


def _pyarrow_lt_7():
    return parse(pyarrow.__version__) < parse("7.0.0")


_S3_CHECKSUM_LOCK = threading.Lock()
_S3_CHECKSUM_ENV = "AWS_REQUEST_CHECKSUM_CALCULATION"
_S3_DELETE_TIMEOUT_SECONDS = 3600


class LegacyOssDirectoryListingError(RuntimeError):
    """Raised when legacy PyArrow OSS cannot enumerate a directory."""


class PyArrowFileIO(FileIO):
    def __init__(self, path: str, catalog_options: Options):
        self.properties = catalog_options
        self.logger = logging.getLogger(__name__)
        self._set_pyarrow_version()
        scheme, netloc, _ = self.parse_location(path)
        self.uri_reader_factory = UriReaderFactory(catalog_options)
        self._is_oss = scheme in {"oss"}
        self._is_s3 = scheme in {"s3", "s3a", "s3n"}
        self._s3_endpoint = (
            self._get_s3_property("endpoint", S3Options.S3_ENDPOINT.key())
            if self._is_s3 else None
        )
        self._oss_bucket = None
        _oss_impl = self.properties.get(OssOptions.OSS_IMPL)
        self._use_jindo = False
        self._legacy_bucket_checked = False
        self._legacy_bucket_error = None
        self._legacy_bucket_lock = threading.Lock()
        self._s3_delete_client = None

        if self._is_oss:
            self._oss_bucket = self._extract_oss_bucket(path)
            if _oss_impl not in ("jindo", "legacy"):
                raise ValueError(
                    f"Unsupported fs.oss.impl value: '{_oss_impl}'. "
                    f"Supported values are 'jindo' and 'legacy'.")
            if _oss_impl == "legacy":
                self.filesystem = self._initialize_oss_fs(path)
            elif JINDO_AVAILABLE:
                self.filesystem = self._initialize_jindo_fs(path)
            else:
                self.logger.info(
                    "fs.oss.impl is 'jindo' but pyjindosdk is not installed. "
                    "Falling back to legacy PyArrow S3FileSystem implementation. "
                    "Install pyjindosdk for better performance: pip install pyjindosdk")
                self.filesystem = self._initialize_oss_fs(path)
        elif self._is_s3:
            self.filesystem = self._initialize_s3_fs()
        elif scheme in {"hdfs", "viewfs"}:
            self.filesystem = self._initialize_hdfs_fs(scheme, netloc)
        elif scheme == "gs":
            self.filesystem = self._initialize_gcs_fs()
        else:
            raise ValueError(f"Unrecognized filesystem type in URI: {scheme}")

    def _set_pyarrow_version(self):
        self._pyarrow_gte_8 = parse(pyarrow.__version__) >= parse("8.0.0")
        # force_virtual_addressing landed in PyArrow 16; below it the OSS bucket
        # goes into endpoint_override, so keys must omit it (init + path share
        # this flag so they can't drift).
        self._pyarrow_gte_16 = parse(pyarrow.__version__) >= parse("16.0.0")
        self._pyarrow_gte_22 = parse(pyarrow.__version__) >= parse("22.0.0")
        self._oss_bucket_in_endpoint = not self._pyarrow_gte_16

    def __getstate__(self):
        state = self.__dict__.copy()
        # threading.Lock cannot be pickled; recreated in __setstate__.
        state.pop("_legacy_bucket_lock", None)
        state.pop("logger", None)
        state.pop("_s3_delete_client", None)
        # Recreate S3-compatible clients with the worker's AWS SDK settings.
        if self._uses_s3_compatibility():
            state.pop("filesystem", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = logging.getLogger(__name__)
        self._set_pyarrow_version()
        if "_is_s3" not in state:
            self._is_s3 = (not self._is_oss
                           and isinstance(self.filesystem, pafs.S3FileSystem))
        if "_s3_endpoint" not in state:
            self._s3_endpoint = (
                self._get_s3_property("endpoint", S3Options.S3_ENDPOINT.key())
                if self._is_s3 else None)
        self._legacy_bucket_lock = threading.Lock()
        self._s3_delete_client = None
        if self._uses_s3_compatibility():
            self.filesystem = (
                self._initialize_oss_fs(None)
                if self._is_oss else self._initialize_s3_fs()
            )

    @staticmethod
    def parse_location(location: str):
        uri = urlparse(location)
        if not uri.scheme:
            return "file", uri.netloc, os.path.abspath(location)
        elif uri.scheme in ("hdfs", "viewfs"):
            return uri.scheme, uri.netloc, uri.path
        else:
            return uri.scheme, uri.netloc, f"{uri.netloc}{uri.path}"

    def _create_s3_retry_config(
            self,
            max_attempts: int = 10,
            request_timeout: int = 60,
            connect_timeout: int = 60
    ) -> Dict[str, Any]:
        if self._pyarrow_gte_8:
            config = {
                'request_timeout': request_timeout,
                'connect_timeout': connect_timeout
            }
            try:
                retry_strategy = pafs.AwsStandardS3RetryStrategy(max_attempts=max_attempts)
                config['retry_strategy'] = retry_strategy
            except ImportError:
                pass
            return config
        else:
            return {}

    def _get_property(self, *keys: str):
        data = self.properties.to_map()
        for key in keys:
            if key in data:
                return data[key]
        return None

    @staticmethod
    def _s3_key_variants(*names: str):
        prefixes = ["s3.", "s3a.", "fs.s3.", "fs.s3a."]
        for prefix in prefixes:
            for name in names:
                yield prefix + name

    def _get_s3_property(self, name: str, legacy_key: str = None):
        keys = []
        if legacy_key:
            keys.append(legacy_key)
        keys.extend(self._s3_key_variants(name))
        return self._get_property(*keys)

    def _get_s3_boolean_property(self, name: str) -> bool:
        value = self._get_s3_property(name)
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        return OptionsUtils.convert_to_boolean(value)

    def _uses_s3_compatibility(self) -> bool:
        return (not self._use_jindo
                and (self._is_oss or bool(self._s3_endpoint)))

    def _uses_s3_delete_fallback(self) -> bool:
        return (self._uses_s3_compatibility()
                and self._pyarrow_gte_22
                and not (self._is_s3 and self._get_s3_boolean_property(
                    "delete.batch-enabled")))

    @staticmethod
    def _create_s3_filesystem(client_kwargs, compatible: bool) -> FileSystem:
        with _S3_CHECKSUM_LOCK:
            if not compatible:
                return pafs.S3FileSystem(**client_kwargs)
            # PyArrow has no per-client checksum option; AWS reads this at construction.
            previous = os.environ.get(_S3_CHECKSUM_ENV)
            os.environ[_S3_CHECKSUM_ENV] = "WHEN_REQUIRED"
            try:
                return pafs.S3FileSystem(**client_kwargs)
            finally:
                if previous is None:
                    os.environ.pop(_S3_CHECKSUM_ENV, None)
                else:
                    os.environ[_S3_CHECKSUM_ENV] = previous

    def _extract_oss_bucket(self, location) -> str:
        uri = urlparse(location)
        if uri.scheme and uri.scheme != "oss":
            raise ValueError("Not an OSS URI: {}".format(location))

        netloc = uri.netloc or ""
        if (getattr(uri, "username", None) or getattr(uri, "password", None)) or ("@" in netloc):
            first_segment = uri.path.lstrip("/").split("/", 1)[0]
            if not first_segment:
                raise ValueError("Invalid OSS URI without bucket: {}".format(location))
            return first_segment

        host = getattr(uri, "hostname", None) or netloc
        if not host:
            raise ValueError("Invalid OSS URI without host: {}".format(location))
        bucket = host.split(".", 1)[0]
        if not bucket:
            raise ValueError("Invalid OSS URI without bucket: {}".format(location))
        return bucket

    def _initialize_jindo_fs(self, path) -> FileSystem:
        """Initialize JindoFileSystem for OSS access."""
        self.logger.info(f"Initializing JindoFileSystem for OSS access: {path}")
        root_path = f"oss://{self._oss_bucket}/"
        fs_handler = JindoFileSystemHandler(root_path, self.properties)
        self._use_jindo = True
        return pafs.PyFileSystem(fs_handler)

    def _initialize_oss_fs(self, path) -> FileSystem:
        if self.properties.get(OssOptions.OSS_ACCESS_KEY_ID):
            # When explicit credentials are provided, disable the EC2 Instance Metadata
            # Service (IMDS) probe to avoid multi-second timeouts in non-AWS environments.
            # Uses setdefault so that an explicit user setting is never overridden.
            # Note: this is process-wide and affects all AWS SDK clients.
            os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

        client_kwargs = {
            "access_key": self.properties.get(OssOptions.OSS_ACCESS_KEY_ID),
            "secret_key": self.properties.get(OssOptions.OSS_ACCESS_KEY_SECRET),
            "session_token": self.properties.get(OssOptions.OSS_SECURITY_TOKEN),
            "region": self.properties.get(OssOptions.OSS_REGION),
        }

        if not self._oss_bucket_in_endpoint:
            client_kwargs['force_virtual_addressing'] = True
            client_kwargs['endpoint_override'] = self.properties.get(OssOptions.OSS_ENDPOINT)
        else:
            client_kwargs['endpoint_override'] = (self._oss_bucket + "." +
                                                  self.properties.get(OssOptions.OSS_ENDPOINT))

        retry_config = self._create_s3_retry_config()
        client_kwargs.update(retry_config)

        return self._create_s3_filesystem(client_kwargs, compatible=True)

    def _initialize_s3_fs(self) -> FileSystem:
        connection = self._s3_connection_options()

        if connection["access_key"]:
            # When explicit credentials are provided, disable the EC2 Instance Metadata
            # Service (IMDS) probe to avoid multi-second timeouts in non-AWS environments.
            # Uses setdefault so that an explicit user setting is never overridden.
            # Note: this is process-wide and affects all AWS SDK clients.
            os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

        client_kwargs = {
            "endpoint_override": self._s3_endpoint,
            "access_key": connection["access_key"],
            "secret_key": connection["secret_key"],
            "session_token": connection["session_token"],
            "region": connection["region"],
        }
        if self._pyarrow_gte_16:
            client_kwargs["force_virtual_addressing"] = not connection["path_style"]

        retry_config = self._create_s3_retry_config()
        client_kwargs.update(retry_config)

        return self._create_s3_filesystem(
            client_kwargs, compatible=bool(self._s3_endpoint))

    def _s3_connection_options(self):
        return {
            "access_key": self._get_property(
                S3Options.S3_ACCESS_KEY_ID.key(),
                *self._s3_key_variants("access-key", "access.key")),
            "secret_key": self._get_property(
                S3Options.S3_ACCESS_KEY_SECRET.key(),
                *self._s3_key_variants("secret-key", "secret.key")),
            "session_token": self._get_property(
                S3Options.S3_SECURITY_TOKEN.key(),
                *self._s3_key_variants(
                    "session-token", "session.token",
                    "security-token", "security.token")),
            "region": self._get_s3_property("region", S3Options.S3_REGION.key()),
            "path_style": (self._pyarrow_gte_16 and (
                self._get_s3_boolean_property("path-style-access") or
                self._get_s3_boolean_property("path.style.access"))),
        }

    def _initialize_hdfs_fs(self, scheme: str, netloc: Optional[str]) -> FileSystem:
        if 'HADOOP_HOME' not in os.environ:
            raise RuntimeError("HADOOP_HOME environment variable is not set.")
        if 'HADOOP_CONF_DIR' not in os.environ:
            raise RuntimeError("HADOOP_CONF_DIR environment variable is not set.")

        hadoop_home = os.environ.get("HADOOP_HOME")
        native_lib_path = f"{hadoop_home}/lib/native"
        os.environ['LD_LIBRARY_PATH'] = f"{native_lib_path}:{os.environ.get('LD_LIBRARY_PATH', '')}"

        class_paths = subprocess.run(
            [f'{hadoop_home}/bin/hadoop', 'classpath', '--glob'],
            capture_output=True,
            text=True,
            check=True
        )
        os.environ['CLASSPATH'] = class_paths.stdout.strip()

        principal = (self.properties.get(SecurityOptions.KERBEROS_PRINCIPAL)
                     or self._get_property("security.principal"))
        keytab = (self.properties.get(SecurityOptions.KERBEROS_KEYTAB)
                  or self._get_property("security.keytab"))
        use_ticket_cache = self.properties.get(SecurityOptions.KERBEROS_USE_TICKET_CACHE)

        if bool(principal) != bool(keytab):
            raise ValueError(
                "security.kerberos.login.principal and security.kerberos.login.keytab "
                "must be both set or both unset")

        # Resolve (host, port) for pafs.HadoopFileSystem.
        # - ViewFS URIs delegate to fs.defaultFS (host='default') so libhdfs
        #   resolves the mount table from core-site.xml.
        # - HDFS HA URIs carry a nameservice without a port; also delegate to
        #   fs.defaultFS to avoid int(None) on the missing port.
        # - Explicit "host:port" URIs connect directly.
        if scheme == 'viewfs' or not netloc:
            host, port = 'default', 0
        else:
            parsed_host, port_str = splitport(netloc)
            if port_str is None:
                host, port = 'default', 0
            else:
                host, port = parsed_host, int(port_str)

        kerb_ticket = None
        if principal and keytab:
            self._kerberos_login_from_keytab(principal, keytab)
            kerb_ticket = self._get_ticket_cache_path()
            if not kerb_ticket:
                raise RuntimeError(
                    "kinit succeeded but no ticket cache path could be determined. "
                    "Set the KRB5CCNAME environment variable to specify the cache location.")
        elif use_ticket_cache:
            cache_path = self._get_ticket_cache_path()
            if cache_path and os.path.exists(cache_path):
                kerb_ticket = cache_path

        if kerb_ticket:
            return pafs.HadoopFileSystem(host=host, port=port, kerb_ticket=kerb_ticket)
        else:
            return pafs.HadoopFileSystem(
                host=host,
                port=port,
                user=os.environ.get('HADOOP_USER_NAME', 'hadoop')
            )

    def _initialize_gcs_fs(self) -> FileSystem:
        if not hasattr(pafs, 'GcsFileSystem'):
            raise ImportError(
                "GCS filesystem support requires PyArrow built with GCS support. "
                "Please upgrade PyArrow or install a version with GCS enabled."
            )

        access_token = self._get_property("gcs.access-token")
        token_expiry = self._get_property("gcs.access-token.expiration")
        project_id = self._get_property("gcs.project-id")

        kwargs = {}
        if access_token:
            from datetime import datetime
            kwargs["access_token"] = access_token
            kwargs["credential_token_expiration"] = (
                datetime.fromisoformat(token_expiry) if token_expiry
                else datetime(9999, 12, 31)
            )
        if project_id:
            kwargs["project_id"] = project_id

        return pafs.GcsFileSystem(**kwargs)

    @staticmethod
    def _kerberos_login_from_keytab(principal: str, keytab: str):
        from pypaimon.filesystem import _kerberos
        _kerberos.kerberos_login_from_keytab(principal, keytab)

    @staticmethod
    def _get_ticket_cache_path() -> Optional[str]:
        from pypaimon.filesystem import _kerberos
        return _kerberos.get_ticket_cache_path()

    def new_input_stream(self, path: str):
        path_str = self.to_filesystem_path(path)
        return self.filesystem.open_input_file(path_str)

    def new_output_stream(self, path: str):
        path_str = self.to_filesystem_path(path)

        if self._use_jindo:
            pass
        elif self._is_oss and self._oss_bucket_in_endpoint:
            # OSS with bucket baked into endpoint: path_str is already the key
            if '/' in path_str:
                parent_dir = '/'.join(path_str.split('/')[:-1])
            else:
                parent_dir = ''

            if parent_dir and not self.exists(parent_dir):
                self.mkdirs(parent_dir)
        else:
            parent_dir = PurePosixPath(path_str).parent
            if str(parent_dir) and not self.exists(str(parent_dir)):
                self.mkdirs(str(parent_dir))

        return self.filesystem.open_output_stream(path_str)

    def _get_file_info(self, path_str: str):
        try:
            file_infos = self.filesystem.get_file_info([path_str])
            return file_infos[0]
        except OSError as e:
            # this is for compatible with pyarrow < 7
            msg = str(e).lower()
            if ("does not exist" in msg or "not exist" in msg or "nosuchkey" in msg
                    or re.search(r'\b133\b', msg) or "notfound" in msg):
                return pafs.FileInfo(path_str, pafs.FileType.NotFound)
            raise

    def get_file_status(self, path: str):
        path_str = self.to_filesystem_path(path)
        file_info = self._get_file_info(path_str)

        if file_info.type == pafs.FileType.NotFound:
            raise FileNotFoundError(f"File {path} (resolved as {path_str}) does not exist")

        return file_info

    def list_status(self, path: str):
        if self._legacy_oss_mode():
            raise LegacyOssDirectoryListingError(
                "Listing OSS directories is not supported with PyArrow < 16 "
                "(it parses the first key segment as a bucket). Upgrade to "
                "pyarrow >= 16, or install pyjindosdk and set fs.oss.impl=jindo.")
        path_str = self.to_filesystem_path(path)
        selector = pafs.FileSelector(path_str, recursive=False, allow_not_found=True)
        return self.filesystem.get_file_info(selector)

    def list_directories(self, path: str):
        file_infos = self.list_status(path)
        return [info for info in file_infos if info.type == pafs.FileType.Directory]

    def _legacy_oss_mode(self) -> bool:
        """OSS in bucket-in-endpoint mode (PyArrow < 16): paths are key-only
        (the bucket is embedded in endpoint_override), but PyArrow still
        parses the first key segment as a bucket, so bucket-level operations
        target the wrong bucket."""
        return self._is_oss and self._oss_bucket_in_endpoint and not self._use_jindo

    def exists(self, path: str) -> bool:
        # Legacy OSS mode limitation: directories always report NotFound
        # (see _legacy_oss_mode); plain objects are probed correctly.
        path_str = self.to_filesystem_path(path)
        return self._get_file_info(path_str).type != pafs.FileType.NotFound

    def exists_batch(self, paths: List[str]) -> Dict[str, bool]:
        """Check existence of multiple paths in a single batched API call."""
        if not paths:
            return {}

        path_strs = [self.to_filesystem_path(p) for p in paths]
        file_infos = self.filesystem.get_file_info(path_strs)
        return {
            paths[i]: info.type != pyarrow.fs.FileType.NotFound
            for i, info in enumerate(file_infos)
        }

    def delete(self, path: str, recursive: bool = False) -> bool:
        path_str = self.to_filesystem_path(path)
        if self._is_oss and (self._use_jindo or self._oss_bucket_in_endpoint):
            bucket_root = path_str.strip("/") in ("", ".")
        elif self._is_s3 or self._is_oss:
            bucket, key = self._split_s3_path(path_str)
            bucket_root = not bucket or not key.strip("/")
        else:
            bucket_root = False
        if bucket_root:
            raise OSError(f"Refusing to delete bucket root: {path}")
        file_info = self._get_file_info(path_str)

        if file_info.type == pafs.FileType.NotFound:
            return False

        if file_info.type == pafs.FileType.Directory:
            if self._uses_s3_delete_fallback():
                if recursive:
                    return self._delete_s3_compatible_directory(path_str)
                selector = pafs.FileSelector(
                    path_str, recursive=False, allow_not_found=True)
                if self.filesystem.get_file_info(selector):
                    raise OSError(f"Directory {path} is not empty")
                bucket, key = self._split_s3_path(path_str)
                if key:
                    client = self._get_s3_delete_client()
                    client.delete_object(Bucket=bucket, Key=key.rstrip("/") + "/")
                    self._ensure_s3_parent_exists(client, bucket, key)
                return True
            if not recursive:
                selector = pafs.FileSelector(path_str, recursive=False, allow_not_found=True)
                dir_contents = self.filesystem.get_file_info(selector)
                if len(dir_contents) > 0:
                    raise OSError(f"Directory {path} is not empty")
            if recursive:
                self.filesystem.delete_dir_contents(path_str)
                self.filesystem.delete_dir(path_str)
            else:
                self.filesystem.delete_dir(path_str)
        else:
            self.filesystem.delete_file(path_str)
        return True

    def _delete_s3_compatible_directory(self, path_str: str) -> bool:
        client = self._get_s3_delete_client()
        bucket, key = self._split_s3_path(path_str)
        prefix = key.rstrip("/")
        if prefix:
            prefix += "/"
        deadline = time.monotonic() + _S3_DELETE_TIMEOUT_SECONDS
        with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as listed, \
                tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as schemas:
            for name in self._list_s3_keys(client, bucket, prefix, deadline, path_str):
                if name == prefix:
                    continue
                target = schemas if "/schema/schema-" in name else listed
                target.write(json.dumps(name) + "\n")

            listed.seek(0)
            self._delete_s3_objects(
                client, bucket, (json.loads(line) for line in listed),
                deadline, path_str)

            known_schemas = self._staged_keys(schemas)
            expected = next(known_schemas, None)
            for name in self._list_s3_keys(client, bucket, prefix, deadline, path_str):
                if name == prefix:
                    continue
                while expected is not None and expected < name:
                    expected = next(known_schemas, None)
                if name != expected:
                    raise OSError(f"S3 directory {path_str} changed during deletion")
                expected = next(known_schemas, None)

            self._check_s3_delete_deadline(deadline, path_str)
            self._ensure_s3_parent_exists(client, bucket, key)
            if prefix:
                self._check_s3_delete_deadline(deadline, path_str)
                client.delete_object(Bucket=bucket, Key=prefix)
            self._delete_s3_objects(
                client, bucket, self._staged_schema_keys(schemas, zero=False),
                deadline, path_str)
            self._delete_s3_objects(
                client, bucket, self._staged_schema_keys(schemas, zero=True),
                deadline, path_str)
        return True

    @staticmethod
    def _staged_keys(staged):
        staged.seek(0)
        for line in staged:
            yield json.loads(line)

    @staticmethod
    def _staged_schema_keys(staged, zero: bool):
        for name in PyArrowFileIO._staged_keys(staged):
            if name.endswith("/schema/schema-0") == zero:
                yield name

    def _list_s3_keys(self, client, bucket: str, prefix: str,
                      deadline: float, path_str: str):
        token = None
        while True:
            self._check_s3_delete_deadline(deadline, path_str)
            params = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token is not None:
                params["ContinuationToken"] = token
            response = client.list_objects_v2(**params)
            yield from self._listed_s3_keys(response, bucket, prefix)
            if not response.get("IsTruncated"):
                return
            next_token = response.get("NextContinuationToken")
            if not next_token or next_token == token:
                raise OSError("S3 listing did not advance")
            token = next_token

    @staticmethod
    def _listed_s3_keys(response, bucket: str, prefix: str):
        if (response.get("Name", bucket) != bucket
                or response.get("Prefix", prefix) != prefix):
            raise OSError("S3 listing returned a different bucket or prefix")
        keys = [item["Key"] for item in response.get("Contents", ())]
        if any(not key.startswith(prefix) for key in keys):
            raise OSError(f"S3 listing returned a key outside prefix {prefix}")
        return keys

    @staticmethod
    def _ensure_s3_parent_exists(client, bucket: str, key: str):
        parent, _, _ = key.rstrip("/").rpartition("/")
        if parent:
            client.put_object(
                Bucket=bucket, Key=parent + "/", Body=b"",
                ContentType="application/x-directory")

    @staticmethod
    def _check_s3_delete_deadline(deadline: float, path_str: str):
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out deleting S3 directory {path_str}")

    @staticmethod
    def _delete_s3_objects(
            client, bucket: str, keys: Iterable[str],
            deadline: float, path_str: str):
        keys = iter(keys)
        while True:
            batch = list(islice(keys, 1000))
            if not batch:
                return
            PyArrowFileIO._check_s3_delete_deadline(deadline, path_str)
            ordinary = []
            for key in batch:
                if key.isprintable():
                    ordinary.append(key)
                else:
                    # Non-printable keys may not round-trip through DeleteObjects XML.
                    PyArrowFileIO._check_s3_delete_deadline(deadline, path_str)
                    client.delete_object(Bucket=bucket, Key=key)
            if not ordinary:
                continue
            PyArrowFileIO._check_s3_delete_deadline(deadline, path_str)
            response = client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": key} for key in ordinary],
                        "Quiet": False})
            deleted = [item["Key"] for item in response.get("Deleted", ())]
            errors = response.get("Errors", ())
            if errors:
                examples = ", ".join(
                    "{}: {!r}".format(item.get("Code"), item.get("Key"))
                    for item in errors[:3])
                raise OSError(
                    f"S3 batch delete incomplete for {path_str}: {examples}")
            if set(deleted) != set(ordinary) or len(deleted) != len(ordinary):
                raise OSError(f"S3 batch delete incomplete for {path_str}")

    @staticmethod
    def _split_s3_path(path_str: str):
        bucket, _, key = path_str.partition("/")
        return bucket, key

    def _get_s3_delete_client(self):
        if self._s3_delete_client is not None:
            return self._s3_delete_client

        import boto3
        from botocore.config import Config

        if self._is_oss:
            endpoint = self.properties.get(OssOptions.OSS_ENDPOINT)
            access_key = self.properties.get(OssOptions.OSS_ACCESS_KEY_ID)
            secret_key = self.properties.get(OssOptions.OSS_ACCESS_KEY_SECRET)
            session_token = self.properties.get(OssOptions.OSS_SECURITY_TOKEN)
            region = self.properties.get(OssOptions.OSS_REGION)
            addressing_style = "virtual"
        else:
            endpoint = self._s3_endpoint
            connection = self._s3_connection_options()
            access_key = connection["access_key"]
            secret_key = connection["secret_key"]
            session_token = connection["session_token"]
            region = connection["region"]
            addressing_style = "path" if connection["path_style"] else "virtual"

        region = region or self.filesystem.region
        if endpoint and "://" not in endpoint:
            endpoint = "https://" + endpoint
        config_args = {
            "retries": {"max_attempts": 10, "mode": "standard"},
            "s3": {"addressing_style": addressing_style},
            "connect_timeout": 60,
            "read_timeout": 60,
        }
        try:
            config = Config(request_checksum_calculation="when_required",
                            **config_args)
            uses_new_checksums = True
        except TypeError:
            config = Config(**config_args)
            uses_new_checksums = False

        self._s3_delete_client = boto3.session.Session().client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region,
            config=config,
        )
        if uses_new_checksums:
            # OSS requires Content-MD5; newer Botocore defaults to CRC32.
            def use_content_md5(request, **kwargs):
                request.headers["Content-MD5"] = base64.b64encode(
                    hashlib.md5(request.body).digest()).decode("ascii")
                for name in list(request.headers):
                    if (name.lower().startswith("x-amz-checksum-") or
                            name.lower() == "x-amz-sdk-checksum-algorithm"):
                        del request.headers[name]

            self._s3_delete_client.meta.events.register(
                "before-sign.s3.DeleteObjects", use_content_md5)
        return self._s3_delete_client

    def mkdirs(self, path: str) -> bool:
        path_str = self.to_filesystem_path(path)
        file_info = self._get_file_info(path_str)

        if file_info.type == pafs.FileType.Directory:
            return True
        if file_info.type == pafs.FileType.File:
            raise FileExistsError(f"Path exists but is not a directory: {path}")

        if self._legacy_oss_mode():
            # create_dir would CreateBucket the first key segment and corrupt
            # the parent directory; object stores need no directories. Only
            # validate that the real bucket exists.
            self._check_legacy_bucket_exists()
            return True

        self.filesystem.create_dir(path_str, recursive=True)
        return True

    def _check_legacy_bucket_exists(self):
        """Raise if the real OSS bucket does not exist (legacy mode only).

        PyArrow < 16 folds NoSuchBucket into the same NotFound as a missing
        key, so probe the bucket root anonymously, at most once per
        instance (the lock serializes concurrent writers). Reject only on
        an OSS NoSuchBucket error body - a bare 404 may come from a proxy
        or custom endpoint. Any probe setup or transport failure
        (Requests-only TLS/proxy settings do not apply to PyArrow's S3
        client) is indeterminate: fail open."""
        with self._legacy_bucket_lock:
            if not self._legacy_bucket_checked:
                self._legacy_bucket_error = self._probe_legacy_bucket()
                self._legacy_bucket_checked = True
            if self._legacy_bucket_error:
                raise OSError(self._legacy_bucket_error)

    def _probe_legacy_bucket(self) -> Optional[str]:
        """Return an error message if the bucket definitely does not exist."""
        import requests

        endpoint = self.properties.get(OssOptions.OSS_ENDPOINT) or ""
        scheme, _, host = endpoint.rpartition("://")
        url = f"{scheme or 'https'}://{self._oss_bucket}.{host}/"
        try:
            response = requests.get(url, timeout=5, allow_redirects=False, stream=True)
            try:
                status = response.status_code
                body = next(response.iter_content(2048), b"") if status == 404 else b""
            finally:
                response.close()
        except (requests.RequestException, OSError):
            return None
        if status == 404 and b"NoSuchBucket" in body:
            return (f"OSS bucket '{self._oss_bucket}' does not exist "
                    f"(NoSuchBucket from {url})")
        return None

    def rename(self, src: str, dst: str) -> bool:
        src_str = self.to_filesystem_path(src)
        dst_str = self.to_filesystem_path(dst)
        dst_parent = PurePosixPath(dst_str).parent
        if str(dst_parent) and not self.exists(str(dst_parent)):
            self.mkdirs(str(dst_parent))

        try:
            if hasattr(self.filesystem, 'rename'):
                return self.filesystem.rename(src_str, dst_str)

            dst_file_info = self._get_file_info(dst_str)
            if dst_file_info.type != pafs.FileType.NotFound:
                if dst_file_info.type == pafs.FileType.File:
                    return False
                # Make it compatible with HadoopFileIO: if dst is an existing directory,
                # dst=dst/srcFileName
                src_name = PurePosixPath(src_str).name
                dst_str = str(PurePosixPath(dst_str) / src_name)
                final_dst_info = self._get_file_info(dst_str)
                if final_dst_info.type != pafs.FileType.NotFound:
                    return False

            self.filesystem.move(src_str, dst_str)
            return True
        except FileNotFoundError:
            return False
        except (PermissionError, OSError):
            return False

    def delete_quietly(self, path: str):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Ready to delete {path}")

        try:
            if not self.delete(path, False) and self.exists(path):
                self.logger.warning(f"Failed to delete file {path}")
        except Exception:
            self.logger.warning(f"Exception occurs when deleting file {path}", exc_info=True)

    def delete_files_quietly(self, files: List[str]):
        for file_path in files:
            self.delete_quietly(file_path)

    def delete_directory_quietly(self, directory: str):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Ready to delete {directory}")

        try:
            if not self.delete(directory, True) and self.exists(directory):
                self.logger.warning(f"Failed to delete directory {directory}")
        except Exception:
            self.logger.warning(f"Exception occurs when deleting directory {directory}", exc_info=True)

    def try_to_write_atomic(self, path: str, content: str) -> bool:
        if self.exists(path):
            path_str = self.to_filesystem_path(path)
            file_info = self._get_file_info(path_str)
            if file_info.type == pafs.FileType.Directory:
                return False

        temp_path = create_temp_path(path)
        success = False
        try:
            self.write_file(temp_path, content, False)
            success = self.rename(temp_path, path)
        finally:
            if not success:
                self.delete_quietly(temp_path)
        return success

    def copy_file(self, source_path: str, target_path: str, overwrite: bool = False):
        if not overwrite and self.exists(target_path):
            raise FileExistsError(f"Target file {target_path} already exists and overwrite=False")

        source_str = self.to_filesystem_path(source_path)
        target_str = self.to_filesystem_path(target_path)
        target_parent = PurePosixPath(target_str).parent

        if str(target_parent) and not self.exists(str(target_parent)):
            self.mkdirs(str(target_parent))

        self.filesystem.copy_file(source_str, target_str)

    def write_parquet(self, path: str, data: pyarrow.Table, compression: str = 'zstd',
                      zstd_level: int = 1, **kwargs):
        try:
            import pyarrow.parquet as pq

            with self.new_output_stream(path) as output_stream:
                if compression.lower() == 'zstd':
                    kwargs['compression_level'] = zstd_level
                pq.write_table(data, output_stream, compression=compression, **kwargs)

        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Parquet file {path}: {e}") from e

    def write_orc(self, path: str, data: pyarrow.Table, compression: str = 'zstd',
                  zstd_level: int = 1, **kwargs):
        try:
            """Write ORC file using PyArrow ORC writer.

            Note: PyArrow's ORC writer doesn't support compression_level parameter.
            ORC files will use zstd compression with default level
            (which is 3, see https://github.com/facebook/zstd/blob/dev/programs/zstdcli.c)
            instead of the specified level.
            """
            import pyarrow.orc as orc

            data = self._cast_time_columns_for_orc(data)

            with self.new_output_stream(path) as output_stream:
                # ORC compression= was added in PyArrow 7.0; PyArrow 6 lacks it.
                if _pyarrow_lt_7():
                    orc.write_table(data, output_stream, **kwargs)
                else:
                    orc.write_table(
                        data,
                        output_stream,
                        compression=compression,
                        **kwargs
                    )

        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write ORC file {path}: {e}") from e

    def write_avro(
            self, path: str, data: pyarrow.Table,
            avro_schema: Optional[Dict[str, Any]] = None,
            compression: str = 'zstd', zstd_level: int = 1, **kwargs):
        import fastavro
        if avro_schema is None:
            from pypaimon.schema.data_types import PyarrowFieldParser
            avro_schema = PyarrowFieldParser.to_avro_schema(data.schema)

        records_dict = data.to_pydict()

        def record_generator():
            num_rows = len(list(records_dict.values())[0])
            for i in range(num_rows):
                record = {}
                for col in records_dict.keys():
                    value = records_dict[col][i]
                    if isinstance(value, datetime) and value.tzinfo is None:
                        value = value.replace(tzinfo=timezone.utc)
                    record[col] = value
                yield record

        records = record_generator()

        codec_map = {
            'null': 'null',
            'deflate': 'deflate',
            'snappy': 'snappy',
            'bzip2': 'bzip2',
            'xz': 'xz',
            'zstandard': 'zstandard',
            'zstd': 'zstandard',  # zstd is commonly used in Paimon
        }
        compression_lower = compression.lower()

        codec = codec_map.get(compression_lower)
        if codec is None:
            raise ValueError(
                f"Unsupported compression '{compression}' for Avro format. "
                f"Supported compressions: {', '.join(sorted(codec_map.keys()))}."
            )

        with self.new_output_stream(path) as output_stream:
            if codec == 'zstandard':
                kwargs['codec_compression_level'] = zstd_level
            fastavro.writer(output_stream, avro_schema, records, codec=codec, **kwargs)

    def write_lance(self, path: str, data: pyarrow.Table, **kwargs):
        try:
            import lance

            from pypaimon.read.reader.lance_utils import to_lance_specified
            file_path_for_lance, storage_options = to_lance_specified(self, path)

            writer = lance.file.LanceFileWriter(
                file_path_for_lance, data.schema, storage_options=storage_options, **kwargs)
            try:
                # Write all batches
                for batch in data.to_batches():
                    writer.write_batch(batch)
            finally:
                writer.close()
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Lance file {path}: {e}") from e

    def write_mosaic(self, path: str, data: pyarrow.Table, **kwargs):
        try:
            import mosaic
            with self.new_output_stream(path) as output_stream:
                mosaic.write_table(data, output_stream, options=kwargs.get("options"))
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Mosaic file {path}: {e}") from e

    def write_vortex(self, path: str, data: pyarrow.Table, **kwargs):
        try:
            import vortex
            from vortex import store

            from pypaimon.read.reader.vortex_utils import to_vortex_specified
            file_path_for_vortex, store_kwargs = to_vortex_specified(self, path)

            if store_kwargs:
                vortex_store = store.from_url(file_path_for_vortex, **store_kwargs)
                vortex_store.write(vortex.array(data))
            else:
                from vortex._lib.io import write as vortex_write
                vortex_write(vortex.array(data), file_path_for_vortex)
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Vortex file {path}: {e}") from e

    def write_row(self, path: str, data: pyarrow.Table, fields=None, zstd_level: int = 1, **kwargs):
        try:
            from pypaimon.write.writer.format_row_writer import FormatRowWriter

            if fields is None:
                fields = PyarrowFieldParser.to_paimon_schema(data.schema)

            with self.new_output_stream(path) as output_stream:
                writer = FormatRowWriter(output_stream, fields, zstd_level=zstd_level)
                writer.write_table(data)
                writer.close()
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write row file {path}: {e}") from e

    def write_blob(self, path: str, data: pyarrow.Table, **kwargs):
        try:
            if data.num_columns != 1:
                raise RuntimeError(f"Blob format only supports a single column, got {data.num_columns} columns")
            field = data.schema[0]
            if pyarrow.types.is_large_binary(field.type):
                fields = [DataField(0, field.name, AtomicType("BLOB"))]
            else:
                paimon_type = PyarrowFieldParser.to_paimon_type(field.type, field.nullable)
                fields = [DataField(0, field.name, paimon_type)]
            records_dict = data.to_pydict()
            num_rows = data.num_rows
            field_name = fields[0].name
            with self.new_output_stream(path) as output_stream:
                writer = BlobFormatWriter(output_stream)
                for i in range(num_rows):
                    writer.write_value(records_dict[field_name][i], fields, self.uri_reader_factory)
                writer.close()

        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write blob file {path}: {e}") from e

    def to_filesystem_path(self, path: str) -> str:
        import re

        from pyarrow.fs import S3FileSystem

        parsed = urlparse(path)
        normalized_path = re.sub(r'/+', '/', parsed.path) if parsed.path else ''

        if self._is_oss and parsed.scheme == "oss" and "@" in parsed.netloc:
            if self._extract_oss_bucket(path) != self._oss_bucket:
                raise OSError("OSS path is outside current bucket")
            _, _, key = normalized_path.lstrip('/').partition('/')
            parsed = parsed._replace(netloc=self._oss_bucket, path='/' + key)
            normalized_path = '/' + key

        if (self._is_oss and (self._use_jindo or self._oss_bucket_in_endpoint)
                and (parsed.scheme or parsed.netloc)):
            if (not parsed.netloc
                    or (parsed.scheme and parsed.scheme != "oss")
                    or self._extract_oss_bucket(path) != self._oss_bucket):
                raise OSError("OSS path is outside current bucket")

        if parsed.scheme and len(parsed.scheme) == 1 and not parsed.netloc:
            return str(path)

        if parsed.scheme == 'file' and parsed.netloc and parsed.netloc.endswith(':'):
            drive_letter = parsed.netloc.rstrip(':')
            path_part = normalized_path.lstrip('/')
            return f"{drive_letter}:/{path_part}" if path_part else f"{drive_letter}:"

        if self._use_jindo:
            # For JindoFileSystem, pass key only
            path_part = normalized_path.lstrip('/')
            return path_part if path_part else '.'

        if isinstance(self.filesystem, S3FileSystem):
            if parsed.scheme:
                if parsed.netloc:
                    path_part = normalized_path.lstrip('/')
                    # OSS with bucket baked into endpoint: pass key only.
                    if self._is_oss and self._oss_bucket_in_endpoint:
                        return path_part if path_part else '.'
                    result = f"{parsed.netloc}/{path_part}" if path_part else parsed.netloc
                    return result
                else:
                    result = normalized_path.lstrip('/')
                    return result if result else '.'
            else:
                return str(path)

        try:
            from pyarrow.fs import GcsFileSystem
        except ImportError:
            GcsFileSystem = None
        if GcsFileSystem is not None and isinstance(self.filesystem, GcsFileSystem):
            if parsed.scheme and parsed.netloc:
                path_part = normalized_path.lstrip('/')
                return f"{parsed.netloc}/{path_part}" if path_part else parsed.netloc
            return str(path)

        if parsed.scheme:
            if not normalized_path:
                return '.'
            return normalized_path

        return str(path)
