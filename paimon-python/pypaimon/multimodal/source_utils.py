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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared validation for external multimodal sources."""

import os
import re
from pathlib import Path, PureWindowsPath
from typing import Mapping
from urllib.parse import quote, unquote, urlparse, urlunparse

from pypaimon.filesystem.resolving_file_io import ResolvingFileIO


class _SourceFileIO:
    """Resolve external source URIs without using target warehouse options."""

    def __init__(self, options):
        self._resolver = ResolvingFileIO(options)

    def _resolve(self, path):
        file_io = self._resolver._get_fileio(path)
        native_path = file_io.to_filesystem_path(path)
        if urlparse(path).scheme.lower() != "file":
            native_path = unquote(native_path)
        return file_io, native_path

    def get_file_status(self, path):
        file_io, native_path = self._resolve(path)
        return file_io.get_file_status(native_path)

    def list_status(self, path):
        file_io, native_path = self._resolve(path)
        return file_io.list_status(native_path)

    def new_input_stream(self, path):
        file_io, native_path = self._resolve(path)
        return file_io.new_input_stream(native_path)

    def to_filesystem_path(self, path):
        return self._resolve(path)[1]

    def close(self):
        self._resolver.close()


def _source_path_text(value):
    try:
        path = os.fspath(value)
    except TypeError as error:
        raise ValueError(
            "paths must contain only filesystem paths or URIs.") from error
    if isinstance(path, bytes):
        raise ValueError("paths must contain only filesystem paths or URIs.")
    return path


def _normalize_source_path(value):
    path = _source_path_text(value)
    parsed = urlparse(path)
    if _is_windows_drive_path(parsed):
        windows_path = PureWindowsPath(path)
        if not windows_path.is_absolute():
            raise ValueError("Windows source paths must be absolute: %s" % path)
        return "file:///%s" % quote(windows_path.as_posix(), safe="/:")
    if not parsed.scheme:
        return Path(path).expanduser().resolve().as_uri()
    return _quote_uri_path(path)


def _quote_uri_path(uri):
    match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*://[^/]*)(.*)$", uri)
    if match is None:
        return uri
    return match.group(1) + quote(match.group(2), safe="/:%")


def _qualified_status_path(parent_path, status):
    status_path = str(status.path)
    status_uri = urlparse(status_path)
    if status_uri.scheme and not _is_windows_drive_path(status_uri):
        return _quote_uri_path(status_path)

    parent_uri = urlparse(parent_path)
    scheme = parent_uri.scheme.lower()
    if scheme == "file":
        return _normalize_source_path(status_path)
    if not scheme or _is_windows_drive_path(parent_uri):
        return _normalize_source_path(status_path)

    if scheme in ("hdfs", "viewfs"):
        return urlunparse((
            scheme,
            parent_uri.netloc,
            quote("/" + status_path.lstrip("/"), safe="/:"),
            "",
            "",
            "",
        ))

    key = status_path.lstrip("/")
    if parent_uri.netloc and not (
            key == parent_uri.netloc
            or key.startswith(parent_uri.netloc + "/")):
        key = parent_uri.netloc + "/" + key
    return "%s://%s" % (scheme, quote(key, safe="/:"))


def _is_windows_drive_path(parsed):
    return len(parsed.scheme) == 1 and not parsed.netloc


def _validated_source_options(source_options):
    if source_options is None:
        return {}
    if not isinstance(source_options, Mapping):
        raise ValueError("source_options must be a mapping.")
    return dict(source_options)


def _validate_source_kerberos(paths, source_options, source_name="HDF5"):
    source_principal = (
        source_options.get("security.kerberos.login.principal")
        or source_options.get("security.principal")
    )
    source_keytab = (
        source_options.get("security.kerberos.login.keytab")
        or source_options.get("security.keytab")
    )
    if not source_principal and not source_keytab:
        return
    if bool(source_principal) != bool(source_keytab):
        raise ValueError(
            "Source Kerberos principal and keytab must be both set or both "
            "unset.")
    if not any(
            urlparse(_source_path_text(path)).scheme.lower()
            in ("hdfs", "viewfs") for path in paths):
        return
    raise ValueError(
        "%s sources cannot use an explicit Kerberos keytab in a shared "
        "process because kinit overwrites process-global credentials. "
        "Run the load in a process-isolated worker with a pre-acquired "
        "ticket cache and omit the source principal and keytab options."
        % source_name)
