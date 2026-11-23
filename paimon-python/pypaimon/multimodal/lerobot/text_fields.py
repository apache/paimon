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

"""Optional source text files stored as LeRobot dataset columns."""

from collections.abc import Mapping
from fnmatch import fnmatchcase
from glob import has_magic
from pathlib import Path
import posixpath
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.fs as pafs

from pypaimon.filesystem.pyarrow_file_io import LegacyOssDirectoryListingError
from pypaimon.multimodal.lerobot.source import (
    _read_remote_bytes,
    _relative_dataset_path,
    _remote_source_path,
)
from pypaimon.multimodal.source_utils import _qualified_status_path


_TEXT_SUFFIXES = {".json", ".txt", ".md", ".yaml", ".yml", ".jsonl", ".ndjson", ".csv", ".tsv"}


def _validated_text_fields(fields, reserved):
    if fields is None:
        return {}
    if not isinstance(fields, Mapping):
        raise ValueError("extra_dataset_fields must map column names to relative file paths.")
    result = {}
    for name, pattern in fields.items():
        if not isinstance(name, str) or not name.strip() or name in reserved:
            raise ValueError("Invalid or reserved extra_dataset_fields column: %r." % (name,))
        result[name] = _relative_dataset_path(pattern, "extra_dataset_fields.%s" % name)
        if not has_magic(result[name]):
            _validate_text_suffix(result[name])
    return result


def _text_fields_schema(fields):
    return [pa.field(name, pa.map_(pa.string(), pa.string()) if has_magic(pattern) else pa.string(),
                     nullable=True) for name, pattern in fields.items()]


def _validate_text_suffix(path):
    if posixpath.splitext(path)[1].lower() not in _TEXT_SUFFIXES:
        raise ValueError("Unsupported extra dataset text file suffix: %s." % path)


def _local_text_path(root, relative):
    path = root / relative
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as error:
        raise ValueError("Extra dataset text file must stay within the source directory: %s."
                         % relative) from error
    return path


def _glob_fileio(source, pattern):
    def walk(directory, parts):
        uri = (_remote_source_path(source.path, directory, "extra_dataset_fields", source.file_io)
               if directory else source.path)
        try:
            statuses = source.file_io.list_status(uri)
        except FileNotFoundError:
            return
        except LegacyOssDirectoryListingError as error:
            raise ValueError("Extra dataset text file listing is unavailable at %s; use Jindo "
                             "or upgrade PyArrow." % uri) from error
        part = parts[0]
        if part == "**" and len(parts) > 1:
            yield from walk(directory, parts[1:])
        for status in statuses:
            name = posixpath.basename(unquote(urlparse(_qualified_status_path(uri, status)).path).rstrip("/"))
            child = posixpath.join(directory, name)
            if part == "**":
                if status.type == pafs.FileType.Directory:
                    yield from walk(child, parts)
                elif len(parts) == 1 and status.type == pafs.FileType.File:
                    yield child
            elif fnmatchcase(name, part):
                if len(parts) == 1 and status.type == pafs.FileType.File:
                    yield child
                elif len(parts) > 1 and status.type == pafs.FileType.Directory:
                    yield from walk(child, parts[1:])

    return sorted(set(walk("", pattern.split("/"))))


def _matches_path(path, pattern):
    """Match Hub paths with the same directory boundaries as filesystem globs."""
    parts, patterns = path.split("/"), pattern.split("/")

    def match(i, j):
        if j == len(patterns):
            return i == len(parts)
        if patterns[j] == "**":
            return match(i, j + 1) or (i < len(parts) and match(i + 1, j))
        return i < len(parts) and fnmatchcase(parts[i], patterns[j]) and match(i + 1, j + 1)

    return match(0, 0)


def _read_text_fields(source, fields):
    if not fields:
        return {}
    root = source.root
    if root is None and urlparse(source.path).scheme == "file":
        root = Path(source.file_io.to_filesystem_path(source.path))
    hub_files = None
    missing_errors = (FileNotFoundError,)
    if root is None and source.file_io is None:
        from huggingface_hub.errors import EntryNotFoundError
        missing_errors += (EntryNotFoundError,)
    if root is None and source.file_io is None and any(has_magic(pattern) for pattern in fields.values()):
        from huggingface_hub import list_repo_files
        hub_files = list_repo_files(source.repo_id, repo_type="dataset")
    result = {}
    for name, pattern in fields.items():
        multiple = has_magic(pattern)
        if not multiple:
            paths = [pattern]
        elif root is not None:
            paths = sorted(path.relative_to(root).as_posix()
                           for path in root.glob(pattern) if path.is_file())
        elif source.file_io is not None:
            paths = _glob_fileio(source, pattern)
        else:
            paths = sorted(path for path in hub_files if _matches_path(path, pattern))
        if not paths:
            result[name] = None
            continue
        base = []
        for part in pattern.split("/"):
            if has_magic(part):
                break
            base.append(part)
        values = {}
        for path in paths:
            _validate_text_suffix(path)
            try:
                if root is not None:
                    content = _local_text_path(root, path).read_bytes()
                elif source.file_io is not None:
                    uri = _remote_source_path(source.path, path, "extra_dataset_fields.%s" % name,
                                              source.file_io)
                    content = _read_remote_bytes(source.file_io, uri)
                else:
                    from huggingface_hub import hf_hub_download
                    content = Path(hf_hub_download(source.repo_id, path, repo_type="dataset")).read_bytes()
                text = content.decode("utf-8")
            except missing_errors:
                result[name] = None
                break
            except (OSError, UnicodeError) as error:
                raise ValueError("Cannot read UTF-8 extra dataset field %s from %s: %s."
                                 % (name, path, error)) from error
            if multiple:
                key = posixpath.splitext(posixpath.relpath(path, "/".join(base) or "."))[0]
                if key in values:
                    raise ValueError("Duplicate extra dataset field key %r for %s." % (key, name))
                values[key] = text
            else:
                result[name] = text
        if multiple and name not in result:
            result[name] = values
    return result
