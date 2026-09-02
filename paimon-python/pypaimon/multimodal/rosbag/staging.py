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

"""Bounded local materialization for ROSBag FileIO sources."""

import shutil
from contextlib import closing, contextmanager
from pathlib import Path
from urllib.parse import urlparse

import pyarrow.fs as pafs

from pypaimon.multimodal.rosbag.source import _status_mtime_ns
from pypaimon.multimodal.source_utils import _qualified_status_path


@contextmanager
def _materialized_rosbag(
        manifest,
        source_file_io,
        staging_root,
        staging,
        base_staging_bytes=0):
    """Yield a local file/directory and reject detected source mutations."""
    if urlparse(manifest.uri).scheme.lower() == "file":
        local_path = Path(source_file_io.to_filesystem_path(manifest.uri))
        yield local_path
        _verify_manifest_members(manifest, source_file_io)
        return

    if staging.copy_buffer_bytes <= 0:
        raise ValueError("staging.copy_buffer_bytes must be positive.")
    staging_root.mkdir(parents=True, exist_ok=False)
    try:
        local_path = _stage_remote_manifest(
            manifest,
            source_file_io,
            staging_root,
            staging,
            base_staging_bytes=base_staging_bytes,
        )
        yield local_path
        _verify_manifest_members(manifest, source_file_io)
    finally:
        shutil.rmtree(str(staging_root), ignore_errors=True)


def _stage_remote_manifest(
        manifest,
        source_file_io,
        staging_root,
        staging,
        base_staging_bytes=0):
    if shutil.disk_usage(str(staging_root)).free < staging.min_free_bytes:
        raise ValueError(
            "ROSBag staging has less than %d free bytes at %s."
            % (staging.min_free_bytes, staging_root))

    expected_bytes = sum(member.size for member in manifest.members)
    if (
            staging.max_bytes is not None
            and base_staging_bytes + expected_bytes > staging.max_bytes):
        raise ValueError(
            "ROSBag staging exceeds configured limit of %d bytes while "
            "copying %s."
            % (staging.max_bytes, manifest.uri))

    total_copied = 0
    for member in manifest.members:
        destination = staging_root / member.relative_path
        _require_contained_path(staging_root, destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        copied = 0
        with closing(source_file_io.new_input_stream(member.uri)) as source:
            with destination.open("wb") as output:
                while True:
                    chunk = source.read(staging.copy_buffer_bytes)
                    if not chunk:
                        break
                    next_total = total_copied + len(chunk)
                    if (
                            staging.max_bytes is not None
                            and base_staging_bytes + next_total
                            > staging.max_bytes):
                        raise ValueError(
                            "ROSBag staging exceeds configured limit of %d "
                            "bytes while copying %s."
                            % (staging.max_bytes, member.uri))
                    output.write(chunk)
                    copied += len(chunk)
                    total_copied = next_total
        if copied != member.size:
            raise ValueError(
                "ROSBag source %s changed or returned a short read: "
                "expected %d bytes, copied %d."
                % (member.uri, member.size, copied))

    _verify_manifest_members(manifest, source_file_io)
    local_path = (
        staging_root
        if any(member.relative_path == "metadata.yaml"
               for member in manifest.members)
        else staging_root / manifest.members[0].relative_path
    )
    return local_path


def _verify_manifest_members(manifest, source_file_io):
    if manifest.directory_members is not None:
        actual_members = tuple(sorted(
            _qualified_status_path(manifest.uri, status)
            for status in source_file_io.list_status(manifest.uri)
        ))
        if actual_members != manifest.directory_members:
            raise ValueError(
                "ROSBag source directory members changed during ingestion: "
                "%s" % manifest.uri)
    for member in manifest.members:
        try:
            status = source_file_io.get_file_status(member.uri)
        except FileNotFoundError as error:
            raise ValueError(
                "ROSBag source changed during ingestion; member disappeared: "
                "%s" % member.uri) from error
        if status.type != pafs.FileType.File or status.size != member.size:
            raise ValueError(
                "ROSBag source changed during ingestion: %s" % member.uri)
        mtime_ns = _status_mtime_ns(status)
        if (
                member.mtime_ns is not None
                and mtime_ns is not None
                and mtime_ns != member.mtime_ns):
            raise ValueError(
                "ROSBag source changed during ingestion: %s" % member.uri)


def _require_contained_path(root, destination):
    resolved_root = root.resolve()
    resolved_destination = destination.resolve()
    try:
        resolved_destination.relative_to(resolved_root)
    except ValueError as error:
        raise ValueError(
            "ROSBag staging member escapes its temporary directory: %s"
            % destination) from error
