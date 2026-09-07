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

"""Validate ROSBag transforms before creating a Paimon writer."""

import io
from contextlib import contextmanager
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from urllib.parse import quote, urlparse

import pyarrow as pa

from pypaimon.multimodal.arrow_utils import strict_arrow_table
from pypaimon.multimodal.rosbag.source import RosbagSource
from pypaimon.multimodal.rosbag.staging import (
    _materialized_rosbag,
    _verify_manifest_members,
)
from pypaimon.multimodal.table import _target_schema
from pypaimon.write.commit_callback import CommitCallback


class _SnapshotRecorder(CommitCallback):

    def __init__(self):
        self.snapshot_id = None

    def call(self, context):
        self.snapshot_id = context.snapshot.id


class _BoundedStagingOutput(io.RawIOBase):
    """File output that rejects a write before it exceeds its byte limit."""

    def __init__(self, path, max_bytes):
        super().__init__()
        self._output = Path(path).open("wb")
        self._max_bytes = max_bytes
        self._reserved_bytes = 0

    def set_reserved_bytes(self, byte_count):
        required_bytes = self._output.tell() + byte_count
        if required_bytes > self._max_bytes:
            self._raise_limit(required_bytes)
        self._reserved_bytes = byte_count

    def writable(self):
        return True

    def write(self, value):
        required_bytes = (
            self._output.tell() + len(value) + self._reserved_bytes)
        if required_bytes > self._max_bytes:
            self._raise_limit(required_bytes)
        return self._output.write(value)

    def _raise_limit(self, required_bytes):
        raise ValueError(
            "ROSBag staging exceeds configured limit of %d bytes "
            "(requires at least %d bytes)."
            % (self._max_bytes, required_bytes))

    def tell(self):
        return self._output.tell()

    def flush(self):
        if not self._output.closed:
            self._output.flush()

    def close(self):
        if not self.closed:
            try:
                super().close()
            finally:
                self._output.close()


@contextmanager
def _staging_sink(path, max_bytes):
    if max_bytes is None:
        with pa.OSFile(str(path), "wb") as sink:
            yield sink, None
        return
    with _BoundedStagingOutput(path, max_bytes) as output:
        with pa.PythonFile(output, mode="w") as sink:
            yield sink, output


def _load_rosbag_manifests(
        table,
        manifests,
        transform,
        source_file_io,
        AnyReader,
        *,
        default_typestore,
        typestore_factory,
        staging):
    from pypaimon.multimodal.rosbag.api import RosbagLoadResult

    target_schema = _target_schema(table.raw_table)
    with TemporaryDirectory(
            prefix="pypaimon_rosbag_", dir=staging.directory) as temp_dir:
        _check_staging_free_space(temp_dir, staging)
        spool_path = Path(temp_dir) / "validated.arrow"
        batch_count = 0
        row_count = 0
        with _staging_sink(
                spool_path, staging.max_bytes) as (sink, bounded_output):
            with pa.ipc.new_file(sink, target_schema) as spool:
                for source_index, manifest in enumerate(manifests):
                    source_staging_bytes = _source_staging_bytes(manifest)
                    _check_staging_bytes(
                        sink.tell() + source_staging_bytes, staging)
                    if bounded_output is not None:
                        bounded_output.set_reserved_bytes(
                            source_staging_bytes)
                    try:
                        for table_value in _transform_rosbag_manifest(
                                manifest,
                                transform,
                                source_file_io,
                                AnyReader,
                                target_schema,
                                default_typestore=default_typestore,
                                typestore_factory=typestore_factory,
                                staging=staging,
                                staging_root=Path(temp_dir) /
                                ("source-%06d" % source_index),
                                batch_index=batch_count,
                                base_staging_bytes=sink.tell()):
                            batch_count += 1
                            row_count += table_value.num_rows
                            for record_batch in table_value.to_batches():
                                spool.write_batch(record_batch)
                                _check_staging_bytes(
                                    sink.tell() + source_staging_bytes,
                                    staging)
                    finally:
                        if bounded_output is not None:
                            bounded_output.set_reserved_bytes(0)
            _check_staging_bytes(sink.tell(), staging)

        for manifest in manifests:
            _verify_manifest_members(manifest, source_file_io)
        snapshot_id = _write_spool(table, spool_path)
        return RosbagLoadResult(
            source_count=len(manifests),
            batch_count=batch_count,
            row_count=row_count,
            snapshot_id=snapshot_id,
        )


def _source_staging_bytes(manifest):
    if urlparse(manifest.uri).scheme.lower() == "file":
        return 0
    return sum(member.size for member in manifest.members)


def _check_staging_bytes(actual_bytes, staging):
    if staging.max_bytes is not None and actual_bytes > staging.max_bytes:
        raise ValueError(
            "ROSBag staging exceeds configured limit of %d bytes "
            "(requires at least %d bytes)."
            % (staging.max_bytes, actual_bytes))


def _check_staging_free_space(directory, staging):
    if shutil.disk_usage(str(directory)).free < staging.min_free_bytes:
        raise ValueError(
            "ROSBag staging has less than %d free bytes at %s."
            % (staging.min_free_bytes, directory))


def _transform_rosbag_manifest(
        manifest,
        transform,
        source_file_io,
        AnyReader,
        target_schema,
        *,
        default_typestore,
        typestore_factory,
        staging,
        staging_root,
        batch_index=0,
        base_staging_bytes=0):
    """Yield validated Arrow tables for one fully preflighted source."""
    produced_rows = 0
    with _materialized_rosbag(
            manifest,
            source_file_io,
            staging_root,
            staging,
            base_staging_bytes=base_staging_bytes) as local_path:
        if manifest.format == "ros2_sqlite3_fragment":
            _validate_sqlite_fragment(local_path, manifest.uri)
        _preflight_reader(
            manifest,
            local_path,
            AnyReader,
            default_typestore,
            typestore_factory,
        )
        source = RosbagSource(
            uri=manifest.uri,
            local_path=local_path,
            format=manifest.format,
        )
        typestore = _new_typestore(default_typestore, typestore_factory)
        with AnyReader(
                [local_path], default_typestore=typestore) as reader:
            transformed = transform(reader, source)
            batches = None
            try:
                batches = _arrow_batches(transformed)
                for index, value in enumerate(batches, start=batch_index):
                    table_value = strict_arrow_table(
                        value,
                        target_schema,
                        manifest.uri,
                        index,
                        "ROSBag",
                    )
                    produced_rows += table_value.num_rows
                    yield table_value
            finally:
                _close_transform_iterator(
                    batches if batches is not None else transformed)
    if produced_rows == 0:
        raise ValueError(
            "ROSBag source %s produced no rows." % manifest.uri)


def _validate_sqlite_fragment(local_path, source_uri):
    import sqlite3

    database_uri = "file:%s?mode=ro&immutable=1" % quote(
        str(local_path), safe="/")
    try:
        connection = sqlite3.connect(database_uri, uri=True)
        try:
            results = [row[0] for row in connection.execute(
                "PRAGMA quick_check")]
        finally:
            connection.close()
    except sqlite3.DatabaseError as error:
        raise ValueError(
            "SQLite integrity check failed for ROSBag fragment %s: %s"
            % (source_uri, error)) from error
    if results != ["ok"]:
        raise ValueError(
            "SQLite integrity check failed for ROSBag fragment %s: %s"
            % (source_uri, "; ".join(results)))


def _preflight_reader(
        manifest,
        local_path,
        AnyReader,
        default_typestore,
        typestore_factory):
    typestore = _new_typestore(default_typestore, typestore_factory)
    with AnyReader([local_path], default_typestore=typestore) as reader:
        topic_counts = {}
        actual_count = 0
        for connection, _, _ in reader.messages():
            actual_count += 1
            topic_counts[connection.id] = (
                topic_counts.get(connection.id, 0) + 1)
        declared_count = reader.message_count
        connections = list(reader.connections)
    if actual_count != declared_count:
        raise ValueError(
            "ROSBag source %s declared %d messages but %d were readable."
            % (manifest.uri, declared_count, actual_count))
    if (
            manifest.expected_message_count is not None
            and actual_count != manifest.expected_message_count):
        raise ValueError(
            "ROSBag source %s metadata declares %d messages but %d were "
            "readable."
            % (
                manifest.uri,
                manifest.expected_message_count,
                actual_count,
            ))
    for connection in connections:
        actual_topic_count = topic_counts.get(connection.id, 0)
        if actual_topic_count != connection.msgcount:
            raise ValueError(
                "ROSBag source %s topic %s declares %d messages but %d "
                "were readable."
                % (
                    manifest.uri,
                    connection.topic,
                    connection.msgcount,
                    actual_topic_count,
                ))


def _new_typestore(default_typestore, typestore_factory):
    if typestore_factory is not None:
        return typestore_factory()
    return default_typestore


def _arrow_batches(transformed):
    if isinstance(transformed, (pa.Table, pa.RecordBatch)):
        return iter([transformed])
    if transformed is None or isinstance(transformed, (str, bytes, dict)):
        raise ValueError(
            "ROSBag transform must return Arrow data or an iterable of "
            "Arrow data.")
    try:
        return iter(transformed)
    except TypeError as error:
        raise ValueError(
            "ROSBag transform must return Arrow data or an iterable of "
            "Arrow data.") from error


def _close_transform_iterator(iterator):
    close = getattr(iterator, "close", None)
    if close is not None:
        close()


def _write_spool(table, spool_path):
    write_builder = table.raw_table.new_batch_write_builder()
    table_write = None
    table_commit = None
    commit_started = False
    snapshot_recorder = _SnapshotRecorder()
    try:
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_commit.add_commit_callback(snapshot_recorder)
        with pa.memory_map(str(spool_path), "r") as source:
            spool = pa.ipc.open_file(source)
            for index in range(spool.num_record_batches):
                table_write.write_arrow(pa.Table.from_batches([
                    spool.get_batch(index)]))
        commit_messages = table_write.prepare_commit()
        commit_started = True
        table_commit.commit(commit_messages)
        if snapshot_recorder.snapshot_id is None:
            raise RuntimeError(
                "ROSBag append committed without reporting a snapshot id.")
        return snapshot_recorder.snapshot_id
    except BaseException:
        if table_write is not None and not commit_started:
            table_write.abort()
        raise
    finally:
        try:
            if table_write is not None:
                table_write.close()
        finally:
            if table_commit is not None:
                table_commit.close()
