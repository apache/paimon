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

import io
import os
from pathlib import Path
import sys
from unittest.mock import Mock, patch
from urllib.parse import urlparse

import pytest
import pyarrow as pa
import pyarrow.fs as pafs

import pypaimon.multimodal as pmm
from pypaimon.common.options import Options
from pypaimon.multimodal.rosbag import RosbagSource
from pypaimon.multimodal.rosbag.api import (
    RosbagStagingConfig,
    load_from_rosbag,
)
from pypaimon.multimodal.rosbag.loader import _BoundedStagingOutput
from pypaimon.multimodal.rosbag.source import _discover_rosbag_sources
from pypaimon.multimodal.rosbag.staging import (
    _materialized_rosbag,
    _stage_remote_manifest,
    _verify_manifest_members,
)
from pypaimon.multimodal.source_utils import _SourceFileIO


pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="rosbags 0.11 requires Python 3.10 or newer",
)


_MSGTYPE = "std_msgs/msg/String"
_START_NS = 1_700_000_000_000_000_000


def _write_ros2(path, storage_plugin, values=("first", "second")):
    from rosbags.rosbag2 import Writer
    from rosbags.typesys import Stores, get_typestore

    typestore = get_typestore(Stores.ROS2_HUMBLE)
    with Writer(
            path,
            version=Writer.VERSION_LATEST,
            storage_plugin=storage_plugin) as writer:
        connection = writer.add_connection(
            "/text", _MSGTYPE, typestore=typestore)
        for index, value in enumerate(values):
            message = typestore.types[_MSGTYPE](data=value)
            writer.write(
                connection,
                _START_NS + index,
                typestore.serialize_cdr(message, _MSGTYPE),
            )
    return path


def _write_ros1(path, values=("first", "second")):
    from rosbags.rosbag1 import Writer
    from rosbags.typesys import Stores, get_typestore

    typestore = get_typestore(Stores.ROS1_NOETIC)
    with Writer(path) as writer:
        connection = writer.add_connection(
            "/text", _MSGTYPE, typestore=typestore)
        for index, value in enumerate(values):
            message = typestore.types[_MSGTYPE](data=value)
            writer.write(
                connection,
                _START_NS + index,
                typestore.serialize_ros1(message, _MSGTYPE),
            )
    return path


class _RemoteFileIO:

    def __init__(self, objects, directories=None, virtual_directories=False):
        self.objects = dict(objects)
        self.directories = dict(directories or {})
        self.virtual_directories = virtual_directories
        self.close_count = 0

    def _get_fileio(self, path):
        return self

    def to_filesystem_path(self, path):
        parsed = urlparse(path)
        return "%s%s" % (parsed.netloc, parsed.path)

    def get_file_status(self, path):
        if path in self.objects:
            return pafs.FileInfo(
                path, pafs.FileType.File, size=len(self.objects[path]))
        if path in self.directories and not self.virtual_directories:
            return pafs.FileInfo(path, pafs.FileType.Directory)
        raise FileNotFoundError(path)

    def list_status(self, path):
        result = []
        for child in self.directories[path]:
            if child in self.objects:
                result.append(pafs.FileInfo(
                    child,
                    pafs.FileType.File,
                    size=len(self.objects[child]),
                ))
            else:
                result.append(pafs.FileInfo(child, pafs.FileType.Directory))
        return result

    def new_input_stream(self, path):
        return io.BytesIO(self.objects[path])

    def close(self):
        self.close_count += 1


def test_rosbag_source_exposes_original_and_local_paths(tmp_path):
    local_path = tmp_path / "recording.bag"

    source = RosbagSource(
        uri="s3://bucket/robot/recording.bag",
        local_path=local_path,
        format="ros1",
    )

    assert source.uri == "s3://bucket/robot/recording.bag"
    assert source.local_path == Path(local_path)
    assert source.format == "ros1"
    assert source.name == "recording.bag"
    assert source.stem == "recording"
    assert source.is_remote is True


def test_rosbag_public_types_are_exported():
    assert pmm.RosbagSource is RosbagSource
    assert pmm.RosbagLoadResult.__name__ == "RosbagLoadResult"
    assert pmm.RosbagStagingConfig.__name__ == "RosbagStagingConfig"


@pytest.mark.parametrize(
    "staging,match",
    [
        (RosbagStagingConfig(max_bytes=-1), "max_bytes"),
        (RosbagStagingConfig(min_free_bytes=-1), "min_free_bytes"),
        (RosbagStagingConfig(copy_buffer_bytes=0), "copy_buffer_bytes"),
    ],
)
def test_rejects_invalid_staging_config(staging, match):
    with pytest.raises(ValueError, match=match):
        load_from_rosbag(
            object(), [], transform=lambda reader, source: (),
            staging=staging)


def test_discovers_explicit_local_ros1_bag(tmp_path):
    bag = tmp_path / "recording.bag"
    bag.write_bytes(b"rosbag")
    source_file_io = _SourceFileIO(Options({}))

    try:
        manifests = _discover_rosbag_sources([bag], source_file_io)
    finally:
        source_file_io.close()

    assert len(manifests) == 1
    assert manifests[0].uri == bag.resolve().as_uri()
    assert manifests[0].format == "ros1"
    assert len(manifests[0].members) == 1
    assert manifests[0].members[0].relative_path == "recording.bag"
    assert manifests[0].members[0].size == 6


def test_discovers_standalone_mcap_as_ros2(tmp_path):
    mcap = tmp_path / "recording.mcap"
    mcap.write_bytes(b"mcap")
    source_file_io = _SourceFileIO(Options({}))

    try:
        manifests = _discover_rosbag_sources([mcap], source_file_io)
    finally:
        source_file_io.close()

    assert len(manifests) == 1
    assert manifests[0].format == "ros2_mcap"
    assert manifests[0].members[0].relative_path == "recording.mcap"


def test_standalone_db3_requires_explicit_fragment_mode(tmp_path):
    database = tmp_path / "recording.db3"
    database.write_bytes(b"sqlite")
    source_file_io = _SourceFileIO(Options({}))

    try:
        with pytest.raises(ValueError, match="allow_storage_fragment=True"):
            _discover_rosbag_sources([database], source_file_io)
    finally:
        source_file_io.close()


def test_rejects_ros1_active_recording(tmp_path):
    active = tmp_path / "recording.bag.active"
    active.write_bytes(b"active")
    source_file_io = _SourceFileIO(Options({}))

    try:
        with pytest.raises(ValueError, match="still being written"):
            _discover_rosbag_sources([active], source_file_io)
    finally:
        source_file_io.close()


def test_discovers_opted_in_standalone_db3_as_fragment(tmp_path):
    database = tmp_path / "recording.db3"
    database.write_bytes(b"sqlite")
    source_file_io = _SourceFileIO(Options({}))

    try:
        manifests = _discover_rosbag_sources(
            [database], source_file_io, allow_storage_fragment=True)
    finally:
        source_file_io.close()

    assert len(manifests) == 1
    assert manifests[0].format == "ros2_sqlite3_fragment"


def test_rejects_corrupt_opted_in_sqlite_fragment_before_writer(tmp_path):
    database = tmp_path / "recording.db3"
    database.write_bytes(b"not-a-sqlite-database")
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "messages",
        schema=pa.schema([pa.field("value", pa.string(), nullable=False)]),
        options={"blob-as-descriptor": "false"},
    )

    with patch.object(
            connection, "get_table", return_value=table), patch.object(
                table.raw_table,
                "new_batch_write_builder") as new_write_builder:
        with pytest.raises(ValueError, match="SQLite integrity check failed"):
            connection.load_from_rosbag(
                table.identifier,
                database,
                transform=lambda reader, source: pa.table({"value": ["x"]}),
                allow_storage_fragment=True,
            )

    new_write_builder.assert_not_called()


def test_discovers_ros2_sqlite_directory_from_metadata(tmp_path):
    from rosbags.rosbag2 import StoragePlugin

    recording = _write_ros2(
        tmp_path / "recording", StoragePlugin.SQLITE3)
    source_file_io = _SourceFileIO(Options({}))

    try:
        manifests = _discover_rosbag_sources([recording], source_file_io)
    finally:
        source_file_io.close()

    assert len(manifests) == 1
    assert manifests[0].uri == recording.resolve().as_uri()
    assert manifests[0].format == "ros2_sqlite3"
    assert manifests[0].expected_message_count == 2
    assert [member.relative_path for member in manifests[0].members] == [
        "metadata.yaml", "recording.db3"]


def test_rejects_ros2_sqlite_sidecar_as_not_finalized(tmp_path):
    from rosbags.rosbag2 import StoragePlugin

    recording = _write_ros2(
        tmp_path / "recording", StoragePlugin.SQLITE3)
    database = next(recording.glob("*.db3"))
    Path("%s-wal" % database).touch()
    source_file_io = _SourceFileIO(Options({}))

    try:
        with pytest.raises(ValueError, match="not finalized"):
            _discover_rosbag_sources([recording], source_file_io)
    finally:
        source_file_io.close()


def test_detects_ros2_directory_member_added_after_discovery(tmp_path):
    from rosbags.rosbag2 import StoragePlugin

    recording = _write_ros2(
        tmp_path / "recording", StoragePlugin.SQLITE3)
    source_file_io = _SourceFileIO(Options({}))
    try:
        manifest = _discover_rosbag_sources(
            [recording], source_file_io)[0]
        (recording / "new-split.db3").write_bytes(b"new")

        with pytest.raises(ValueError, match="members changed"):
            _verify_manifest_members(manifest, source_file_io)
    finally:
        source_file_io.close()


def test_detects_same_size_local_source_replacement(tmp_path):
    bag = tmp_path / "recording.bag"
    bag.write_bytes(b"old")
    source_file_io = _SourceFileIO(Options({}))
    try:
        manifest = _discover_rosbag_sources([bag], source_file_io)[0]
        original_mtime_ns = bag.stat().st_mtime_ns
        bag.write_bytes(b"new")
        os.utime(
            bag,
            ns=(original_mtime_ns + 1_000_000_000,) * 2,
        )

        with pytest.raises(ValueError, match="source changed"):
            _verify_manifest_members(manifest, source_file_io)
    finally:
        source_file_io.close()


def test_discovers_ros2_mcap_directory_from_metadata(tmp_path):
    from rosbags.rosbag2 import StoragePlugin

    recording = _write_ros2(
        tmp_path / "recording", StoragePlugin.MCAP)
    source_file_io = _SourceFileIO(Options({}))

    try:
        manifests = _discover_rosbag_sources([recording], source_file_io)
    finally:
        source_file_io.close()

    assert len(manifests) == 1
    assert manifests[0].format == "ros2_mcap"
    assert [member.relative_path for member in manifests[0].members] == [
        "metadata.yaml", "recording.mcap"]


def test_explicit_ros2_storage_directory_without_metadata_is_incomplete(tmp_path):
    from rosbags.rosbag2 import StoragePlugin

    recording = _write_ros2(
        tmp_path / "recording", StoragePlugin.MCAP)
    (recording / "metadata.yaml").unlink()
    source_file_io = _SourceFileIO(Options({}))

    try:
        with pytest.raises(ValueError, match="missing metadata.yaml"):
            _discover_rosbag_sources([recording], source_file_io)
    finally:
        source_file_io.close()


def test_recursively_discovers_sorts_and_deduplicates_mixed_sources(tmp_path):
    from rosbags.rosbag2 import StoragePlugin

    root = tmp_path / "sources"
    root.mkdir()
    ros1 = _write_ros1(root / "b.bag")
    ros2 = _write_ros2(root / "nested" / "a", StoragePlugin.SQLITE3)
    (root / "README.txt").write_text("ignored", encoding="utf-8")
    source_file_io = _SourceFileIO(Options({}))

    try:
        manifests = _discover_rosbag_sources(
            [root, ros1, ros2], source_file_io)
    finally:
        source_file_io.close()

    assert [(item.uri, item.format) for item in manifests] == [
        (ros1.resolve().as_uri(), "ros1"),
        (ros2.resolve().as_uri(), "ros2_sqlite3"),
    ]


def test_rejects_encoded_ros2_member_path_traversal(tmp_path):
    from rosbags.rosbag2 import StoragePlugin
    from ruamel.yaml import YAML

    recording = _write_ros2(
        tmp_path / "recording", StoragePlugin.SQLITE3)
    metadata_path = recording / "metadata.yaml"
    yaml = YAML()
    metadata = yaml.load(metadata_path.read_text(encoding="utf-8"))
    metadata["rosbag2_bagfile_information"]["relative_file_paths"] = [
        "%252e%252e/escape.db3"]
    with metadata_path.open("w", encoding="utf-8") as stream:
        yaml.dump(metadata, stream)
    source_file_io = _SourceFileIO(Options({}))

    try:
        with pytest.raises(ValueError, match="unsafe.*relative_file_paths"):
            _discover_rosbag_sources([recording], source_file_io)
    finally:
        source_file_io.close()


def test_rejects_duplicate_normalized_ros2_members(tmp_path):
    from rosbags.rosbag2 import StoragePlugin
    from ruamel.yaml import YAML

    recording = _write_ros2(
        tmp_path / "recording", StoragePlugin.SQLITE3)
    metadata_path = recording / "metadata.yaml"
    yaml = YAML()
    metadata = yaml.load(metadata_path.read_text(encoding="utf-8"))
    information = metadata["rosbag2_bagfile_information"]
    member = information["relative_file_paths"][0]
    information["relative_file_paths"] = [member, member.upper()]
    with metadata_path.open("w", encoding="utf-8") as stream:
        yaml.dump(metadata, stream)
    source_file_io = _SourceFileIO(Options({}))

    try:
        with pytest.raises(ValueError, match="duplicate.*relative_file_paths"):
            _discover_rosbag_sources([recording], source_file_io)
    finally:
        source_file_io.close()


def test_rejects_nested_ros2_member_paths_unsupported_by_rosbags(tmp_path):
    from rosbags.rosbag2 import StoragePlugin
    from ruamel.yaml import YAML

    recording = _write_ros2(
        tmp_path / "recording", StoragePlugin.SQLITE3)
    metadata_path = recording / "metadata.yaml"
    yaml = YAML()
    metadata = yaml.load(metadata_path.read_text(encoding="utf-8"))
    information = metadata["rosbag2_bagfile_information"]
    information["relative_file_paths"] = [
        "nested/%s" % information["relative_file_paths"][0]]
    with metadata_path.open("w", encoding="utf-8") as stream:
        yaml.dump(metadata, stream)
    source_file_io = _SourceFileIO(Options({}))

    try:
        with pytest.raises(ValueError, match="nested.*relative_file_paths"):
            _discover_rosbag_sources([recording], source_file_io)
    finally:
        source_file_io.close()


def test_rejects_malformed_ros2_metadata(tmp_path):
    recording = tmp_path / "recording"
    recording.mkdir()
    (recording / "metadata.yaml").write_text("[", encoding="utf-8")
    source_file_io = _SourceFileIO(Options({}))

    try:
        with pytest.raises(ValueError, match="Cannot read ROS2 metadata"):
            _discover_rosbag_sources([recording], source_file_io)
    finally:
        source_file_io.close()


def test_loads_ros1_after_preflight_into_one_snapshot(tmp_path):
    bag = _write_ros1(tmp_path / "recording.bag")
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "messages",
        schema=pa.schema([
            pa.field("source", pa.string(), nullable=False),
            pa.field("timestamp", pa.int64(), nullable=False),
            pa.field("value", pa.string(), nullable=False),
        ]),
        options={"blob-as-descriptor": "false"},
    )

    def transform(reader, source):
        rows = []
        for ros_connection, timestamp, rawdata in reader.messages():
            message = reader.deserialize(rawdata, ros_connection.msgtype)
            rows.append({
                "source": source.name,
                "timestamp": timestamp,
                "value": message.data,
            })
        return pa.Table.from_pylist(rows)

    result = connection.load_from_rosbag(
        table.identifier, bag, transform=transform)

    assert result.source_count == 1
    assert result.batch_count == 1
    assert result.row_count == 2
    assert result.snapshot_id is not None
    assert table.scan().to_arrow().select([
        "source", "timestamp", "value"]
    ).to_pylist() == [
        {
            "source": "recording.bag",
            "timestamp": _START_NS,
            "value": "first",
        },
        {
            "source": "recording.bag",
            "timestamp": _START_NS + 1,
            "value": "second",
        },
    ]


def test_loads_remote_ros1_through_fileio_staging(tmp_path):
    local_bag = _write_ros1(tmp_path / "source.bag")
    remote = _RemoteFileIO({
        "bucket/recording.bag": local_bag.read_bytes(),
    })
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "messages",
        schema=pa.schema([
            pa.field("value", pa.string(), nullable=False),
        ]),
        options={"blob-as-descriptor": "false"},
    )

    def transform(reader, source):
        values = [
            reader.deserialize(rawdata, ros_connection.msgtype).data
            for ros_connection, _, rawdata in reader.messages()
        ]
        return pa.table({"value": values})

    with patch(
            "pypaimon.multimodal.source_utils.ResolvingFileIO",
            return_value=remote):
        result = connection.load_from_rosbag(
            table.identifier,
            "s3://bucket/recording.bag",
            transform=transform,
        )

    assert result.row_count == 2
    assert table.scan().to_arrow().select(["value"]).to_pylist() == [
        {"value": "first"}, {"value": "second"}]
    assert remote.close_count == 1


def test_loads_remote_ros2_directory_from_manifest_members(tmp_path):
    from rosbags.rosbag2 import StoragePlugin

    local = _write_ros2(tmp_path / "local", StoragePlugin.SQLITE3)
    metadata = local / "metadata.yaml"
    database = next(local.glob("*.db3"))
    remote = _RemoteFileIO(
        objects={
            "bucket/recording/metadata.yaml": metadata.read_bytes(),
            "bucket/recording/%s" % database.name: database.read_bytes(),
        },
        directories={
            "bucket/recording": [
                "bucket/recording/metadata.yaml",
                "bucket/recording/%s" % database.name,
            ],
        },
    )
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "messages",
        schema=pa.schema([pa.field("value", pa.string(), nullable=False)]),
        options={"blob-as-descriptor": "false"},
    )

    def transform(reader, source):
        return pa.table({"value": [
            reader.deserialize(rawdata, ros_connection.msgtype).data
            for ros_connection, _, rawdata in reader.messages()
        ]})

    with patch(
            "pypaimon.multimodal.source_utils.ResolvingFileIO",
            return_value=remote):
        result = connection.load_from_rosbag(
            table.identifier,
            "s3://bucket/recording",
            transform=transform,
        )

    assert result.source_count == 1
    assert result.row_count == 2
    assert table.scan().to_arrow().select(["value"]).to_pylist() == [
        {"value": "first"}, {"value": "second"}]


def test_discovers_remote_ros2_virtual_directory_prefix(tmp_path):
    from rosbags.rosbag2 import StoragePlugin

    local = _write_ros2(tmp_path / "local", StoragePlugin.MCAP)
    metadata = local / "metadata.yaml"
    mcap = next(local.glob("*.mcap"))
    remote = _RemoteFileIO(
        objects={
            "bucket/recording/metadata.yaml": metadata.read_bytes(),
            "bucket/recording/%s" % mcap.name: mcap.read_bytes(),
        },
        directories={
            "bucket/recording": [
                "bucket/recording/metadata.yaml",
                "bucket/recording/%s" % mcap.name,
            ],
        },
        virtual_directories=True,
    )

    with patch(
            "pypaimon.multimodal.source_utils.ResolvingFileIO",
            return_value=remote):
        source_file_io = _SourceFileIO(Options({}))
        try:
            manifests = _discover_rosbag_sources(
                ["s3://bucket/recording"], source_file_io)
        finally:
            source_file_io.close()

    assert len(manifests) == 1
    assert manifests[0].format == "ros2_mcap"


def test_remote_source_staging_is_removed_after_transform_scope(tmp_path):
    local_bag = _write_ros1(tmp_path / "source.bag")
    remote = _RemoteFileIO({
        "bucket/recording.bag": local_bag.read_bytes(),
    })
    staging_root = tmp_path / "staging"

    with patch(
            "pypaimon.multimodal.source_utils.ResolvingFileIO",
            return_value=remote):
        source_file_io = _SourceFileIO(Options({}))
        try:
            manifest = _discover_rosbag_sources(
                ["s3://bucket/recording.bag"], source_file_io)[0]
            with _materialized_rosbag(
                    manifest,
                    source_file_io,
                    staging_root,
                    RosbagStagingConfig(min_free_bytes=0)) as local_path:
                assert local_path.is_file()
            assert not staging_root.exists()
        finally:
            source_file_io.close()


def test_remote_staging_limit_stops_before_writing_excess_bytes(tmp_path):
    class UnderreportedRemoteFileIO(_RemoteFileIO):

        def get_file_status(self, path):
            status = super().get_file_status(path)
            return pafs.FileInfo(
                status.path,
                status.type,
                size=1,
            )

    uri = "s3://bucket/recording.bag"
    remote = UnderreportedRemoteFileIO({
        "bucket/recording.bag": b"x" * 1024,
    })
    with patch(
            "pypaimon.multimodal.source_utils.ResolvingFileIO",
            return_value=remote):
        source_file_io = _SourceFileIO(Options({}))
        try:
            manifest = _discover_rosbag_sources([uri], source_file_io)[0]
            staging_root = tmp_path / "staging"
            staging_root.mkdir()

            with pytest.raises(ValueError, match="configured limit"):
                _stage_remote_manifest(
                    manifest,
                    source_file_io,
                    staging_root,
                    RosbagStagingConfig(
                        max_bytes=1,
                        min_free_bytes=0,
                        copy_buffer_bytes=1024,
                    ),
                )

            staged = staging_root / "recording.bag"
            assert staged.stat().st_size <= 1
        finally:
            source_file_io.close()


def test_metadata_count_failure_happens_before_transform_and_writer(tmp_path):
    from rosbags.rosbag2 import StoragePlugin
    from ruamel.yaml import YAML

    recording = _write_ros2(
        tmp_path / "recording", StoragePlugin.SQLITE3)
    metadata_path = recording / "metadata.yaml"
    yaml = YAML()
    metadata = yaml.load(metadata_path.read_text(encoding="utf-8"))
    metadata["rosbag2_bagfile_information"]["message_count"] += 1
    with metadata_path.open("w", encoding="utf-8") as stream:
        yaml.dump(metadata, stream)

    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "messages",
        schema=pa.schema([pa.field("value", pa.string(), nullable=False)]),
        options={"blob-as-descriptor": "false"},
    )
    transform = Mock()

    with patch.object(
            connection, "get_table", return_value=table), patch.object(
                table.raw_table,
                "new_batch_write_builder") as new_write_builder:
        with pytest.raises(ValueError, match="declared 3 messages.*2"):
            connection.load_from_rosbag(
                table.identifier, recording, transform=transform)

    transform.assert_not_called()
    new_write_builder.assert_not_called()


def test_topic_count_failure_happens_before_transform_and_writer(tmp_path):
    from rosbags.rosbag2 import StoragePlugin
    from ruamel.yaml import YAML

    recording = _write_ros2(
        tmp_path / "recording", StoragePlugin.MCAP)
    metadata_path = recording / "metadata.yaml"
    yaml = YAML()
    metadata = yaml.load(metadata_path.read_text(encoding="utf-8"))
    topic = metadata["rosbag2_bagfile_information"][
        "topics_with_message_count"][0]
    topic["message_count"] += 1
    with metadata_path.open("w", encoding="utf-8") as stream:
        yaml.dump(metadata, stream)

    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "messages",
        schema=pa.schema([pa.field("value", pa.string(), nullable=False)]),
        options={"blob-as-descriptor": "false"},
    )
    transform = Mock()

    with patch.object(
            connection, "get_table", return_value=table), patch.object(
                table.raw_table,
                "new_batch_write_builder") as new_write_builder:
        with pytest.raises(ValueError, match="topic /text declares 3.*2"):
            connection.load_from_rosbag(
                table.identifier, recording, transform=transform)

    transform.assert_not_called()
    new_write_builder.assert_not_called()


def test_rechecks_all_sources_after_last_transform_before_writer(tmp_path):
    first = _write_ros1(tmp_path / "a.bag", values=("first",))
    second = _write_ros1(tmp_path / "b.bag", values=("second",))
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "messages",
        schema=pa.schema([pa.field("value", pa.string(), nullable=False)]),
        options={"blob-as-descriptor": "false"},
    )

    def transform(reader, source):
        values = [
            reader.deserialize(rawdata, ros_connection.msgtype).data
            for ros_connection, _, rawdata in reader.messages()
        ]
        if source.name == "b.bag":
            with first.open("ab") as stream:
                stream.write(b"changed")
        return pa.table({"value": values})

    with patch.object(
            connection, "get_table", return_value=table), patch.object(
                table.raw_table,
                "new_batch_write_builder") as new_write_builder:
        with pytest.raises(ValueError, match="source changed"):
            connection.load_from_rosbag(
                table.identifier,
                [first, second],
                transform=transform,
            )

    new_write_builder.assert_not_called()


def test_arrow_ipc_staging_limit_fails_before_writer(tmp_path):
    bag = _write_ros1(tmp_path / "recording.bag", values=("value",))
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "messages",
        schema=pa.schema([pa.field("value", pa.string(), nullable=False)]),
        options={"blob-as-descriptor": "false"},
    )

    def transform(reader, source):
        return pa.table({"value": ["larger-than-one-byte"]})

    with patch.object(
            connection, "get_table", return_value=table), patch.object(
                table.raw_table,
                "new_batch_write_builder") as new_write_builder:
        with pytest.raises(ValueError, match="staging exceeds.*1 bytes"):
            connection.load_from_rosbag(
                table.identifier,
                bag,
                transform=transform,
                staging=RosbagStagingConfig(
                    max_bytes=1,
                    min_free_bytes=0,
                ),
            )

    new_write_builder.assert_not_called()


def test_bounded_staging_output_rejects_bytes_before_writing(tmp_path):
    spool_path = tmp_path / "validated.arrow"

    with _BoundedStagingOutput(spool_path, 1) as output:
        with pytest.raises(ValueError, match="configured limit"):
            output.write(b"too large")

    assert spool_path.stat().st_size == 0


def test_bounded_staging_output_reserves_remote_source_bytes(tmp_path):
    spool_path = tmp_path / "validated.arrow"

    with _BoundedStagingOutput(spool_path, 2) as output:
        output.set_reserved_bytes(1)
        output.write(b"x")
        with pytest.raises(ValueError, match="configured limit"):
            output.write(b"y")

    assert spool_path.stat().st_size == 1


def test_arrow_ipc_staging_requires_free_space_before_transform(tmp_path):
    bag = _write_ros1(tmp_path / "recording.bag", values=("value",))
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "messages",
        schema=pa.schema([pa.field("value", pa.string(), nullable=False)]),
        options={"blob-as-descriptor": "false"},
    )
    transform = Mock(return_value=pa.table({"value": ["value"]}))

    with patch.object(
            connection, "get_table", return_value=table), patch.object(
                table.raw_table,
                "new_batch_write_builder") as new_write_builder, patch(
                    "pypaimon.multimodal.rosbag.loader.shutil.disk_usage",
                    return_value=Mock(free=0)):
        with pytest.raises(ValueError, match="less than 1 free bytes"):
            connection.load_from_rosbag(
                table.identifier,
                bag,
                transform=transform,
                staging=RosbagStagingConfig(min_free_bytes=1),
            )

    transform.assert_not_called()
    new_write_builder.assert_not_called()
