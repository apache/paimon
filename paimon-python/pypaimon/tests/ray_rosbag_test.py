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

import pickle
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon.common.options import Options
from pypaimon.multimodal.rosbag.api import RosbagStagingConfig
from pypaimon.multimodal.rosbag.source import _discover_rosbag_sources
from pypaimon.multimodal.source_utils import _SourceFileIO
from pypaimon.ray.rosbag import _TransformRosbagSource, load_from_rosbag


pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="rosbags 0.11 requires Python 3.10 or newer",
)


_MSGTYPE = "std_msgs/msg/String"


def _write_ros1(path):
    from rosbags.rosbag1 import Writer
    from rosbags.typesys import Stores, get_typestore

    typestore = get_typestore(Stores.ROS1_NOETIC)
    with Writer(path) as writer:
        connection = writer.add_connection(
            "/text", _MSGTYPE, typestore=typestore)
        message = typestore.types[_MSGTYPE](data="worker")
        writer.write(
            connection,
            1_700_000_000_000_000_000,
            typestore.serialize_ros1(message, _MSGTYPE),
        )
    return path


def test_ray_rosbag_loader_is_exported():
    from pypaimon.ray import load_from_rosbag as exported

    assert exported is load_from_rosbag


def test_ray_rosbag_rejects_invalid_staging_before_catalog_access():
    with patch(
            "pypaimon.catalog.catalog_factory.CatalogFactory.create"
    ) as create_catalog:
        with pytest.raises(ValueError, match="copy_buffer_bytes"):
            load_from_rosbag(
                "default.messages",
                [],
                {"warehouse": "/tmp/warehouse"},
                transform=lambda reader, source: (),
                staging=RosbagStagingConfig(copy_buffer_bytes=0),
            )

    create_catalog.assert_not_called()


def test_ray_rosbag_materializes_validation_before_paimon_write(tmp_path):
    bag = tmp_path / "recording.bag"
    bag.write_bytes(b"bag")
    lazy_dataset = Mock(name="lazy_dataset")
    materialized_dataset = Mock(name="materialized_dataset")
    lazy_dataset.materialize.return_value = materialized_dataset
    inputs = Mock(name="inputs")
    inputs.map_batches.return_value = lazy_dataset
    ray_data = Mock(name="ray_data")
    ray_data.from_items.return_value = inputs
    table = Mock()
    table.table_schema.to_arrow_schema.return_value = pa.schema([
        pa.field("value", pa.int64(), nullable=False)])
    catalog = Mock()
    catalog.get_table.return_value = table
    write_result = SimpleNamespace(row_count=3, snapshot_id=7)

    with patch(
            "pypaimon.ray.ray_paimon._require_ray_data",
            return_value=ray_data), patch(
                "pypaimon.catalog.catalog_factory.CatalogFactory.create",
                return_value=catalog), patch(
                    "pypaimon.multimodal.table._target_schema",
                    return_value=pa.schema([
                        pa.field("value", pa.int64(), nullable=False)])), patch(
                    "pypaimon.ray.ray_paimon.write_paimon",
                    return_value=write_result) as write_paimon:
        result = load_from_rosbag(
            "default.messages",
            bag,
            {"warehouse": str(tmp_path / "warehouse")},
            transform=lambda reader, source: pa.table({"value": [1]}),
        )

    lazy_dataset.materialize.assert_called_once_with()
    assert write_paimon.call_args.args[0] is materialized_dataset
    assert result.source_count == 1
    assert result.batch_count is None
    assert result.row_count == 3
    assert result.snapshot_id == 7


def test_ray_rosbag_materialize_failure_never_starts_paimon_write(tmp_path):
    bag = tmp_path / "recording.bag"
    bag.write_bytes(b"bag")
    lazy_dataset = Mock()
    lazy_dataset.materialize.side_effect = ValueError("invalid source")
    inputs = Mock()
    inputs.map_batches.return_value = lazy_dataset
    ray_data = Mock()
    ray_data.from_items.return_value = inputs
    catalog = Mock()
    catalog.get_table.return_value = Mock()

    with patch(
            "pypaimon.ray.ray_paimon._require_ray_data",
            return_value=ray_data), patch(
                "pypaimon.catalog.catalog_factory.CatalogFactory.create",
                return_value=catalog), patch(
                    "pypaimon.multimodal.table._target_schema",
                    return_value=pa.schema([
                        pa.field("value", pa.int64(), nullable=False)])), patch(
                    "pypaimon.ray.ray_paimon.write_paimon") as write_paimon:
        with pytest.raises(ValueError, match="invalid source"):
            load_from_rosbag(
                "default.messages",
                bag,
                {"warehouse": str(tmp_path / "warehouse")},
                transform=lambda reader, source: pa.table({"value": [1]}),
            )

    write_paimon.assert_not_called()


def test_ray_rosbag_rechecks_sources_after_materialize_before_write(tmp_path):
    bag = tmp_path / "recording.bag"
    bag.write_bytes(b"bag")
    materialized_dataset = Mock()
    lazy_dataset = Mock()

    def materialize():
        with bag.open("ab") as stream:
            stream.write(b"changed")
        return materialized_dataset

    lazy_dataset.materialize.side_effect = materialize
    inputs = Mock()
    inputs.map_batches.return_value = lazy_dataset
    ray_data = Mock()
    ray_data.from_items.return_value = inputs
    catalog = Mock()
    catalog.get_table.return_value = Mock()

    with patch(
            "pypaimon.ray.ray_paimon._require_ray_data",
            return_value=ray_data), patch(
                "pypaimon.catalog.catalog_factory.CatalogFactory.create",
                return_value=catalog), patch(
                    "pypaimon.multimodal.table._target_schema",
                    return_value=pa.schema([
                        pa.field("value", pa.int64(), nullable=False)])), patch(
                    "pypaimon.ray.ray_paimon.write_paimon") as write_paimon:
        with pytest.raises(ValueError, match="source changed"):
            load_from_rosbag(
                "default.messages",
                bag,
                {"warehouse": str(tmp_path / "warehouse")},
                transform=lambda reader, source: pa.table({"value": [1]}),
            )

    write_paimon.assert_not_called()


def test_ray_rosbag_worker_validates_and_transforms_one_source(tmp_path):
    bag = _write_ros1(tmp_path / "recording.bag")
    source_file_io = _SourceFileIO(Options({}))
    try:
        manifest = _discover_rosbag_sources([bag], source_file_io)[0]
    finally:
        source_file_io.close()
    schema = pa.schema([pa.field("value", pa.string(), nullable=False)])

    def transform(reader, source):
        values = [
            reader.deserialize(rawdata, connection.msgtype).data
            for connection, _, rawdata in reader.messages()
        ]
        return pa.table({"value": values})

    worker = _TransformRosbagSource(
        transform=transform,
        default_typestore=None,
        typestore_factory=None,
        source_options={},
        staging=RosbagStagingConfig(min_free_bytes=0),
        target_schema=schema,
    )

    output = list(worker(pa.table({
        "manifest": [pickle.dumps(manifest)],
    })))

    assert len(output) == 1
    assert output[0].schema == schema
    assert output[0].to_pylist() == [{"value": "worker"}]


def test_ray_rosbag_rejects_nonserializable_transform_on_driver(tmp_path):
    bag = tmp_path / "recording.bag"
    bag.write_bytes(b"bag")

    class NonSerializableTransform:

        def __init__(self):
            self.values = (value for value in [1])

        def __call__(self, reader, source):
            return pa.table({"value": list(self.values)})

    ray_data = Mock()
    with patch(
            "pypaimon.ray.ray_paimon._require_ray_data",
            return_value=ray_data):
        with pytest.raises(ValueError, match="transform must be Ray-serializable"):
            load_from_rosbag(
                "default.messages",
                bag,
                {"warehouse": str(tmp_path / "warehouse")},
                transform=NonSerializableTransform(),
            )

    ray_data.from_items.assert_not_called()
