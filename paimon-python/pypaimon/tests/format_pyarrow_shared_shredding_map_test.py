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

import base64
from datetime import datetime, time, timezone
import json
import os
import shutil
import struct
import tempfile
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.orc as orc
import pyarrow.parquet as pq

from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.schema.data_types import (
    ArrayType,
    AtomicType,
    DataField,
    MapType,
    RowType,
)


class _LocalFileIO:
    filesystem = pafs.LocalFileSystem()

    def to_filesystem_path(self, path):
        return path


def _metadata(compression):
    field_dict = json.dumps(
        {"camera": 0, "state": 1, "action": 2},
        separators=(",", ":"), sort_keys=True).encode("utf-8")
    if compression == "none":
        compressed = field_dict
    elif compression == "zstd":
        compressed = bytes(pa.Codec("zstd").compress(field_dict))
    else:
        payload = bytes(pa.Codec("lz4_raw").compress(field_dict))
        compressed = struct.pack("<ii", len(payload), len(field_dict)) + payload

    return {
        "paimon.map.storage-layout": "shared-shredding",
        "paimon.map.shared-shredding.version": "1",
        "paimon.map.shared-shredding.field-dict": compressed.decode("latin-1"),
        "paimon.map.shared-shredding.field-dict-compression": compression,
        "paimon.map.shared-shredding.field-dict-original-size": str(len(field_dict)),
        "paimon.map.shared-shredding.num-columns": "2",
    }


class SharedShreddingMapReaderTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.value_arrow_type = pa.struct([
            pa.field("record_index", pa.int64()),
            pa.field("timestamp_ns", pa.int64()),
        ])
        self.value_type = RowType(True, [
            DataField(1, "record_index", AtomicType("BIGINT")),
            DataField(2, "timestamp_ns", AtomicType("BIGINT")),
        ])

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write(self, compression, file_format):
        values0 = pa.array([
            {"record_index": 10, "timestamp_ns": 100},
            {"record_index": 40, "timestamp_ns": 400},
            None,
            None,
        ], type=self.value_arrow_type)
        values1 = pa.array([
            {"record_index": 20, "timestamp_ns": 200},
            None,
            None,
            None,
        ], type=self.value_arrow_type)
        overflow = pa.array([
            [(2, {"record_index": 30, "timestamp_ns": 300})],
            [(99, {"record_index": 50, "timestamp_ns": 500})],
            None,
            [],
        ], type=pa.map_(pa.int32(), self.value_arrow_type))
        physical = pa.StructArray.from_arrays(
            [
                pa.array([[0, 1], [1, -1], [0, -1], [-1, -1]],
                         type=pa.list_(pa.int32())),
                values0,
                values1,
                overflow,
            ],
            names=["__field_mapping", "__col_0", "__col_1", "__overflow"],
            mask=pa.array([False, False, True, False]),
        )
        field = pa.field(
            "content_refs", physical.type, metadata=_metadata(compression))
        table = pa.Table.from_arrays(
            [pa.array([0, 1, 2, 3]), physical],
            schema=pa.schema([pa.field("id", pa.int64()), field]))
        path = os.path.join(
            self.tmp, "{}.{}".format(compression, file_format))
        if file_format == "parquet":
            pq.write_table(table, path, row_group_size=3)
        else:
            orc.write_table(table, path)
        return path

    def test_reads_complete_map_for_all_metadata_compressions(self):
        expected = [
            [
                ("camera", {"record_index": 10, "timestamp_ns": 100}),
                ("state", {"record_index": 20, "timestamp_ns": 200}),
                ("action", {"record_index": 30, "timestamp_ns": 300}),
            ],
            [("state", {"record_index": 40, "timestamp_ns": 400})],
            None,
            [],
        ]
        for compression in ("none", "lz4", "zstd"):
            with self.subTest(compression=compression):
                self._assert_complete_map("parquet", compression, expected)

    def _assert_complete_map(
            self, file_format, compression, expected, path=None):
        reader = FormatPyArrowReader(
            _LocalFileIO(), file_format,
            path or self._write(compression, file_format),
            [DataField(
                0,
                "content_refs",
                MapType(
                    True, AtomicType("STRING", False), self.value_type),
            )],
            None,
            batch_size=2,
        )
        actual = []
        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break
            self.assertTrue(pa.types.is_map(batch.column(0).type))
            actual.extend(batch.column(0).to_pylist())
        self.assertEqual(expected, actual)

    def test_reads_arrow_schema_metadata_from_orc(self):
        path = self._write("none", "orc")
        physical_schema = orc.ORCFile(path).schema
        fields = list(physical_schema)
        index = physical_schema.get_field_index("content_refs")
        fields[index] = pa.field(
            "content_refs", fields[index].type, metadata=_metadata("none"))
        arrow_schema = base64.b64encode(
            pa.schema(fields).serialize().to_pybytes())

        expected = [
            [
                ("camera", {"record_index": 10, "timestamp_ns": 100}),
                ("state", {"record_index": 20, "timestamp_ns": 200}),
                ("action", {"record_index": 30, "timestamp_ns": 300}),
            ],
            [("state", {"record_index": 40, "timestamp_ns": 400})],
            None,
            [],
        ]
        metadata = mock.Mock()
        metadata.get.side_effect = lambda key: (
            arrow_schema if key in (b"ARROW:schema", "ARROW:schema") else None)
        orc_file = mock.Mock(metadata=metadata)
        with mock.patch("pyarrow.orc.ORCFile", return_value=orc_file):
            self._assert_complete_map(
                "orc", "none", expected, path=path)

    def test_restores_time_values_from_orc(self):
        overflow = pa.array(
            [[(2, 5678)]], type=pa.map_(pa.int32(), pa.int32()))
        physical = pa.StructArray.from_arrays(
            [
                pa.array([[0, -1]], type=pa.list_(pa.int32())),
                pa.array([1234], type=pa.int32()),
                pa.array([None], type=pa.int32()),
                overflow,
            ],
            names=["__field_mapping", "__col_0", "__col_1", "__overflow"],
        )
        path = os.path.join(self.tmp, "time.orc")
        orc.write_table(pa.table({"content_refs": physical}), path)

        result = self._read_orc_shared_map(path, AtomicType("TIME(3)"))

        self.assertEqual(pa.map_(pa.string(), pa.time32("ms")), result.type)
        self.assertEqual(
            [[("camera", time(0, 0, 1, 234000)),
              ("action", time(0, 0, 5, 678000))]],
            result.to_pylist(),
        )

    def test_restores_timestamp_precision_from_orc(self):
        camera_timestamp = datetime(2024, 1, 2, 3, 4, 5, 123000)
        action_timestamp = datetime(2024, 1, 2, 3, 4, 5, 678000)
        physical = pa.StructArray.from_arrays(
            [
                pa.array([[0, -1]], type=pa.list_(pa.int32())),
                pa.array([camera_timestamp], type=pa.timestamp("ns")),
                pa.array([None], type=pa.timestamp("ns")),
                pa.array(
                    [[(2, action_timestamp)]],
                    type=pa.map_(pa.int32(), pa.timestamp("ns")),
                ),
            ],
            names=["__field_mapping", "__col_0", "__col_1", "__overflow"],
        )
        path = os.path.join(self.tmp, "timestamp.orc")
        orc.write_table(pa.table({"content_refs": physical}), path)

        result = self._read_orc_shared_map(
            path, AtomicType("TIMESTAMP(3)"))

        self.assertEqual(
            pa.map_(pa.string(), pa.timestamp("ms")), result.type)
        self.assertEqual(
            [[("camera", camera_timestamp), ("action", action_timestamp)]],
            result.to_pylist(),
        )

    def test_restores_nested_timestamp_values_from_orc(self):
        camera_timestamp = datetime(2024, 1, 2, 3, 4, 5, 123000)
        history_timestamp = datetime(
            2024, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc)
        physical_value_type = pa.struct([
            pa.field("captured_at", pa.timestamp("ns")),
            pa.field("history", pa.list_(pa.timestamp("ns", tz="UTC"))),
        ])
        physical = pa.StructArray.from_arrays(
            [
                pa.array([[0, -1]], type=pa.list_(pa.int32())),
                pa.array(
                    [{
                        "captured_at": camera_timestamp,
                        "history": [history_timestamp],
                    }],
                    type=physical_value_type,
                ),
                pa.array([None], type=physical_value_type),
                pa.array(
                    [[]], type=pa.map_(pa.int32(), physical_value_type)),
            ],
            names=["__field_mapping", "__col_0", "__col_1", "__overflow"],
        )
        path = os.path.join(self.tmp, "nested-timestamp.orc")
        orc.write_table(pa.table({"content_refs": physical}), path)
        logical_value_type = RowType(True, [
            DataField(1, "captured_at", AtomicType("TIMESTAMP(3)")),
            DataField(
                2,
                "history",
                ArrayType(True, AtomicType("TIMESTAMP_LTZ(6)")),
            ),
        ])

        result = self._read_orc_shared_map(path, logical_value_type)

        self.assertEqual(
            pa.struct([
                pa.field("captured_at", pa.timestamp("ms")),
                pa.field(
                    "history", pa.list_(pa.timestamp("us", tz="UTC"))),
            ]),
            result.type.item_type,
        )
        self.assertEqual(
            [[("camera", {
                "captured_at": camera_timestamp,
                "history": [history_timestamp],
            })]],
            result.to_pylist(),
        )

    def _read_orc_shared_map(self, path, value_type):
        physical_field = orc.ORCFile(path).schema.field("content_refs")
        metadata_field = pa.field(
            "content_refs", physical_field.type, metadata=_metadata("none"))
        arrow_schema = base64.b64encode(
            pa.schema([metadata_field]).serialize().to_pybytes())
        metadata = mock.Mock()
        metadata.get.side_effect = lambda key: (
            arrow_schema if key in (b"ARROW:schema", "ARROW:schema") else None)

        with mock.patch(
                "pyarrow.orc.ORCFile",
                return_value=mock.Mock(metadata=metadata)):
            reader = FormatPyArrowReader(
                _LocalFileIO(), "orc", path,
                [DataField(
                    0,
                    "content_refs",
                    MapType(
                        True,
                        AtomicType("STRING", False),
                        value_type,
                    ),
                )],
                None,
            )
            return reader.read_arrow_batch().column(0)

    def test_restores_map_with_nested_projection_alias(self):
        physical = pa.StructArray.from_arrays(
            [
                pa.array([[0, -1]], type=pa.list_(pa.int32())),
                pa.array([10], type=pa.int64()),
                pa.array([None], type=pa.int64()),
                pa.array([[]], type=pa.map_(pa.int32(), pa.int64())),
            ],
            names=["__field_mapping", "__col_0", "__col_1", "__overflow"],
        )
        nested = pa.StructArray.from_arrays(
            [pa.array([7], type=pa.int64())], names=["b"])
        path = os.path.join(self.tmp, "nested-alias.parquet")
        pq.write_table(
            pa.Table.from_arrays(
                [nested, physical],
                schema=pa.schema([
                    pa.field("a", nested.type),
                    pa.field(
                        "a_b", physical.type, metadata=_metadata("none")),
                ]),
            ),
            path,
        )

        reader = FormatPyArrowReader(
            _LocalFileIO(),
            "parquet",
            path,
            [
                DataField(1, "a_b", AtomicType("BIGINT")),
                DataField(
                    2,
                    "a_b__0",
                    MapType(
                        True,
                        AtomicType("STRING", False),
                        AtomicType("BIGINT"),
                    ),
                ),
            ],
            None,
            nested_name_paths=[["a", "b"], ["a_b"]],
        )
        batch = reader.read_arrow_batch()

        self.assertEqual(["a_b", "a_b__0"], batch.schema.names)
        self.assertEqual([7], batch.column(0).to_pylist())
        self.assertEqual(
            [[("camera", 10)]], batch.column(1).to_pylist())

    def test_leaves_normal_map_unchanged(self):
        path = os.path.join(self.tmp, "normal.parquet")
        pq.write_table(
            pa.table({"content_refs": pa.array(
                [[("camera", 1)]], type=pa.map_(pa.string(), pa.int64()))}),
            path,
        )
        reader = FormatPyArrowReader(
            _LocalFileIO(), "parquet", path,
            [DataField(
                0, "content_refs",
                MapType(
                    True,
                    AtomicType("STRING", False),
                    AtomicType("BIGINT")))],
            None,
        )
        self.assertEqual(
            [[("camera", 1)]], reader.read_arrow_batch().column(0).to_pylist())


if __name__ == "__main__":
    unittest.main()
