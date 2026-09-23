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
from typing import Dict, List, Optional
from unittest import mock

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.orc as orc
import pyarrow.parquet as pq

from pypaimon.data import map_shared_shredding
from pypaimon.data.map_shared_shredding import (
    assemble_shared_shredding_selected_keys,
    map_selected_keys_field,
)
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
        {"first": 0, "second": 1, "third": 2},
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
        "paimon.map.shared-shredding.field-columns": json.dumps({
            "0": [0], "1": [0, 1], "2": []}),
        "paimon.map.shared-shredding.overflow-set": "[2]",
        "paimon.map.shared-shredding.num-columns": "2",
    }


class SharedShreddingMapReaderTest(unittest.TestCase):
    def test_selected_keys_preserve_sliced_mapping_and_overflow(self):
        # Exercise both sides of the batch/candidate limits with the same
        # sliced data, including null parents, moving keys and duplicate overflow.
        # Keep explicit boundaries to catch accidental changes to the policy.
        cases = [
            (pa.string(), 6, 2, False),
            (pa.string(), 1023, 8, False),
            (pa.string(), 1024, 8, True),
            (pa.string(), 1025, 8, True),
            (pa.string(), 1024, 9, False),
            (pa.struct([("v", pa.int64())]), 1024, 8, False),
            (pa.int64(), 1024, 8, True),
            (pa.float64(), 1024, 8, True),
            (pa.bool_(), 1024, 8, True),
            (pa.binary(), 1024, 8, True),
        ]
        for value_type, rows, width, vectorized in cases:
            with self.subTest(value_type=value_type, rows=rows, width=width):

                def value(number: int):
                    if pa.types.is_string(value_type):
                        return str(number)
                    if pa.types.is_binary(value_type):
                        return number.to_bytes(2, "little")
                    if pa.types.is_integer(value_type):
                        return number
                    if pa.types.is_floating(value_type):
                        return number / 8.0
                    if pa.types.is_boolean(value_type):
                        return bool(number % 2)
                    return {"v": number}

                mappings = [[0, 1], [], [-1, -1], [0, 1], [-1, -1], [1, 0]]
                mappings = [m + [-1] * (width - 2) if m else [] for m in mappings]
                repeats = (rows + 5) // 6
                # Prefix is excluded by the slice but shifts both child offsets.
                mapping = [[0, 1] + [-1] * (width - 2)] + mappings * repeats
                first = [value(99)] + [
                    value(1),
                    None,
                    None,
                    None,
                    None,
                    value(15),
                ] * repeats
                second = [value(199)] + [
                    value(11),
                    None,
                    None,
                    None,
                    None,
                    value(5),
                ] * repeats
                overflow = [[(0, value(999))]] + [
                    [],
                    [],
                    [(0, value(2)), (0, value(3)), (1, value(12))],
                    [(0, value(4)), (1, value(14))],
                    [],
                    None,
                ] * repeats
                physical = pa.StructArray.from_arrays(
                    [
                        pa.array(mapping, type=pa.list_(pa.int32())),
                        pa.array(first, type=value_type),
                        pa.array(second, type=value_type),
                    ]
                    + [
                        pa.nulls(len(mapping), type=value_type)
                        for _ in range(width - 2)
                    ]
                    + [pa.array(overflow, type=pa.map_(pa.int32(), value_type))],
                    names=["__field_mapping"]
                    + ["__col_%d" % i for i in range(width)]
                    + ["__overflow"],
                    mask=pa.array(
                        [False] + [False, True, False, False, False, False] * repeats
                    ),
                )
                expected = [
                    {"k": value(1), "other": value(11), "missing": None},
                    None,
                    {"k": value(2), "other": value(12), "missing": None},
                    {"k": None, "other": None, "missing": None},
                    {"k": None, "other": None, "missing": None},
                    {"k": value(5), "other": value(15), "missing": None},
                ] * repeats
                with mock.patch.object(
                    map_shared_shredding.pc,
                    "list_value_length",
                    wraps=map_shared_shredding.pc.list_value_length,
                ) as vectorized_lengths:
                    selected = assemble_shared_shredding_selected_keys(
                        physical.slice(1, rows),
                        ["k", "other", "missing"],
                        value_type,
                        (
                            {0: "k", 1: "other"},
                            {0: list(range(width)), 1: list(range(min(width, 8)))},
                            {0, 1},
                            width,
                        ),
                    )
                    self.assertEqual(vectorized_lengths.call_count, int(vectorized))
                expected_array = pa.array(expected[:rows], type=selected.type)
                assert isinstance(expected_array, pa.StructArray)
                self.assertTrue(selected.equals(expected_array))

    def test_selected_keys_mapping_boundaries(self):
        for rows in (1023, 1024):
            for case in (
                "tail_null", "null_id", "short_mapping", "unknown_key",
                "missing_column", "fixed_size_list", "large_list",
            ):
                with self.subTest(rows=rows, case=case):
                    mappings: List[List[Optional[int]]] = [[0, 1] for _ in range(rows + 1)]
                    mask = [False] * (rows + 1)
                    expected: List[Optional[Dict[str, Optional[str]]]] = [
                        {"k": str(i)} for i in range(1, rows + 1)
                    ]
                    selected_keys = ["k"]
                    mapping_type = pa.list_(pa.int32())
                    if case == "tail_null":
                        mappings[-1] = []
                        mask[-1] = True
                        expected[-1] = None
                    elif case == "null_id":
                        # A later null candidate must not erase an earlier match.
                        mappings[7] = [0, None]
                    elif case in ("short_mapping", "unknown_key"):
                        mappings[7] = []
                        if case == "unknown_key":
                            selected_keys = ["missing"]
                            expected = [{"missing": None}] * rows
                    elif case == "missing_column":
                        mappings[7] = [1, 0]
                    elif case == "fixed_size_list":
                        mapping_type = pa.list_(pa.int32(), 2)
                    elif case == "large_list":
                        mapping_type = pa.large_list(pa.int32())
                    arrays = [
                        pa.array(mappings, type=mapping_type),
                        pa.array([str(i) for i in range(rows + 1)]),
                    ]
                    names = ["__field_mapping", "__col_0"]
                    if case != "missing_column":
                        arrays.append(pa.nulls(rows + 1, type=pa.string()))
                        names.append("__col_1")
                    physical = pa.StructArray.from_arrays(
                        arrays, names=names, mask=pa.array(mask)
                    ).slice(1)
                    metadata = ({0: "k", 1: "other"}, {0: [0, 1]}, set(), 2)
                    if case in ("short_mapping", "missing_column"):
                        message = (
                            "field mapping length must equal 2"
                            if case == "short_mapping"
                            else "Missing shared-shredding physical column 1"
                        )
                        with self.assertRaisesRegex(ValueError, message):
                            assemble_shared_shredding_selected_keys(
                                physical, selected_keys, pa.string(), metadata
                            )
                    else:
                        selected = assemble_shared_shredding_selected_keys(
                            physical, selected_keys, pa.string(), metadata
                        )
                        expected_array = pa.array(expected, type=selected.type)
                        assert isinstance(expected_array, pa.StructArray)
                        self.assertTrue(selected.equals(expected_array))

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.value_arrow_type = pa.struct([
            pa.field("value_x", pa.int64()),
            pa.field("value_y", pa.int64()),
        ])
        self.value_type = RowType(True, [
            DataField(1, "value_x", AtomicType("BIGINT")),
            DataField(2, "value_y", AtomicType("BIGINT")),
        ])

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write(self, compression, file_format):
        values0 = pa.array([
            {"value_x": 10, "value_y": 100},
            {"value_x": 40, "value_y": 400},
            None,
            None,
        ], type=self.value_arrow_type)
        values1 = pa.array([
            {"value_x": 20, "value_y": 200},
            None,
            None,
            None,
        ], type=self.value_arrow_type)
        overflow = pa.array([
            [(2, {"value_x": 30, "value_y": 300})],
            [(99, {"value_x": 50, "value_y": 500})],
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
            "attributes", physical.type, metadata=_metadata(compression))
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
                ("first", {"value_x": 10, "value_y": 100}),
                ("second", {"value_x": 20, "value_y": 200}),
                ("third", {"value_x": 30, "value_y": 300}),
            ],
            [("second", {"value_x": 40, "value_y": 400})],
            None,
            [],
        ]
        for compression in ("none", "lz4", "zstd"):
            with self.subTest(compression=compression):
                self._assert_complete_map("parquet", compression, expected)

    def test_reads_only_selected_shared_shredding_keys(self):
        map_field = DataField(
            0,
            "attributes",
            MapType(
                True, AtomicType("STRING", False), self.value_type),
        )
        selected_field = map_selected_keys_field(
            map_field, ["first", "third", "missing"])
        reader = FormatPyArrowReader(
            _LocalFileIO(), "parquet", self._write("none", "parquet"),
            [selected_field], None, batch_size=2,
        )

        self.assertEqual(
            [
                "attributes.__field_mapping",
                "attributes.__col_0",
                "attributes.__overflow",
            ],
            reader._row_group_read_columns(),
        )
        actual = []
        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break
            actual.extend(batch.column(0).to_pylist())
        self.assertEqual([
            {
                "first": {"value_x": 10, "value_y": 100},
                "third": {"value_x": 30, "value_y": 300},
                "missing": None,
            },
            {"first": None, "third": None, "missing": None},
            None,
            {"first": None, "third": None, "missing": None},
        ], actual)

    def test_reads_unknown_key_without_value_columns(self):
        map_field = DataField(
            0,
            "attributes",
            MapType(
                True, AtomicType("STRING", False), self.value_type),
        )
        reader = FormatPyArrowReader(
            _LocalFileIO(),
            "parquet",
            self._write("none", "parquet"),
            [map_selected_keys_field(map_field, ["missing"])],
            None,
        )

        self.assertEqual(
            ["attributes.__field_mapping"],
            reader._row_group_read_columns(),
        )
        actual = []
        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break
            actual.extend(batch.column(0).to_pylist())
        self.assertEqual([
            {"missing": None}, {"missing": None}, None, {"missing": None},
        ], actual)

    def test_reads_only_selected_shared_shredding_keys_from_orc(self):
        path = self._write("none", "orc")
        physical_schema = orc.ORCFile(path).schema
        fields = list(physical_schema)
        index = physical_schema.get_field_index("attributes")
        fields[index] = pa.field(
            "attributes", fields[index].type, metadata=_metadata("none"))
        map_field = DataField(
            0,
            "attributes",
            MapType(
                True, AtomicType("STRING", False), self.value_type),
        )
        with mock.patch(
                "pypaimon.read.reader.format_pyarrow_reader."
                "_orc_schema_with_field_metadata",
                return_value=pa.schema(fields)):
            reader = FormatPyArrowReader(
                _LocalFileIO(), "orc", path,
                [map_selected_keys_field(
                    map_field, ["first", "third", "missing"])],
                None,
                batch_size=2,
            )
            actual = []
            while True:
                batch = reader.read_arrow_batch()
                if batch is None:
                    break
                actual.extend(batch.column(0).to_pylist())

        self.assertEqual([
            {
                "first": {"value_x": 10, "value_y": 100},
                "third": {"value_x": 30, "value_y": 300},
                "missing": None,
            },
            {"first": None, "third": None, "missing": None},
            None,
            {"first": None, "third": None, "missing": None},
        ], actual)

    def test_reads_selected_keys_from_normal_map(self):
        map_type = pa.map_(pa.string(), self.value_arrow_type)
        path = os.path.join(self.tmp, "normal.parquet")
        pq.write_table(pa.table({
            "attributes": pa.array([
                [("first", {"value_x": 1, "value_y": 10})],
                [],
                None,
            ], type=map_type),
        }), path)
        map_field = DataField(
            0,
            "attributes",
            MapType(
                True, AtomicType("STRING", False), self.value_type),
        )
        reader = FormatPyArrowReader(
            _LocalFileIO(), "parquet", path,
            [map_selected_keys_field(map_field, ["first", "missing"])],
            None,
        )

        batch = reader.read_arrow_batch()
        self.assertEqual([
            {
                "first": {"value_x": 1, "value_y": 10},
                "missing": None,
            },
            {"first": None, "missing": None},
            None,
        ], batch.column(0).to_pylist())

    def _assert_complete_map(
            self, file_format, compression, expected, path=None):
        reader = FormatPyArrowReader(
            _LocalFileIO(), file_format,
            path or self._write(compression, file_format),
            [DataField(
                0,
                "attributes",
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
        index = physical_schema.get_field_index("attributes")
        fields[index] = pa.field(
            "attributes", fields[index].type, metadata=_metadata("none"))
        arrow_schema = base64.b64encode(
            pa.schema(fields).serialize().to_pybytes())

        expected = [
            [
                ("first", {"value_x": 10, "value_y": 100}),
                ("second", {"value_x": 20, "value_y": 200}),
                ("third", {"value_x": 30, "value_y": 300}),
            ],
            [("second", {"value_x": 40, "value_y": 400})],
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
        orc.write_table(pa.table({"attributes": physical}), path)

        result = self._read_orc_shared_map(path, AtomicType("TIME(3)"))

        self.assertEqual(pa.map_(pa.string(), pa.time32("ms")), result.type)
        self.assertEqual(
            [[("first", time(0, 0, 1, 234000)),
              ("third", time(0, 0, 5, 678000))]],
            result.to_pylist(),
        )

    def test_restores_selected_time_values_from_orc(self):
        physical = pa.StructArray.from_arrays(
            [
                pa.array([[0, -1]], type=pa.list_(pa.int32())),
                pa.array([1234], type=pa.int32()),
                pa.array([None], type=pa.int32()),
                pa.array(
                    [[(2, 5678)]], type=pa.map_(pa.int32(), pa.int32())),
            ],
            names=["__field_mapping", "__col_0", "__col_1", "__overflow"],
        )
        path = os.path.join(self.tmp, "selected-time.orc")
        orc.write_table(pa.table({"attributes": physical}), path)
        physical_field = orc.ORCFile(path).schema.field("attributes")
        metadata_schema = pa.schema([pa.field(
            "attributes",
            physical_field.type,
            metadata=_metadata("none"),
        )])
        map_field = DataField(
            0,
            "attributes",
            MapType(
                True,
                AtomicType("STRING", False),
                AtomicType("TIME(3)"),
            ),
        )

        with mock.patch(
                "pypaimon.read.reader.format_pyarrow_reader."
                "_orc_schema_with_field_metadata",
                return_value=metadata_schema):
            reader = FormatPyArrowReader(
                _LocalFileIO(),
                "orc",
                path,
                [map_selected_keys_field(
                    map_field, ["first", "third"])],
                None,
            )
            result = reader.read_arrow_batch().column(0)

        self.assertEqual(
            pa.struct([
                pa.field("first", pa.time32("ms")),
                pa.field("third", pa.time32("ms")),
            ]),
            result.type,
        )
        self.assertEqual([{
            "first": time(0, 0, 1, 234000),
            "third": time(0, 0, 5, 678000),
        }], result.to_pylist())

    def test_restores_timestamp_precision_from_orc(self):
        first_timestamp = datetime(2024, 1, 2, 3, 4, 5, 123000)
        third_timestamp = datetime(2024, 1, 2, 3, 4, 5, 678000)
        physical = pa.StructArray.from_arrays(
            [
                pa.array([[0, -1]], type=pa.list_(pa.int32())),
                pa.array([first_timestamp], type=pa.timestamp("ns")),
                pa.array([None], type=pa.timestamp("ns")),
                pa.array(
                    [[(2, third_timestamp)]],
                    type=pa.map_(pa.int32(), pa.timestamp("ns")),
                ),
            ],
            names=["__field_mapping", "__col_0", "__col_1", "__overflow"],
        )
        path = os.path.join(self.tmp, "timestamp.orc")
        orc.write_table(pa.table({"attributes": physical}), path)

        result = self._read_orc_shared_map(
            path, AtomicType("TIMESTAMP(3)"))

        self.assertEqual(
            pa.map_(pa.string(), pa.timestamp("ms")), result.type)
        self.assertEqual(
            [[("first", first_timestamp), ("third", third_timestamp)]],
            result.to_pylist(),
        )

    def test_restores_nested_timestamp_values_from_orc(self):
        first_timestamp = datetime(2024, 1, 2, 3, 4, 5, 123000)
        history_timestamp = datetime(
            2024, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc)
        physical_value_type = pa.struct([
            pa.field("event_time", pa.timestamp("ns")),
            pa.field("history", pa.list_(pa.timestamp("ns", tz="UTC"))),
        ])
        physical = pa.StructArray.from_arrays(
            [
                pa.array([[0, -1]], type=pa.list_(pa.int32())),
                pa.array(
                    [{
                        "event_time": first_timestamp,
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
        orc.write_table(pa.table({"attributes": physical}), path)
        logical_value_type = RowType(True, [
            DataField(1, "event_time", AtomicType("TIMESTAMP(3)")),
            DataField(
                2,
                "history",
                ArrayType(True, AtomicType("TIMESTAMP_LTZ(6)")),
            ),
        ])

        result = self._read_orc_shared_map(path, logical_value_type)

        self.assertEqual(
            pa.struct([
                pa.field("event_time", pa.timestamp("ms")),
                pa.field(
                    "history", pa.list_(pa.timestamp("us", tz="UTC"))),
            ]),
            result.type.item_type,
        )
        self.assertEqual(
            [[("first", {
                "event_time": first_timestamp,
                "history": [history_timestamp],
            })]],
            result.to_pylist(),
        )

    def _read_orc_shared_map(self, path, value_type):
        physical_field = orc.ORCFile(path).schema.field("attributes")
        metadata_field = pa.field(
            "attributes", physical_field.type, metadata=_metadata("none"))
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
                    "attributes",
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
            [[("first", 10)]], batch.column(1).to_pylist())

    def test_leaves_normal_map_unchanged(self):
        path = os.path.join(self.tmp, "normal.parquet")
        pq.write_table(
            pa.table({"attributes": pa.array(
                [[("first", 1)]], type=pa.map_(pa.string(), pa.int64()))}),
            path,
        )
        reader = FormatPyArrowReader(
            _LocalFileIO(), "parquet", path,
            [DataField(
                0, "attributes",
                MapType(
                    True,
                    AtomicType("STRING", False),
                    AtomicType("BIGINT")))],
            None,
        )
        self.assertEqual(
            [[("first", 1)]], reader.read_arrow_batch().column(0).to_pylist())


if __name__ == "__main__":
    unittest.main()
