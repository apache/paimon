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

import inspect
import json
import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from pypaimon.data.generic_variant import GenericVariant
from pypaimon.data.variant_shredding import (
    parse_shredding_schema_option,
    shredding_schema_to_arrow_type,
    shred_variant_column,
)
from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.schema.data_types import (
    ArrayType,
    AtomicType,
    DataField,
    MapType,
    RowType,
)

_VARIANT_TYPE = pa.struct([
    pa.field("value", pa.binary(), nullable=False),
    pa.field("metadata", pa.binary(), nullable=False),
])


class _LocalFileIO:
    filesystem = pafs.LocalFileSystem()

    def to_filesystem_path(self, path):
        return path


def _drain(reader):
    rows = 0
    columns = None
    content_keys = set()
    while True:
        batch = reader.read_arrow_batch()
        if batch is None:
            break
        rows += batch.num_rows
        columns = batch.schema.names
        if "content_key" in columns:
            content_keys |= set(
                batch.column(columns.index("content_key")).to_pylist())
    return rows, columns, content_keys


class VariantRowGroupReaderTest(unittest.TestCase):

    def setUp(self):
        self.n = 2000
        content_key = [
            "robot_pose_raw" if i % 2 == 0 else "imu_raw"
            for i in range(self.n)
        ]
        payload = [
            {"value": b"v%d" % i, "metadata": b"m"}
            for i in range(self.n)
        ]
        table = pa.table({
            "content_key": pa.array(content_key),
            "payload": pa.array(payload, type=_VARIANT_TYPE),
        })
        self.tmp = tempfile.mkdtemp()
        self.path = os.path.join(self.tmp, "topics.parquet")
        pq.write_table(table, self.path, row_group_size=1000)
        self.assertEqual(2, pq.ParquetFile(self.path).num_row_groups)
        self.read_fields = [
            DataField(0, "content_key", AtomicType("STRING")),
            DataField(1, "payload", AtomicType("VARIANT")),
        ]

    def _reader(self, read_fields, predicate=None, predicate_field_names=None):
        return FormatPyArrowReader(
            _LocalFileIO(), "parquet", self.path, read_fields,
            predicate, batch_size=256,
            predicate_field_names=predicate_field_names)

    def _large_dictionary_payload(self):
        if "store_schema" not in inspect.signature(pq.write_table).parameters:
            self.skipTest("PyArrow does not support store_schema")
        row_count = 15_340
        values = pa.DictionaryArray.from_arrays(
            pa.array([0] * row_count, type=pa.int32()),
            pa.array([b"x" * 140_000], type=pa.binary()))
        payload = pa.StructArray.from_arrays(
            [values, pa.array([b"m"] * row_count)],
            names=["value", "metadata"])
        return row_count, payload

    def _large_dictionary_variant(self):
        row_count, payload = self._large_dictionary_payload()
        path = os.path.join(self.tmp, "large-dictionary-variant.parquet")
        pq.write_table(
            pa.table({"payload": payload}), path,
            use_dictionary=True, compression="zstd", store_schema=False)
        reader = FormatPyArrowReader(
            _LocalFileIO(), "parquet", path,
            [DataField(0, "payload", AtomicType("VARIANT"))],
            None, batch_size=128)
        return pq.ParquetFile(path).num_row_groups, _drain(reader)

    def _shredded_variant_payload(self):
        schema = json.dumps({
            "type": "ROW",
            "fields": [{
                "id": 0,
                "name": "v",
                "type": {
                    "type": "ROW",
                    "fields": [{
                        "id": 1,
                        "name": "age",
                        "type": "BIGINT",
                    }],
                },
            }],
        })
        obj_fields = parse_shredding_schema_option(schema)["v"]
        target_type = shredding_schema_to_arrow_type(obj_fields)
        variants = GenericVariant.to_arrow_array([
            GenericVariant.from_python({"age": 1, "extra": "x"}),
            None,
            GenericVariant.from_python({"age": 3, "extra": "z"}),
        ])
        return shred_variant_column(variants, obj_fields, target_type)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_filter_only_column_not_in_projection(self):
        reader = self._reader(
            [DataField(1, "payload", AtomicType("VARIANT"))],
            predicate=ds.field("content_key") == "robot_pose_raw",
            predicate_field_names={"content_key"})
        rows, columns, _ = _drain(reader)
        self.assertEqual(self.n // 2, rows)
        self.assertEqual(["payload"], columns)

    def test_reads_all_rows_across_row_groups(self):
        rows, columns, _ = _drain(self._reader(self.read_fields))
        self.assertEqual(self.n, rows)
        self.assertEqual(["content_key", "payload"], columns)

    def test_reads_large_dictionary_variant_in_single_row_group(self):
        row_groups, (rows, columns, _) = self._large_dictionary_variant()
        self.assertEqual(1, row_groups)
        self.assertEqual(15_340, rows)
        self.assertEqual(["payload"], columns)

    def test_predicate_returns_only_matching_rows(self):
        predicate = ds.field("content_key") == "robot_pose_raw"
        rows, _, content_keys = _drain(
            self._reader(self.read_fields, predicate))
        self.assertEqual(self.n // 2, rows)
        self.assertEqual({"robot_pose_raw"}, content_keys)

    def test_projection_returns_only_requested_columns(self):
        rows, columns, _ = _drain(
            self._reader([DataField(1, "payload", AtomicType("VARIANT"))]))
        self.assertEqual(self.n, rows)
        self.assertEqual(["payload"], columns)

    def test_projection_preserves_requested_order(self):
        reader = self._reader([
            DataField(1, "payload", AtomicType("VARIANT")),
            DataField(0, "content_key", AtomicType("STRING")),
        ])
        batch = reader.read_arrow_batch()
        self.assertEqual(["payload", "content_key"], batch.schema.names)
        self.assertEqual(
            {"value": b"v0", "metadata": b"m"}, batch.column(0)[0].as_py())
        self.assertEqual("robot_pose_raw", batch.column(1)[0].as_py())

    def test_dotted_top_level_name_does_not_match_nested_path(self):
        path = os.path.join(self.tmp, "dotted-name.parquet")
        pq.write_table(pa.table({
            "a": pa.array(
                [{"b": "nested"}],
                type=pa.struct([pa.field("b", pa.string())])),
            "a.b": pa.array(["top"]),
            "payload": pa.array(
                [{"value": b"v", "metadata": b"m"}], type=_VARIANT_TYPE),
        }), path)
        reader = FormatPyArrowReader(
            _LocalFileIO(), "parquet", path,
            [
                DataField(1, "a.b", AtomicType("STRING")),
                DataField(2, "payload", AtomicType("VARIANT")),
            ],
            None, batch_size=128)
        batch = reader.read_arrow_batch()
        self.assertEqual(["a.b", "payload"], batch.schema.names)
        self.assertEqual("top", batch.column(0)[0].as_py())
        self.assertEqual(
            {"value": b"v", "metadata": b"m"}, batch.column(1)[0].as_py())

    def test_reads_variant_nested_in_container_types(self):
        row_count, payload = self._large_dictionary_payload()
        offsets = pa.array(range(row_count + 1), type=pa.int32())
        variant_type = AtomicType("VARIANT")
        cases = [
            (
                "row",
                pa.StructArray.from_arrays([payload], names=["v"]),
                RowType(True, [DataField(1, "v", variant_type)]),
            ),
            (
                "array",
                pa.ListArray.from_arrays(offsets, payload),
                ArrayType(True, variant_type),
            ),
            (
                "map",
                pa.MapArray.from_arrays(
                    offsets, pa.array(["k"] * row_count), payload),
                MapType(True, AtomicType("STRING", False), variant_type),
            ),
        ]
        for name, column, data_type in cases:
            with self.subTest(name=name):
                path = os.path.join(self.tmp, "nested-{}.parquet".format(name))
                pq.write_table(
                    pa.table({name: column}), path,
                    use_dictionary=True, compression="zstd",
                    store_schema=False)
                reader = FormatPyArrowReader(
                    _LocalFileIO(), "parquet", path,
                    [DataField(0, name, data_type)], None, batch_size=128)
                rows, columns, _ = _drain(reader)
                self.assertEqual(row_count, rows)
                self.assertEqual([name], columns)

    def test_nested_variant_paths_use_bounded_reader(self):
        path = os.path.join(self.tmp, "small-nested-variant.parquet")
        payload = pa.array([
            {"value": b"v0", "metadata": b"m"},
            {"value": b"v1", "metadata": b"m"},
            {"value": b"v2", "metadata": b"m"},
        ], type=_VARIANT_TYPE)
        offsets = pa.array([0, 1, 2, 3], type=pa.int32())
        pq.write_table(pa.table({
            "row": pa.StructArray.from_arrays(
                [payload], names=["v"],
                mask=pa.array([False, False, True])),
            "array": pa.ListArray.from_arrays(offsets, payload),
            "map": pa.MapArray.from_arrays(
                offsets, pa.array(["k", "k", "k"]), payload),
            "kind": pa.array(["keep", "drop", "keep"]),
        }), path)

        variant_type = AtomicType("VARIANT")
        cases = [
            ("row", RowType(True, [DataField(1, "v", variant_type)])),
            ("array", ArrayType(True, variant_type)),
            ("map", MapType(
                True, AtomicType("STRING", False), variant_type)),
        ]
        for name, data_type in cases:
            reader = FormatPyArrowReader(
                _LocalFileIO(), "parquet", path,
                [DataField(0, name, data_type)], None, batch_size=128)
            self.assertIsNotNone(reader._parquet_file)
            self.assertEqual(3, _drain(reader)[0])

        nested_reader = FormatPyArrowReader(
            _LocalFileIO(), "parquet", path,
            [DataField(1, "row_v", AtomicType("VARIANT"))],
            ds.field("kind") == "keep", batch_size=128,
            nested_name_paths=[["row", "v"]],
            predicate_field_names={"kind"})
        self.assertIsNotNone(nested_reader._parquet_file)
        batch = nested_reader.read_arrow_batch()
        self.assertEqual(["row_v"], batch.schema.names)
        self.assertEqual([
            {"value": b"v0", "metadata": b"m"}, None,
        ], batch.column(0).to_pylist())

    def test_reads_nested_variant_projection(self):
        row_count, payload = self._large_dictionary_payload()
        path = os.path.join(self.tmp, "nested-projection.parquet")
        pq.write_table(
            pa.table({
                "row": pa.StructArray.from_arrays([payload], names=["v"]),
                "kind": pa.array(
                    ["keep" if i % 2 == 0 else "drop"
                     for i in range(row_count)]),
            }),
            path, use_dictionary=True, compression="zstd", store_schema=False)
        reader = FormatPyArrowReader(
            _LocalFileIO(), "parquet", path,
            [DataField(0, "row_v", AtomicType("VARIANT"))],
            ds.field("kind") == "keep", batch_size=128,
            nested_name_paths=[["row", "v"]],
            predicate_field_names={"kind"})
        rows, columns, _ = _drain(reader)
        self.assertEqual((row_count + 1) // 2, rows)
        self.assertEqual(["row_v"], columns)

    def test_assembles_shredded_variant_in_nested_types(self):
        shredded = self._shredded_variant_payload()
        values = shredded.to_pylist()
        path = os.path.join(self.tmp, "nested-shredded-variant.parquet")
        pq.write_table(pa.table({
            "row": pa.array(
                [{"v": values[0]}, {"v": values[1]}, None, {"v": values[2]}],
                type=pa.struct([pa.field("v", shredded.type)])),
            "array": pa.array(
                [[values[0]], [values[1]], None, [values[2]]],
                type=pa.list_(pa.field("item", shredded.type))),
            "map": pa.array(
                [[("k", values[0])], [("k", values[1])], None,
                 [("k", values[2])]],
                type=pa.map_(
                    pa.field("key", pa.string(), nullable=False),
                    pa.field("value", shredded.type))),
            "kind": pa.array(["keep", "drop", "keep", "keep"]),
        }), path)

        variant_type = AtomicType("VARIANT")
        data_types = {
            "row": RowType(True, [DataField(1, "v", variant_type)]),
            "array": ArrayType(True, variant_type),
            "map": MapType(
                True, AtomicType("STRING", False), variant_type),
        }
        outputs = {}
        for name, data_type in data_types.items():
            reader = FormatPyArrowReader(
                _LocalFileIO(), "parquet", path,
                [DataField(0, name, data_type)], None, batch_size=128)
            outputs[name] = reader.read_arrow_batch().column(0)

        self.assertEqual(_VARIANT_TYPE, outputs["row"].type[0].type)
        self.assertEqual(_VARIANT_TYPE, outputs["array"].type.value_type)
        self.assertEqual(_VARIANT_TYPE, outputs["map"].type.item_type)

        def decode(value):
            if value is None:
                return None
            return GenericVariant.from_arrow_struct(value).to_python()

        row_values = outputs["row"].to_pylist()
        self.assertEqual({"age": 1, "extra": "x"}, decode(row_values[0]["v"]))
        self.assertIsNone(row_values[1]["v"])
        self.assertIsNone(row_values[2])

        array_values = outputs["array"].to_pylist()
        self.assertEqual({"age": 1, "extra": "x"}, decode(array_values[0][0]))
        self.assertIsNone(array_values[1][0])
        self.assertIsNone(array_values[2])

        map_values = outputs["map"].to_pylist()
        self.assertEqual(
            {"age": 1, "extra": "x"}, decode(map_values[0][0][1]))
        self.assertIsNone(map_values[1][0][1])
        self.assertIsNone(map_values[2])

        nested_reader = FormatPyArrowReader(
            _LocalFileIO(), "parquet", path,
            [DataField(1, "row_v", AtomicType("VARIANT"))],
            ds.field("kind") == "keep", batch_size=128,
            nested_name_paths=[["row", "v"]],
            predicate_field_names={"kind"})
        nested = nested_reader.read_arrow_batch().column(0)
        self.assertEqual(_VARIANT_TYPE, nested.type)
        nested_values = nested.to_pylist()
        self.assertEqual({"age": 1, "extra": "x"}, decode(nested_values[0]))
        self.assertIsNone(nested_values[1])
        self.assertEqual({"age": 3, "extra": "z"}, decode(nested_values[2]))

    def test_single_row_group_scalar_read_uses_fast_path(self):
        rows, columns, _ = _drain(
            self._reader([DataField(0, "content_key", AtomicType("STRING"))]))
        self.assertEqual(self.n, rows)
        self.assertEqual(["content_key"], columns)

    def test_row_group_pruning_by_statistics(self):
        rows_per_group = 1000
        content_key = []
        for group in range(8):
            content_key += (
                ["match" if group == 3 else "other"] * rows_per_group)
        total = len(content_key)
        payload = [
            {"value": b"v%d" % i, "metadata": b"m"}
            for i in range(total)
        ]
        path = os.path.join(self.tmp, "clustered.parquet")
        pq.write_table(
            pa.table({
                "content_key": pa.array(content_key),
                "payload": pa.array(payload, type=_VARIANT_TYPE),
            }),
            path, row_group_size=rows_per_group)
        self.assertEqual(8, pq.ParquetFile(path).num_row_groups)

        reader = FormatPyArrowReader(
            _LocalFileIO(), "parquet", path, self.read_fields,
            ds.field("content_key") == "match", batch_size=512)
        self.assertEqual([3], list(reader._surviving_row_group_ids()))
        rows, _, keys = _drain(reader)
        self.assertEqual(rows_per_group, rows)
        self.assertEqual({"match"}, keys)


if __name__ == "__main__":
    unittest.main()
