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
import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.schema.data_types import AtomicType, DataField

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

    def _large_dictionary_variant(self):
        if "store_schema" not in inspect.signature(pq.write_table).parameters:
            self.skipTest("PyArrow does not support store_schema")
        row_count = 15_340
        values = pa.DictionaryArray.from_arrays(
            pa.array([0] * row_count, type=pa.int32()),
            pa.array([b"x" * 140_000], type=pa.binary()))
        payload = pa.StructArray.from_arrays(
            [values, pa.array([b"m"] * row_count)],
            names=["value", "metadata"])
        path = os.path.join(self.tmp, "large-dictionary-variant.parquet")
        pq.write_table(
            pa.table({"payload": payload}), path,
            use_dictionary=True, compression="zstd", store_schema=False)
        reader = FormatPyArrowReader(
            _LocalFileIO(), "parquet", path,
            [DataField(0, "payload", AtomicType("VARIANT"))],
            None, batch_size=128)
        return pq.ParquetFile(path).num_row_groups, _drain(reader)

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
