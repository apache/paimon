################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import unittest
from unittest.mock import patch

import pyarrow as pa

from pypaimon.read import table_read
from pypaimon.read.table_read import ROW_KIND_COLUMN, TableRead

# Patching table_read.pyarrow.array also rebinds pa.array (same module object),
# so keep a handle to the genuine implementation for use inside the fakes.
_REAL_ARRAY = pa.array


def _make_table_read(include_row_kind: bool) -> TableRead:
    # The conversion method only depends on self.include_row_kind, so build a
    # bare instance instead of standing up a full catalog/table.
    tr = TableRead.__new__(TableRead)
    tr.include_row_kind = include_row_kind
    return tr


class TableReadChunkedOverflowTest(unittest.TestCase):
    """Reading PK tables with very large STRING columns used to crash because a
    65536-row chunk could overflow pyarrow.string()'s 2GB per-column limit, making
    pyarrow.array() return a ChunkedArray that RecordBatch.from_arrays rejects.

    Instead of allocating 2GB, we patch pyarrow.array (as referenced by the module)
    to simulate the auto-chunking once a column list grows past a small threshold,
    and assert the chunk gets split into several single-Array batches.
    """

    OVERFLOW_THRESHOLD = 4  # pretend any column with > 4 rows overflows 2GB

    def _patched_array(self, obj, *args, **kwargs):
        real = _REAL_ARRAY(obj, *args, **kwargs)
        if isinstance(real, pa.Array) and len(real) > self.OVERFLOW_THRESHOLD:
            # Emulate pyarrow auto-chunking a >2GB string column.
            return pa.chunked_array([real])
        return real

    def _run(self, include_row_kind):
        schema = pa.schema([("id", pa.int64()), ("payload", pa.string())])
        if include_row_kind:
            schema = pa.schema(
                [(ROW_KIND_COLUMN, pa.string())] + [(f.name, f.type) for f in schema]
            )

        num_rows = 10
        row_tuples = [(i, f"v{i}") for i in range(num_rows)]
        row_kinds = ["+I"] * num_rows if include_row_kind else []

        tr = _make_table_read(include_row_kind)
        with patch.object(table_read.pyarrow, "array", side_effect=self._patched_array):
            batches = list(
                tr._convert_rows_to_arrow_batches_with_row_kind(row_tuples, row_kinds, schema)
            )

        # The single oversized chunk must be split into multiple batches.
        self.assertGreater(len(batches), 1)
        for b in batches:
            self.assertEqual(b.schema, schema)
            for col in b.columns:
                self.assertIsInstance(col, pa.Array)
                self.assertNotIsInstance(col, pa.ChunkedArray)

        merged = pa.Table.from_batches(batches)
        self.assertEqual(merged.num_rows, num_rows)
        self.assertEqual(merged.column("id").to_pylist(), [i for i in range(num_rows)])
        self.assertEqual(
            merged.column("payload").to_pylist(), [f"v{i}" for i in range(num_rows)]
        )
        if include_row_kind:
            self.assertEqual(
                merged.column(ROW_KIND_COLUMN).to_pylist(), ["+I"] * num_rows
            )

    def test_split_without_row_kind(self):
        self._run(include_row_kind=False)

    def test_split_with_row_kind(self):
        self._run(include_row_kind=True)

    def test_no_split_when_small(self):
        # Below the overflow threshold a single batch is produced as before.
        schema = pa.schema([("id", pa.int64()), ("payload", pa.string())])
        row_tuples = [(i, f"v{i}") for i in range(self.OVERFLOW_THRESHOLD)]

        tr = _make_table_read(include_row_kind=False)
        with patch.object(table_read.pyarrow, "array", side_effect=self._patched_array):
            batches = list(
                tr._convert_rows_to_arrow_batches_with_row_kind(row_tuples, [], schema)
            )

        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0].num_rows, self.OVERFLOW_THRESHOLD)

    def test_single_row_overflow_raises(self):
        # A single row that still overflows cannot be represented; guard against
        # infinite recursion with a clear error.
        schema = pa.schema([("payload", pa.string())])

        def always_chunk(obj, *args, **kwargs):
            return pa.chunked_array([_REAL_ARRAY(obj, *args, **kwargs)])

        tr = _make_table_read(include_row_kind=False)
        with patch.object(table_read.pyarrow, "array", side_effect=always_chunk):
            with self.assertRaises(ValueError):
                list(tr._convert_rows_to_arrow_batches_with_row_kind([("x",)], [], schema))


if __name__ == "__main__":
    unittest.main()
