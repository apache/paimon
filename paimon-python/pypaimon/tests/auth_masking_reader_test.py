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
#  limitations under the License.
################################################################################

import json
import unittest

import pyarrow as pa

from pypaimon.read.reader.auth_masking_reader import (
    AuthFilterReader,
    AuthMaskingReader,
    ColumnProjectReader,
    RecordReaderToBatchAdapter,
)


class _FakeField:
    def __init__(self, name):
        self.name = name


class _FakeBatchReader:
    def __init__(self, batches):
        self._batches = iter(batches)

    def read_arrow_batch(self):
        return next(self._batches, None)

    def close(self):
        pass


class _FakeRecordIterator:
    """Simulates RecordIterator[InternalRow] from a RecordReader."""
    def __init__(self, rows):
        self._rows = iter(rows)

    def next(self):
        return next(self._rows, None)


class _FakeOffsetRow:
    """Simulates OffsetRow returned by RecordReader.read_batch()."""
    def __init__(self, row_tuple, arity):
        self.row_tuple = row_tuple
        self.offset = 0
        self.arity = arity


class _FakeRecordReader:
    """Simulates a RecordReader (non-batch) like MergeFileSplitRead returns."""
    def __init__(self, batches_of_rows):
        self._batches = iter(batches_of_rows)
        self.closed = False

    def read_batch(self):
        batch = next(self._batches, None)
        if batch is None:
            return None
        return _FakeRecordIterator(batch)

    def close(self):
        self.closed = True


class TestRecordReaderToBatchAdapter(unittest.TestCase):

    def test_converts_rows_to_record_batch(self):
        rows = [
            _FakeOffsetRow((1, "alice"), 2),
            _FakeOffsetRow((2, "bob"), 2),
        ]
        inner = _FakeRecordReader([[rows[0], rows[1]]])
        schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
        adapter = RecordReaderToBatchAdapter(inner, schema)

        batch = adapter.read_arrow_batch()
        self.assertIsNotNone(batch)
        self.assertEqual(batch.num_rows, 2)
        self.assertEqual(batch.column("id").to_pylist(), [1, 2])
        self.assertEqual(batch.column("name").to_pylist(), ["alice", "bob"])

        # Second call returns None (exhausted)
        self.assertIsNone(adapter.read_arrow_batch())

    def test_multiple_inner_batches(self):
        batch1 = [_FakeOffsetRow((10,), 1)]
        batch2 = [_FakeOffsetRow((20,), 1), _FakeOffsetRow((30,), 1)]
        inner = _FakeRecordReader([batch1, batch2])
        schema = pa.schema([("val", pa.int64())])
        adapter = RecordReaderToBatchAdapter(inner, schema)

        batch = adapter.read_arrow_batch()
        self.assertIsNotNone(batch)
        self.assertEqual(batch.column("val").to_pylist(), [10, 20, 30])

    def test_empty_reader(self):
        inner = _FakeRecordReader([])
        schema = pa.schema([("x", pa.int64())])
        adapter = RecordReaderToBatchAdapter(inner, schema)
        self.assertIsNone(adapter.read_arrow_batch())

    def test_close_delegates(self):
        inner = _FakeRecordReader([])
        schema = pa.schema([("x", pa.int64())])
        adapter = RecordReaderToBatchAdapter(inner, schema)
        adapter.close()
        self.assertTrue(inner.closed)

    def test_works_with_auth_filter_reader(self):
        """Integration: adapter output can be wrapped by AuthFilterReader."""
        rows = [
            _FakeOffsetRow((1, "eng"), 2),
            _FakeOffsetRow((2, "sales"), 2),
            _FakeOffsetRow((3, "eng"), 2),
        ]
        inner = _FakeRecordReader([[rows[0], rows[1], rows[2]]])
        schema = pa.schema([("id", pa.int64()), ("dept", pa.string())])
        adapter = RecordReaderToBatchAdapter(inner, schema)

        def filter_fn(batch):
            import pyarrow.compute as pc
            return pc.equal(batch.column("dept"), "eng")

        filtered = AuthFilterReader(adapter, filter_fn)
        batch = filtered.read_arrow_batch()
        self.assertEqual(batch.num_rows, 2)
        self.assertEqual(batch.column("id").to_pylist(), [1, 3])


class TestAuthMaskingReaderTransforms(unittest.TestCase):

    def setUp(self):
        self.batch = pa.RecordBatch.from_pydict({
            "name": ["alice", "bob", "charlie"],
            "email": ["A@x.com", "B@y.com", "C@z.com"],
            "age": [25, 30, 35],
            "dept": ["eng", "sales", "eng"],
        })
        self.fields = [
            _FakeField("name"),
            _FakeField("email"),
            _FakeField("age"),
            _FakeField("dept"),
        ]

    def _apply_masking(self, masking_rules, batch=None, fields=None):
        batch = batch or self.batch
        fields = fields or self.fields
        reader = AuthMaskingReader(
            _FakeBatchReader([batch]), masking_rules, fields
        )
        return reader.read_arrow_batch()

    def test_null_transform(self):
        result = self._apply_masking(
            {"name": json.dumps({"name": "NULL"})}
        )
        self.assertEqual(result.column("name").to_pylist(), [None, None, None])
        self.assertEqual(result.schema.field("name").type, pa.string())

    def test_upper_transform(self):
        result = self._apply_masking({
            "email": json.dumps({
                "name": "UPPER",
                "inputs": [{"index": 1, "name": "email", "type": "STRING"}],
            })
        })
        self.assertEqual(
            result.column("email").to_pylist(),
            ["A@X.COM", "B@Y.COM", "C@Z.COM"],
        )

    def test_lower_transform(self):
        result = self._apply_masking({
            "email": json.dumps({
                "name": "LOWER",
                "inputs": [{"index": 1, "name": "email", "type": "STRING"}],
            })
        })
        self.assertEqual(
            result.column("email").to_pylist(),
            ["a@x.com", "b@y.com", "c@z.com"],
        )

    def test_field_ref_transform(self):
        result = self._apply_masking({
            "name": json.dumps({
                "name": "FIELD_REF",
                "fieldRef": {"index": 3, "name": "dept", "type": "STRING"},
            })
        })
        self.assertEqual(
            result.column("name").to_pylist(), ["eng", "sales", "eng"]
        )

    def test_cast_transform(self):
        result = self._apply_masking({
            "age": json.dumps({
                "name": "CAST",
                "fieldRef": {"index": 2, "name": "age", "type": "INT"},
                "type": "BIGINT",
            })
        })
        self.assertEqual(result.column("age").type, pa.int64())
        self.assertEqual(result.column("age").to_pylist(), [25, 30, 35])

    def test_cast_transform_preserves_original_schema(self):
        batch = pa.RecordBatch.from_pydict(
            {"id": pa.array([1, 2, 3], type=pa.int32())},
            schema=pa.schema([("id", pa.int32())]),
        )
        fields = [_FakeField("id")]
        result = self._apply_masking(
            {"id": json.dumps({
                "name": "CAST",
                "fieldRef": {"index": 0, "name": "id", "type": "INT"},
                "type": "BIGINT",
            })},
            batch=batch,
            fields=fields,
        )
        self.assertEqual(result.column("id").type, pa.int32())
        self.assertEqual(result.column("id").to_pylist(), [1, 2, 3])
        self.assertEqual(result.schema, batch.schema)

    def test_concat_transform(self):
        result = self._apply_masking({
            "name": json.dumps({
                "name": "CONCAT",
                "inputs": [
                    "***",
                    {"index": 1, "name": "email", "type": "STRING"},
                ],
            })
        })
        self.assertEqual(
            result.column("name").to_pylist(),
            ["***A@x.com", "***B@y.com", "***C@z.com"],
        )

    def test_concat_null_emits_null(self):
        batch = pa.RecordBatch.from_pydict({
            "name": ["alice", None, "charlie"],
            "tag": ["x", "y", "z"],
        })
        fields = [_FakeField("name"), _FakeField("tag")]
        result = self._apply_masking(
            {
                "tag": json.dumps({
                    "name": "CONCAT",
                    "inputs": [
                        {"index": 0, "name": "name", "type": "STRING"},
                        "@masked",
                    ],
                })
            },
            batch=batch,
            fields=fields,
        )
        self.assertEqual(
            result.column("tag").to_pylist(),
            ["alice@masked", None, "charlie@masked"],
        )

    def test_concat_ws_transform(self):
        batch = pa.RecordBatch.from_pydict({
            "name": ["alice", None, "charlie"],
            "dept": ["eng", "sales", "eng"],
        })
        fields = [_FakeField("name"), _FakeField("dept")]
        result = self._apply_masking(
            {
                "name": json.dumps({
                    "name": "CONCAT_WS",
                    "inputs": [
                        "-",
                        {"index": 0, "name": "name", "type": "STRING"},
                        {"index": 1, "name": "dept", "type": "STRING"},
                    ],
                })
            },
            batch=batch,
            fields=fields,
        )
        self.assertEqual(
            result.column("name").to_pylist(),
            ["alice-eng", "sales", "charlie-eng"],
        )

    def test_concat_ws_field_ref_separator(self):
        batch = pa.RecordBatch.from_pydict({
            "sep": ["-", "|", ":"],
            "a": ["x", "y", "z"],
            "b": ["1", "2", "3"],
        })
        fields = [_FakeField("sep"), _FakeField("a"), _FakeField("b")]
        result = self._apply_masking(
            {
                "a": json.dumps({
                    "name": "CONCAT_WS",
                    "inputs": [
                        {"index": 0, "name": "sep", "type": "STRING"},
                        {"index": 1, "name": "a", "type": "STRING"},
                        {"index": 2, "name": "b", "type": "STRING"},
                    ],
                })
            },
            batch=batch,
            fields=fields,
        )
        self.assertEqual(
            result.column("a").to_pylist(), ["x-1", "y|2", "z:3"]
        )


class TestMaskingOrderIndependence(unittest.TestCase):

    def test_cross_reference_uses_original_batch(self):
        batch = pa.RecordBatch.from_pydict({"a": ["x", "y"], "b": ["p", "q"]})
        fields = [_FakeField("a"), _FakeField("b")]
        masking = {
            "a": json.dumps({
                "name": "FIELD_REF",
                "fieldRef": {"index": 1, "name": "b", "type": "STRING"},
            }),
            "b": json.dumps({
                "name": "FIELD_REF",
                "fieldRef": {"index": 0, "name": "a", "type": "STRING"},
            }),
        }
        reader = AuthMaskingReader(_FakeBatchReader([batch]), masking, fields)
        result = reader.read_arrow_batch()
        self.assertEqual(result.column("a").to_pylist(), ["p", "q"])
        self.assertEqual(result.column("b").to_pylist(), ["x", "y"])


class TestMaskingFieldValidation(unittest.TestCase):

    def test_missing_field_raises(self):
        batch = pa.RecordBatch.from_pydict({"name": ["alice"]})
        fields = [_FakeField("name")]
        with self.assertRaises(RuntimeError) as ctx:
            AuthMaskingReader(
                _FakeBatchReader([batch]),
                {
                    "name": json.dumps({
                        "name": "FIELD_REF",
                        "fieldRef": {"index": 0, "name": "nonexistent", "type": "STRING"},
                    })
                },
                fields,
            )
        self.assertIn("nonexistent", str(ctx.exception))


class TestAuthFilterReader(unittest.TestCase):

    def test_filters_rows(self):
        import pyarrow.compute as pc

        batch = pa.RecordBatch.from_pydict({
            "dept": ["eng", "sales", "eng", "hr"],
        })

        def filter_fn(b):
            return pc.equal(b.column("dept"), "eng")

        reader = AuthFilterReader(_FakeBatchReader([batch]), filter_fn)
        result = reader.read_arrow_batch()
        self.assertEqual(result.num_rows, 2)
        self.assertEqual(result.column("dept").to_pylist(), ["eng", "eng"])

    def test_returns_none_at_end(self):
        import pyarrow.compute as pc

        reader = AuthFilterReader(
            _FakeBatchReader([]),
            lambda b: pc.equal(b.column("x"), 1),
        )
        self.assertIsNone(reader.read_arrow_batch())


class TestColumnProjectReader(unittest.TestCase):

    def test_selects_columns(self):
        batch = pa.RecordBatch.from_pydict({
            "a": [1, 2],
            "b": ["x", "y"],
            "c": [3.0, 4.0],
        })
        reader = ColumnProjectReader(_FakeBatchReader([batch]), ["a", "c"])
        result = reader.read_arrow_batch()
        self.assertEqual(result.schema.names, ["a", "c"])
        self.assertEqual(result.column("a").to_pylist(), [1, 2])
        self.assertEqual(result.column("c").to_pylist(), [3.0, 4.0])

    def test_returns_none_at_end(self):
        reader = ColumnProjectReader(_FakeBatchReader([]), ["a"])
        self.assertIsNone(reader.read_arrow_batch())


class TestRecordReaderToBatchAdapterDataLoss(unittest.TestCase):

    def test_no_data_loss_when_batch_exceeds_chunk_size(self):
        total_rows = 100
        chunk_size = 30

        all_rows = [_FakeOffsetRow((i, f"name_{i}"), 2) for i in range(total_rows)]
        row_iter = iter(all_rows)

        class MockBatchIterator:
            def next(self_inner):
                return next(row_iter, None)

        call_count = [0]

        class MockRecordReader:
            closed = False

            def read_batch(self):
                if call_count[0] == 0:
                    call_count[0] = 1
                    return MockBatchIterator()
                return None

            def close(self):
                self.closed = True

        inner = MockRecordReader()
        schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
        adapter = RecordReaderToBatchAdapter(inner, schema, chunk_size=chunk_size)

        collected_rows = 0
        while True:
            batch = adapter.read_arrow_batch()
            if batch is None:
                break
            collected_rows += batch.num_rows

        self.assertEqual(
            collected_rows, total_rows,
            f"Expected {total_rows} rows, got {collected_rows} (data loss!)")


class TestRecordReaderToBatchAdapterRowKind(unittest.TestCase):

    def test_include_row_kind_captures_kind(self):
        row1 = _FakeOffsetRow((1, "alice"), 2)
        row1.row_kind_byte = 0  # +I
        row2 = _FakeOffsetRow((2, "bob"), 2)
        row2.row_kind_byte = 3  # -D

        class FakeRowKindRow:
            def __init__(self, row):
                self.row_tuple = row.row_tuple
                self.offset = row.offset
                self.arity = row.arity
                self._row_kind_byte = row.row_kind_byte

            def get_row_kind(self):
                from pypaimon.table.row.row_kind import RowKind
                return RowKind(self._row_kind_byte)

        rows = [FakeRowKindRow(row1), FakeRowKindRow(row2)]
        inner = _FakeRecordReader([rows])
        schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
        adapter = RecordReaderToBatchAdapter(inner, schema, include_row_kind=True)

        batch = adapter.read_arrow_batch()
        self.assertIsNotNone(batch)
        self.assertIn("_row_kind", batch.schema.names)
        self.assertEqual(batch.column("_row_kind").to_pylist(), ["+I", "-D"])
        self.assertEqual(batch.column("id").to_pylist(), [1, 2])
        self.assertEqual(batch.column("name").to_pylist(), ["alice", "bob"])

    def test_no_row_kind_by_default(self):
        rows = [_FakeOffsetRow((1,), 1)]
        inner = _FakeRecordReader([[rows[0]]])
        schema = pa.schema([("id", pa.int64())])
        adapter = RecordReaderToBatchAdapter(inner, schema)

        batch = adapter.read_arrow_batch()
        self.assertNotIn("_row_kind", batch.schema.names)


class TestColumnProjectReaderRowKind(unittest.TestCase):

    def test_preserves_row_kind_column(self):
        batch = pa.RecordBatch.from_pydict({
            "_row_kind": ["+I", "-D"],
            "a": [1, 2],
            "b": ["x", "y"],
            "c": [3.0, 4.0],
        })
        reader = ColumnProjectReader(_FakeBatchReader([batch]), ["a", "c"])
        result = reader.read_arrow_batch()
        self.assertEqual(result.schema.names, ["_row_kind", "a", "c"])
        self.assertEqual(result.column("_row_kind").to_pylist(), ["+I", "-D"])

    def test_no_row_kind_no_change(self):
        batch = pa.RecordBatch.from_pydict({"a": [1], "b": [2]})
        reader = ColumnProjectReader(_FakeBatchReader([batch]), ["a"])
        result = reader.read_arrow_batch()
        self.assertEqual(result.schema.names, ["a"])


if __name__ == "__main__":
    unittest.main()
