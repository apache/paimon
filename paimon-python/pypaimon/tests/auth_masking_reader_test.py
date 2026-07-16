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
)
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader


class _FakeField:
    def __init__(self, name):
        self.name = name


class _FakeBatchReader(RecordBatchReader):
    def __init__(self, batches):
        self._batches = iter(batches)

    def read_arrow_batch(self):
        return next(self._batches, None)

    def close(self):
        pass


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

    def test_cast_transform_changes_column_type(self):
        """CAST transform changes the output column type (matching Java behavior)."""
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
        self.assertEqual(result.column("id").type, pa.int64())
        self.assertEqual(result.column("id").to_pylist(), [1, 2, 3])

    def test_cast_transform_int_to_string(self):
        """CAST INT to STRING changes column type to string (matching Java)."""
        batch = pa.RecordBatch.from_pydict(
            {"id": pa.array([100, 200], type=pa.int32())},
            schema=pa.schema([("id", pa.int32())]),
        )
        fields = [_FakeField("id")]
        result = self._apply_masking(
            {"id": json.dumps({
                "name": "CAST",
                "fieldRef": {"index": 0, "name": "id", "type": "INT"},
                "type": "STRING",
            })},
            batch=batch,
            fields=fields,
        )
        self.assertEqual(result.column("id").type, pa.string())
        self.assertEqual(result.column("id").to_pylist(), ["100", "200"])

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


class TestMaskingSkipsNonProjectedColumns(unittest.TestCase):
    """Java skips masking rules whose target column is absent from the output row type."""

    def test_non_projected_masking_target_does_not_raise(self):
        """If REST returns secret=FIELD_REF(email) but user reads only id,
        the rule should be silently skipped, not raise for missing email."""
        batch = pa.RecordBatch.from_pydict({"id": [1, 2, 3]})
        fields = [_FakeField("id")]
        # Masking rule targets 'secret', which is NOT in the user's projection
        masking_rules = {
            "secret": json.dumps({
                "name": "FIELD_REF",
                "fieldRef": {"index": 0, "name": "email", "type": "STRING"},
            })
        }
        # Should not raise even though 'email' is not in read_fields
        reader = AuthMaskingReader(
            _FakeBatchReader([batch]), masking_rules, fields)
        result = reader.read_arrow_batch()
        self.assertEqual(result.column("id").to_pylist(), [1, 2, 3])

    def test_projected_masking_target_still_validates(self):
        """If target IS projected, referenced fields must still exist."""
        batch = pa.RecordBatch.from_pydict({"name": ["alice"]})
        fields = [_FakeField("name")]
        masking_rules = {
            "name": json.dumps({
                "name": "FIELD_REF",
                "fieldRef": {"index": 0, "name": "nonexistent", "type": "STRING"},
            })
        }
        with self.assertRaises(RuntimeError) as ctx:
            AuthMaskingReader(
                _FakeBatchReader([batch]), masking_rules, fields)
        self.assertIn("nonexistent", str(ctx.exception))


class TestMaskingSkipsBlankJsonValues(unittest.TestCase):
    """Java extractColumnMasking skips entries with empty column name or empty JSON value."""

    def test_blank_json_value_skipped(self):
        batch = pa.RecordBatch.from_pydict({"name": ["alice", "bob"]})
        fields = [_FakeField("name")]
        reader = AuthMaskingReader(
            _FakeBatchReader([batch]),
            {"name": ""},
            fields
        )
        result = reader.read_arrow_batch()
        self.assertEqual(result.column("name").to_pylist(), ["alice", "bob"])

    def test_valid_and_blank_rules_mixed(self):
        batch = pa.RecordBatch.from_pydict({"name": ["alice"], "email": ["a@b.com"]})
        fields = [_FakeField("name"), _FakeField("email")]
        reader = AuthMaskingReader(
            _FakeBatchReader([batch]),
            {"name": "", "email": json.dumps({"name": "NULL"})},
            fields
        )
        result = reader.read_arrow_batch()
        self.assertEqual(result.column("name").to_pylist(), ["alice"])
        self.assertEqual(result.column("email").to_pylist(), [None])


class TestConcatWsAllNullMasking(unittest.TestCase):

    def test_concat_ws_all_null_values_returns_empty_string(self):
        batch = pa.RecordBatch.from_pydict(
            {"name": [None, None, None]},
            schema=pa.schema([("name", pa.string())]),
        )
        fields = [_FakeField("name")]
        masking_rules = {
            "name": json.dumps({
                "name": "CONCAT_WS",
                "inputs": [",", None, None],
            })
        }
        reader = AuthMaskingReader(
            _FakeBatchReader([batch]), masking_rules, fields)
        result = reader.read_arrow_batch()
        self.assertEqual(len(result), 3)
        self.assertEqual(result.column("name").to_pylist(), ["", "", ""])

    def test_concat_ws_null_separator_returns_null(self):
        batch = pa.RecordBatch.from_pydict(
            {"name": ["hello", "world", "test"]},
            schema=pa.schema([("name", pa.string())]),
        )
        fields = [_FakeField("name")]
        masking_rules = {
            "name": json.dumps({
                "name": "CONCAT_WS",
                "inputs": [None, "a", "b"],
            })
        }
        reader = AuthMaskingReader(
            _FakeBatchReader([batch]), masking_rules, fields)
        result = reader.read_arrow_batch()
        self.assertEqual(len(result), 3)
        self.assertEqual(result.column("name").to_pylist(), [None, None, None])

    def test_concat_ws_mixed_null_preserves_row_positions(self):
        batch = pa.RecordBatch.from_pydict(
            {"a": [None, "x", None, "p"], "b": [None, "y", "z", None]},
            schema=pa.schema([("a", pa.string()), ("b", pa.string())]),
        )
        fields = [_FakeField("a"), _FakeField("b")]
        masking_rules = {
            "a": json.dumps({
                "name": "CONCAT_WS",
                "inputs": ["-", {"index": 0, "name": "a", "type": "STRING"},
                           {"index": 1, "name": "b", "type": "STRING"}],
            })
        }
        reader = AuthMaskingReader(
            _FakeBatchReader([batch]), masking_rules, fields)
        result = reader.read_arrow_batch()
        self.assertEqual(len(result), 4)
        self.assertEqual(result.column("a").to_pylist(), ["", "x-y", "z", "p"])


class TestPickleTableQueryAuthFn(unittest.TestCase):

    def test_auth_fn_is_pickleable(self):
        import pickle
        from pypaimon.catalog.catalog_environment import _TableQueryAuthFn

        fn = _TableQueryAuthFn(None, "db.table")
        restored = pickle.loads(pickle.dumps(fn))
        self.assertEqual(restored._identifier, "db.table")
        self.assertIsNone(restored._catalog_loader)


class TestTableNoPermissionExceptionUnified(unittest.TestCase):

    def test_catalog_exception_is_base(self):
        from pypaimon.catalog.catalog_exception import (
            CatalogException,
            TableNoPermissionException,
        )
        from pypaimon.common.identifier import Identifier
        exc = TableNoPermissionException(Identifier("db", "table"))
        self.assertIsInstance(exc, CatalogException)

    def test_message_contains_table_name(self):
        from pypaimon.catalog.catalog_exception import TableNoPermissionException
        from pypaimon.common.identifier import Identifier
        exc = TableNoPermissionException(Identifier("db", "table"))
        self.assertIn("db.table", str(exc))
        self.assertIn("No permission", str(exc))

    def test_catches_as_catalog_exception(self):
        from pypaimon.catalog.catalog_exception import (
            CatalogException,
            TableNoPermissionException,
        )
        from pypaimon.common.identifier import Identifier
        with self.assertRaises(CatalogException):
            raise TableNoPermissionException(Identifier("db", "table"))


if __name__ == "__main__":
    unittest.main()
