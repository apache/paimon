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

from pypaimon.common.predicate_json_parser import (
    _convert_literal,
    _paimon_type_to_arrow,
    extract_referenced_fields,
    parse_predicate_to_batch_filter,
)


def _make_leaf(field_name, function, literals=None, field_type="INT"):
    d = {
        "kind": "LEAF",
        "transform": {
            "name": "FIELD_REF",
            "fieldRef": {"index": 0, "name": field_name, "type": field_type},
        },
        "function": function,
    }
    if literals is not None:
        d["literals"] = literals
    return json.dumps(d)


class TestLeafFunctions(unittest.TestCase):

    def setUp(self):
        self.batch = pa.RecordBatch.from_pydict({
            "id": [1, 2, 3, 4, 5],
            "name": ["alice", "bob", "charlie", "alice", "eve"],
            "score": [85.5, 92.0, 78.3, 95.1, 60.0],
        })

    def _filter(self, json_str):
        return parse_predicate_to_batch_filter(json_str)(self.batch).to_pylist()

    def test_equal(self):
        self.assertEqual(
            self._filter(_make_leaf("id", "EQUAL", [3])),
            [False, False, True, False, False],
        )

    def test_not_equal(self):
        self.assertEqual(
            self._filter(_make_leaf("id", "NOT_EQUAL", [3])),
            [True, True, False, True, True],
        )

    def test_less_than(self):
        self.assertEqual(
            self._filter(_make_leaf("id", "LESS_THAN", [3])),
            [True, True, False, False, False],
        )

    def test_less_or_equal(self):
        self.assertEqual(
            self._filter(_make_leaf("id", "LESS_OR_EQUAL", [3])),
            [True, True, True, False, False],
        )

    def test_greater_than(self):
        self.assertEqual(
            self._filter(_make_leaf("id", "GREATER_THAN", [3])),
            [False, False, False, True, True],
        )

    def test_greater_or_equal(self):
        self.assertEqual(
            self._filter(_make_leaf("id", "GREATER_OR_EQUAL", [3])),
            [False, False, True, True, True],
        )

    def test_is_null(self):
        batch = pa.RecordBatch.from_pydict({"val": [1, None, 3, None, 5]})
        f = parse_predicate_to_batch_filter(_make_leaf("val", "IS_NULL"))
        self.assertEqual(f(batch).to_pylist(), [False, True, False, True, False])

    def test_is_not_null(self):
        batch = pa.RecordBatch.from_pydict({"val": [1, None, 3, None, 5]})
        f = parse_predicate_to_batch_filter(_make_leaf("val", "IS_NOT_NULL"))
        self.assertEqual(f(batch).to_pylist(), [True, False, True, False, True])

    def test_in(self):
        self.assertEqual(
            self._filter(_make_leaf("id", "IN", [1, 3, 5])),
            [True, False, True, False, True],
        )

    def test_not_in(self):
        self.assertEqual(
            self._filter(_make_leaf("id", "NOT_IN", [1, 3, 5])),
            [False, True, False, True, False],
        )

    def test_between(self):
        self.assertEqual(
            self._filter(_make_leaf("id", "BETWEEN", [2, 4])),
            [False, True, True, True, False],
        )

    def test_not_between(self):
        self.assertEqual(
            self._filter(_make_leaf("id", "NOT_BETWEEN", [2, 4])),
            [True, False, False, False, True],
        )

    def test_starts_with(self):
        self.assertEqual(
            self._filter(_make_leaf("name", "STARTS_WITH", ["al"], "STRING")),
            [True, False, False, True, False],
        )

    def test_ends_with(self):
        self.assertEqual(
            self._filter(_make_leaf("name", "ENDS_WITH", ["e"], "STRING")),
            [True, False, True, True, True],
        )

    def test_contains(self):
        self.assertEqual(
            self._filter(_make_leaf("name", "CONTAINS", ["li"], "STRING")),
            [True, False, True, True, False],
        )

    def test_like(self):
        like_json = json.dumps({
            "kind": "LEAF",
            "transform": {"name": "FIELD_REF", "fieldRef": {"index": 0, "name": "name", "type": "STRING"}},
            "function": "LIKE",
            "literals": ["%li%"],
        })
        self.assertEqual(
            parse_predicate_to_batch_filter(like_json)(self.batch).to_pylist(),
            [True, False, True, True, False],
        )

    def test_true(self):
        self.assertEqual(
            self._filter(_make_leaf("id", "TRUE")),
            [True, True, True, True, True],
        )

    def test_false(self):
        self.assertEqual(
            self._filter(_make_leaf("id", "FALSE")),
            [False, False, False, False, False],
        )


class TestCompoundPredicates(unittest.TestCase):

    def setUp(self):
        self.batch = pa.RecordBatch.from_pydict({
            "id": [1, 2, 3, 4, 5],
        })

    def test_and(self):
        pred = json.dumps({
            "kind": "COMPOUND",
            "function": "AND",
            "children": [
                {"kind": "LEAF",
                 "transform": {"name": "FIELD_REF",
                               "fieldRef": {"index": 0, "name": "id", "type": "INT"}},
                 "function": "GREATER_THAN", "literals": [2]},
                {"kind": "LEAF",
                 "transform": {"name": "FIELD_REF",
                               "fieldRef": {"index": 0, "name": "id", "type": "INT"}},
                 "function": "LESS_THAN", "literals": [5]},
            ],
        })
        f = parse_predicate_to_batch_filter(pred)
        self.assertEqual(f(self.batch).to_pylist(), [False, False, True, True, False])

    def test_or(self):
        pred = json.dumps({
            "kind": "COMPOUND",
            "function": "OR",
            "children": [
                {"kind": "LEAF",
                 "transform": {"name": "FIELD_REF",
                               "fieldRef": {"index": 0, "name": "id", "type": "INT"}},
                 "function": "EQUAL", "literals": [1]},
                {"kind": "LEAF",
                 "transform": {"name": "FIELD_REF",
                               "fieldRef": {"index": 0, "name": "id", "type": "INT"}},
                 "function": "EQUAL", "literals": [5]},
            ],
        })
        f = parse_predicate_to_batch_filter(pred)
        self.assertEqual(f(self.batch).to_pylist(), [True, False, False, False, True])


class TestPredicateTransforms(unittest.TestCase):

    def setUp(self):
        self.batch = pa.RecordBatch.from_pydict({
            "id": [1, 2, 3, 4, 5],
            "name": ["alice", "bob", "charlie", "alice", "eve"],
        })

    def test_upper_transform(self):
        pred = json.dumps({
            "kind": "LEAF",
            "transform": {"name": "UPPER", "inputs": [{"index": 1, "name": "name", "type": "STRING"}]},
            "function": "EQUAL",
            "literals": ["ALICE"],
        })
        f = parse_predicate_to_batch_filter(pred)
        self.assertEqual(f(self.batch).to_pylist(), [True, False, False, True, False])

    def test_lower_transform(self):
        pred = json.dumps({
            "kind": "LEAF",
            "transform": {"name": "LOWER", "inputs": [{"index": 1, "name": "name", "type": "STRING"}]},
            "function": "EQUAL",
            "literals": ["bob"],
        })
        f = parse_predicate_to_batch_filter(pred)
        self.assertEqual(f(self.batch).to_pylist(), [False, True, False, False, False])

    def test_cast_transform(self):
        pred = json.dumps({
            "kind": "LEAF",
            "transform": {"name": "CAST", "fieldRef": {"index": 0, "name": "id", "type": "INT"}, "type": "BIGINT"},
            "function": "GREATER_THAN",
            "literals": [3],
        })
        f = parse_predicate_to_batch_filter(pred)
        self.assertEqual(f(self.batch).to_pylist(), [False, False, False, True, True])

    def test_null_transform(self):
        pred = json.dumps({
            "kind": "LEAF",
            "transform": {"name": "NULL"},
            "function": "IS_NULL",
        })
        f = parse_predicate_to_batch_filter(pred)
        self.assertEqual(f(self.batch).to_pylist(), [True, True, True, True, True])

    def test_concat_transform_in_predicate(self):
        batch = pa.RecordBatch.from_pydict({
            "first": ["john", "jane"],
            "last": ["doe", "smith"],
        })
        pred = json.dumps({
            "kind": "LEAF",
            "transform": {
                "name": "CONCAT",
                "inputs": [
                    {"index": 0, "name": "first", "type": "STRING"},
                    {"index": 1, "name": "last", "type": "STRING"},
                ],
            },
            "function": "EQUAL",
            "literals": ["johndoe"],
        })
        f = parse_predicate_to_batch_filter(pred)
        self.assertEqual(f(batch).to_pylist(), [True, False])

    def test_concat_ws_transform_in_predicate(self):
        batch = pa.RecordBatch.from_pydict({
            "first": ["john", "jane"],
            "last": ["doe", "smith"],
        })
        pred = json.dumps({
            "kind": "LEAF",
            "transform": {
                "name": "CONCAT_WS",
                "inputs": [
                    " ",
                    {"index": 0, "name": "first", "type": "STRING"},
                    {"index": 1, "name": "last", "type": "STRING"},
                ],
            },
            "function": "EQUAL",
            "literals": ["john doe"],
        })
        f = parse_predicate_to_batch_filter(pred)
        self.assertEqual(f(batch).to_pylist(), [True, False])


class TestPaimonTypeToArrow(unittest.TestCase):

    def test_simple_types(self):
        self.assertEqual(_paimon_type_to_arrow("INT"), pa.int32())
        self.assertEqual(_paimon_type_to_arrow("BIGINT"), pa.int64())
        self.assertEqual(_paimon_type_to_arrow("SMALLINT"), pa.int16())
        self.assertEqual(_paimon_type_to_arrow("TINYINT"), pa.int8())
        self.assertEqual(_paimon_type_to_arrow("FLOAT"), pa.float32())
        self.assertEqual(_paimon_type_to_arrow("DOUBLE"), pa.float64())
        self.assertEqual(_paimon_type_to_arrow("STRING"), pa.string())
        self.assertEqual(_paimon_type_to_arrow("BOOLEAN"), pa.bool_())
        self.assertEqual(_paimon_type_to_arrow("BYTES"), pa.binary())
        self.assertEqual(_paimon_type_to_arrow("DATE"), pa.date32())

    def test_varchar_char(self):
        self.assertEqual(_paimon_type_to_arrow("VARCHAR"), pa.string())
        self.assertEqual(_paimon_type_to_arrow("VARCHAR(100)"), pa.string())
        self.assertEqual(_paimon_type_to_arrow("CHAR(10)"), pa.string())

    def test_timestamp(self):
        self.assertEqual(_paimon_type_to_arrow("TIMESTAMP(3)"), pa.timestamp("ms"))
        self.assertEqual(_paimon_type_to_arrow("TIMESTAMP(6)"), pa.timestamp("us"))
        self.assertEqual(_paimon_type_to_arrow("TIMESTAMP(9)"), pa.timestamp("ns"))
        self.assertEqual(_paimon_type_to_arrow("TIMESTAMP(0)"), pa.timestamp("s"))

    def test_timestamp_ltz(self):
        self.assertEqual(
            _paimon_type_to_arrow("TIMESTAMP WITH LOCAL TIME ZONE(6)"),
            pa.timestamp("us", tz="UTC"),
        )

    def test_decimal(self):
        self.assertEqual(
            _paimon_type_to_arrow("DECIMAL(10, 2)"),
            pa.decimal128(10, 2),
        )

    def test_unsupported_type_raises(self):
        with self.assertRaises(ValueError):
            _paimon_type_to_arrow("ARRAY<INT>")


class TestConvertLiteral(unittest.TestCase):

    def test_none_literal(self):
        self.assertIsNone(_convert_literal(None, pa.int32()))

    def test_int_passthrough(self):
        self.assertEqual(_convert_literal(42, pa.int32()), 42)

    def test_string_passthrough(self):
        self.assertEqual(_convert_literal("hello", pa.string()), "hello")

    def test_timestamp_literal(self):
        result = _convert_literal("2024-01-15T10:30:00", pa.timestamp("us"))
        self.assertIsInstance(result, pa.Scalar)

    def test_timestamp_z_suffix(self):
        result = _convert_literal("2024-01-15T10:30:00Z", pa.timestamp("us", tz="UTC"))
        self.assertIsInstance(result, pa.Scalar)

    def test_date_literal(self):
        result = _convert_literal("2024-01-15", pa.date32())
        self.assertIsInstance(result, pa.Scalar)

    def test_decimal_literal(self):
        result = _convert_literal(123.45, pa.decimal128(10, 2))
        self.assertIsInstance(result, pa.Scalar)


class TestExtractReferencedFields(unittest.TestCase):

    def test_leaf_field_ref(self):
        refs = extract_referenced_fields(json.dumps({
            "kind": "LEAF",
            "transform": {"name": "FIELD_REF", "fieldRef": {"index": 0, "name": "col1", "type": "INT"}},
            "function": "EQUAL",
            "literals": [1],
        }))
        self.assertEqual(refs, {"col1"})

    def test_leaf_cast(self):
        refs = extract_referenced_fields(json.dumps({
            "kind": "LEAF",
            "transform": {"name": "CAST", "fieldRef": {"index": 0, "name": "col1", "type": "INT"}, "type": "BIGINT"},
            "function": "EQUAL",
            "literals": [1],
        }))
        self.assertEqual(refs, {"col1"})

    def test_compound_collects_all(self):
        refs = extract_referenced_fields(json.dumps({
            "kind": "COMPOUND",
            "function": "AND",
            "children": [
                {"kind": "LEAF",
                 "transform": {"name": "FIELD_REF",
                               "fieldRef": {"index": 0, "name": "a", "type": "INT"}},
                 "function": "EQUAL", "literals": [1]},
                {"kind": "LEAF",
                 "transform": {"name": "UPPER",
                               "inputs": [{"index": 1, "name": "b", "type": "STRING"}]},
                 "function": "EQUAL", "literals": ["X"]},
            ],
        }))
        self.assertEqual(refs, {"a", "b"})

    def test_concat_inputs(self):
        refs = extract_referenced_fields(json.dumps({
            "kind": "LEAF",
            "transform": {
                "name": "CONCAT",
                "inputs": [
                    {"index": 0, "name": "first", "type": "STRING"},
                    "literal",
                    {"index": 1, "name": "last", "type": "STRING"},
                ],
            },
            "function": "EQUAL",
            "literals": ["x"],
        }))
        self.assertEqual(refs, {"first", "last"})


class TestLikeEdgeCases(unittest.TestCase):

    def test_dot_is_literal(self):
        batch = pa.RecordBatch.from_pydict({"v": ["a.b", "axb", "a.bc"]})
        f = parse_predicate_to_batch_filter(json.dumps({
            "kind": "LEAF",
            "transform": {"name": "FIELD_REF", "fieldRef": {"index": 0, "name": "v", "type": "STRING"}},
            "function": "LIKE",
            "literals": ["a.b"],
        }))
        self.assertEqual(f(batch).to_pylist(), [True, False, False])

    def test_underscore_matches_single_char(self):
        batch = pa.RecordBatch.from_pydict({"v": ["abc", "axc", "ac", "abbc"]})
        f = parse_predicate_to_batch_filter(json.dumps({
            "kind": "LEAF",
            "transform": {"name": "FIELD_REF", "fieldRef": {"index": 0, "name": "v", "type": "STRING"}},
            "function": "LIKE",
            "literals": ["a_c"],
        }))
        self.assertEqual(f(batch).to_pylist(), [True, True, False, False])

    def test_percent_matches_any(self):
        batch = pa.RecordBatch.from_pydict({"v": ["abc", "ac", "axyzc", "def"]})
        f = parse_predicate_to_batch_filter(json.dumps({
            "kind": "LEAF",
            "transform": {"name": "FIELD_REF", "fieldRef": {"index": 0, "name": "v", "type": "STRING"}},
            "function": "LIKE",
            "literals": ["a%c"],
        }))
        self.assertEqual(f(batch).to_pylist(), [True, True, True, False])


class TestLikeEscapeSemantics(unittest.TestCase):

    def _make_like_batch(self, values):
        return pa.RecordBatch.from_pydict(
            {"name": values}, schema=pa.schema([("name", pa.string())]))

    def _make_like_predicate(self, pattern):
        return json.dumps({
            "kind": "LEAF",
            "transform": {"name": "FIELD_REF", "fieldRef": {"name": "name"}},
            "function": "LIKE",
            "literals": [pattern]
        })

    def test_like_simple_percent(self):
        batch = self._make_like_batch(["admin", "admin_foo", "user"])
        fn = parse_predicate_to_batch_filter(self._make_like_predicate("admin%"))
        self.assertEqual(fn(batch).to_pylist(), [True, True, False])

    def test_like_simple_underscore(self):
        batch = self._make_like_batch(["ab", "abc", "a"])
        fn = parse_predicate_to_batch_filter(self._make_like_predicate("a_"))
        self.assertEqual(fn(batch).to_pylist(), [True, False, False])

    def test_like_escaped_underscore(self):
        batch = self._make_like_batch(["admin_foo", "adminXfoo", "admin"])
        fn = parse_predicate_to_batch_filter(self._make_like_predicate("admin\\_%"))
        self.assertEqual(fn(batch).to_pylist(), [True, False, False])

    def test_like_escaped_percent(self):
        batch = self._make_like_batch(["100%", "100X", "100"])
        fn = parse_predicate_to_batch_filter(self._make_like_predicate("100\\%"))
        self.assertEqual(fn(batch).to_pylist(), [True, False, False])

    def test_like_escaped_backslash(self):
        batch = self._make_like_batch(["a\\b", "a/b", "ab"])
        fn = parse_predicate_to_batch_filter(self._make_like_predicate("a\\\\b"))
        self.assertEqual(fn(batch).to_pylist(), [True, False, False])

    def test_like_regex_special_chars(self):
        batch = self._make_like_batch(["a.b", "axb", "a..b"])
        fn = parse_predicate_to_batch_filter(self._make_like_predicate("a.b"))
        self.assertEqual(fn(batch).to_pylist(), [True, False, False])

    def test_like_percent_matches_newline(self):
        batch = self._make_like_batch(["hello\nworld", "hello"])
        fn = parse_predicate_to_batch_filter(self._make_like_predicate("hello%"))
        self.assertEqual(fn(batch).to_pylist(), [True, True])

    def test_like_invalid_escape_sequence(self):
        import pytest
        fn = parse_predicate_to_batch_filter(self._make_like_predicate("admin\\x"))
        batch = self._make_like_batch(["admin"])
        with pytest.raises(RuntimeError, match="Invalid escape sequence"):
            fn(batch)


if __name__ == "__main__":
    unittest.main()
