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


_NO_LENGTH = object()


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


class TestSubstringTransform(unittest.TestCase):

    def setUp(self):
        self.batch = pa.RecordBatch.from_pydict({
            "ssn": ["123-45-6789", "987-65-4321", None],
            "begin": [8, 1, 1],
            "length": [4, 3, 3],
        })
        self.fields = [_FakeField("ssn"), _FakeField("begin"), _FakeField("length")]

    def _mask(self, transform, batch=None, fields=None):
        reader = AuthMaskingReader(
            _FakeBatchReader([batch if batch is not None else self.batch]),
            {"ssn": json.dumps(transform)},
            fields if fields is not None else self.fields,
        )
        return reader.read_arrow_batch().column("ssn").to_pylist()

    @staticmethod
    def _ssn_ref():
        return {"index": 0, "name": "ssn", "type": "STRING"}

    def test_begin_and_length(self):
        self.assertEqual(
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), 8, 4]}),
            ["6789", "4321", None],
        )

    def test_begin_only_runs_to_end_of_string(self):
        self.assertEqual(
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), 8]}),
            ["6789", "4321", None],
        )

    def test_begin_past_end_yields_empty_string_not_null(self):
        self.assertEqual(
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), 99, 4]}),
            ["", "", None],
        )

    def test_length_longer_than_string_is_clamped(self):
        self.assertEqual(
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), 8, 100]}),
            ["6789", "4321", None],
        )

    def test_positions_read_from_other_fields(self):
        self.assertEqual(
            self._mask({
                "name": "SUBSTRING",
                "inputs": [
                    self._ssn_ref(),
                    {"index": 1, "name": "begin", "type": "INT"},
                    {"index": 2, "name": "length", "type": "INT"},
                ],
            }),
            ["6789", "987", None],
        )

    def _mask_with_position_fields(self, begin, length=_NO_LENGTH, ssn="123-45-6789"):
        cols = {"ssn": pa.array([ssn], type=pa.string()),
                "begin": pa.array([begin], type=pa.int32())}
        inputs = [self._ssn_ref(), {"index": 1, "name": "begin", "type": "INT"}]
        if length is not _NO_LENGTH:
            cols["length"] = pa.array([length], type=pa.int32())
            inputs.append({"index": 2, "name": "length", "type": "INT"})
        batch = pa.RecordBatch.from_arrays(list(cols.values()), names=list(cols))
        reader = AuthMaskingReader(
            _FakeBatchReader([batch]),
            {"ssn": json.dumps({"name": "SUBSTRING", "inputs": inputs})},
            [_FakeField(n) for n in cols],
        )
        return reader.read_arrow_batch().column("ssn").to_pylist()

    def _mask_with_bigint_position(self, begin):
        batch = pa.RecordBatch.from_arrays(
            [pa.array(["abcdef"], type=pa.string()), pa.array([begin], type=pa.int64())],
            names=["ssn", "begin"])
        reader = AuthMaskingReader(
            _FakeBatchReader([batch]),
            {"ssn": json.dumps({"name": "SUBSTRING", "inputs": [
                self._ssn_ref(), {"index": 1, "name": "begin", "type": "BIGINT"}]})},
            [_FakeField("ssn"), _FakeField("begin")])
        return reader.read_arrow_batch().column("ssn").to_pylist()

    def test_wider_position_field_is_rejected_only_once_a_row_reads_it(self):
        # Java checks a position for null before reading it, so a null propagates
        # whatever the field type is; only a non-null one reaches the read
        self.assertEqual(self._mask_with_bigint_position(None), [None])
        with self.assertRaisesRegex(ValueError, "position field must be INT"):
            self._mask_with_bigint_position(2)

    def test_begin_past_end_yields_empty_string_for_field_positions(self):
        self.assertEqual(self._mask_with_position_fields(99, 4), [""])

    def test_non_positive_length_rejected_for_field_positions(self):
        with self.assertRaisesRegex(ValueError, "SUBSTRING out of bounds"):
            self._mask_with_position_fields(1, 0)

    def test_begin_only_runs_to_end_for_field_positions(self):
        self.assertEqual(self._mask_with_position_fields(8), ["6789"])

    def test_null_position_masks_the_row_to_null(self):
        self.assertEqual(self._mask_with_position_fields(None, 4), [None])
        self.assertEqual(self._mask_with_position_fields(8, None), [None])

    def test_non_ascii_positions_count_characters_on_both_paths(self):
        source = "身份证12345678"
        self.assertEqual(
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), 8, 4]},
                       batch=pa.RecordBatch.from_arrays(
                           [pa.array([source], type=pa.string())], names=["ssn"]),
                       fields=[_FakeField("ssn")]),
            ["5678"],
        )
        self.assertEqual(self._mask_with_position_fields(8, 4, ssn=source), ["5678"])

    def test_fractional_position_rejected(self):
        for begin in [1.5, 2.0, "1.5", True]:
            with self.assertRaisesRegex(ValueError, "must be an integer"):
                self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), begin, 2]})

    def test_textual_position_accepted_like_integer_parse_int(self):
        for begin in ["8", "+8", "008"]:
            self.assertEqual(
                self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), begin, 4]}),
                ["6789", "4321", None],
                begin,
            )

    def test_textual_position_outside_parse_int_syntax_rejected(self):
        for begin in ["1_0", " 2 ", "2\n"]:
            with self.assertRaisesRegex(ValueError, "must be an integer"):
                self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), begin, 4]})

    def test_number_in_the_source_slot_rejected(self):
        with self.assertRaisesRegex(ValueError, "source must be a string or a field"):
            self._mask({"name": "SUBSTRING", "inputs": [123, 1, 1]})

    def test_supplementary_plane_digit_rejected(self):
        with self.assertRaisesRegex(ValueError, "must be an integer"):
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), "\U0001D7DA", 4]})

    def test_invalid_json_position_type_fails_even_when_the_row_short_circuits(self):
        for length in [1.5, True]:
            with self.assertRaisesRegex(ValueError, "must be an integer"):
                self._mask({"name": "SUBSTRING", "inputs": ["abcdef", 99, length]})

    def test_unicode_digit_position_accepted_like_character_digit(self):
        for begin in ["8", "\u0668", "\uff18"]:
            self.assertEqual(
                self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), begin, 4]}),
                ["6789", "4321", None],
                begin,
            )

    def test_explicit_null_position_propagates(self):
        self.assertEqual(
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), 8, None]}),
            [None, None, None],
        )
        self.assertEqual(
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), None]}),
            [None, None, None],
        )

    def test_null_length_propagates_even_when_begin_is_past_the_end(self):
        self.assertEqual(
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), 99, None]}),
            [None, None, None],
        )

    def test_wrong_arity_rejected(self):
        with self.assertRaisesRegex(ValueError, "SUBSTRING takes 2 or 3 inputs"):
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), 8, 4, 9]})
        with self.assertRaisesRegex(ValueError, "SUBSTRING takes 2 or 3 inputs"):
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref()]})

    def test_position_field_of_a_non_integer_type_rejected(self):
        batch = pa.RecordBatch.from_arrays(
            [pa.array(["abcdef"], type=pa.string()), pa.array(["2"], type=pa.string())],
            names=["ssn", "begin"],
        )
        reader = AuthMaskingReader(
            _FakeBatchReader([batch]),
            {"ssn": json.dumps({
                "name": "SUBSTRING",
                "inputs": [self._ssn_ref(), {"index": 1, "name": "begin", "type": "STRING"}],
            })},
            [_FakeField("ssn"), _FakeField("begin")],
        )
        with self.assertRaisesRegex(ValueError, "must be INT"):
            reader.read_arrow_batch()

    def test_position_field_of_a_wider_integer_type_rejected(self):
        for declared, arrow_type in [("BIGINT", pa.int64()), ("SMALLINT", pa.int16()),
                                     ("TINYINT", pa.int8())]:
            batch = pa.RecordBatch.from_arrays(
                [pa.array(["abcdef"], type=pa.string()), pa.array([2], type=arrow_type)],
                names=["ssn", "begin"],
            )
            reader = AuthMaskingReader(
                _FakeBatchReader([batch]),
                {"ssn": json.dumps({
                    "name": "SUBSTRING",
                    "inputs": [self._ssn_ref(), {"index": 1, "name": "begin", "type": declared}],
                })},
                [_FakeField("ssn"), _FakeField("begin")],
            )
            with self.assertRaisesRegex(ValueError, "must be INT"):
                reader.read_arrow_batch()

    def test_null_source_wins_over_a_bad_begin(self):
        batch = pa.RecordBatch.from_arrays(
            [pa.array([None], type=pa.string())], names=["ssn"])
        self.assertEqual(
            self._mask({"name": "SUBSTRING", "inputs": [None, None]},
                       batch=batch, fields=[_FakeField("ssn")]),
            [None],
        )

    def test_begin_past_end_wins_over_a_malformed_length(self):
        self.assertEqual(
            self._mask({"name": "SUBSTRING", "inputs": ["abc", 99, "bad"]}),
            ["", "", ""],
        )

    def test_null_length_wins_over_a_malformed_begin(self):
        # Java checks every position for null before it parses any of them, so the
        # malformed begin is never reached
        self.assertEqual(
            self._mask({"name": "SUBSTRING", "inputs": ["abc", "bad", None]}),
            [None, None, None],
        )

    def test_end_overflowing_the_integer_range_rejected(self):
        with self.assertRaisesRegex(ValueError, "overflows the integer range"):
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), 2, 2 ** 31 - 1]})

    def test_position_outside_the_integer_range_rejected(self):
        with self.assertRaisesRegex(ValueError, "out of the integer range"):
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), 2 ** 31, 4]})

    def test_null_length_field_propagates_even_when_begin_is_past_the_end(self):
        self.assertEqual(self._mask_with_position_fields(99, None), [None])

    def test_supplementary_characters_count_code_points_like_java(self):
        self.assertEqual(
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), 2, 2]},
                       batch=pa.RecordBatch.from_arrays(
                           [pa.array(["\U0001F600abc"], type=pa.string())], names=["ssn"]),
                       fields=[_FakeField("ssn")]),
            ["ab"],
        )

    def test_literal_source_instead_of_field(self):
        self.assertEqual(
            self._mask({"name": "SUBSTRING", "inputs": ["123-45-6789", 8, 4]}),
            ["6789", "6789", "6789"],
        )

    def test_non_positive_length_rejected(self):
        with self.assertRaisesRegex(ValueError, "SUBSTRING out of bounds"):
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), 1, 0]})

    def test_begin_below_one_rejected(self):
        with self.assertRaisesRegex(ValueError, "SUBSTRING out of bounds"):
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), 0]})

    def test_begin_below_one_rejected_for_field_positions(self):
        batch = pa.RecordBatch.from_pydict({"ssn": ["123-45-6789"], "begin": [0]})
        reader = AuthMaskingReader(
            _FakeBatchReader([batch]),
            {"ssn": json.dumps({
                "name": "SUBSTRING",
                "inputs": [self._ssn_ref(), {"index": 1, "name": "begin", "type": "INT"}],
            })},
            [_FakeField("ssn"), _FakeField("begin")],
        )
        with self.assertRaisesRegex(ValueError, "SUBSTRING out of bounds"):
            reader.read_arrow_batch()

    def test_begin_past_end_wins_over_a_bad_length(self):
        self.assertEqual(
            self._mask({"name": "SUBSTRING", "inputs": [self._ssn_ref(), 99, 0]}),
            ["", "", None],
        )


class TestTrimTransform(unittest.TestCase):

    def setUp(self):
        self.batch = pa.RecordBatch.from_pydict({
            "s": ["  x  ", "\ty\t", None],
            "chars": [" ", "\t", "z"],
        })
        self.fields = [_FakeField("s"), _FakeField("chars")]

    def _mask(self, transform):
        reader = AuthMaskingReader(
            _FakeBatchReader([self.batch]), {"s": json.dumps(transform)}, self.fields
        )
        return reader.read_arrow_batch().column("s").to_pylist()

    @staticmethod
    def _transform(flag, extra_inputs=()):
        return {
            "name": "TRIM",
            "inputs": [{"index": 0, "name": "s", "type": "STRING"}, *extra_inputs],
            "trimFlag": flag,
        }

    def test_both(self):
        self.assertEqual(self._mask(self._transform("BOTH")), ["x", "\ty\t", None])

    def test_leading(self):
        self.assertEqual(self._mask(self._transform("LEADING")), ["x  ", "\ty\t", None])

    def test_trailing(self):
        self.assertEqual(self._mask(self._transform("TRAILING")), ["  x", "\ty\t", None])

    def test_wrong_arity_rejected(self):
        with self.assertRaisesRegex(ValueError, "TRIM takes 1 or 2 inputs"):
            self._mask(self._transform("BOTH", ["x", "y"]))

    def test_unknown_flag_rejected(self):
        for flag in ["both", "LTRIM"]:
            with self.assertRaisesRegex(ValueError, "Unknown trimFlag"):
                self._mask(self._transform(flag))

    def test_numeric_trim_flag_rejected(self):
        # Jackson would read a number as an enum ordinal, so Java must reject it too
        for flag in (0, "0", 2):
            with self.assertRaisesRegex(ValueError, "trimFlag"):
                self._mask({"name": "TRIM", "inputs": ["  x  "], "trimFlag": flag})

    def test_non_string_source_rejected(self):
        with self.assertRaisesRegex(ValueError, "TRIM source"):
            self._mask({"name": "TRIM", "inputs": [123], "trimFlag": "BOTH"})
        with self.assertRaisesRegex(ValueError, "TRIM source"):
            self._mask({"name": "TRIM", "inputs": ["  x  ", 123], "trimFlag": "BOTH"})

    def test_structured_position_rejected_before_any_shortcut(self):
        # begin past the end would otherwise short-circuit to an empty string
        with self.assertRaisesRegex(ValueError, "position must be an integer"):
            self._mask({"name": "SUBSTRING", "inputs": ["abc", 99, []]})
        with self.assertRaisesRegex(ValueError, "position must be an integer"):
            self._mask({"name": "SUBSTRING", "inputs": ["abc", []]})

    def test_unknown_flag_rejected_with_null_chars(self):
        with self.assertRaisesRegex(ValueError, "Unknown trimFlag"):
            self._mask({
                "name": "TRIM",
                "inputs": [{"index": 0, "name": "s", "type": "STRING"}, None],
                "trimFlag": "LTRIM",
            })

    def _trim_by(self, chars, values):
        batch = pa.RecordBatch.from_arrays(
            [pa.array(values, type=pa.string())], names=["s"])
        reader = AuthMaskingReader(
            _FakeBatchReader([batch]),
            {"s": json.dumps({
                "name": "TRIM",
                "inputs": [{"index": 0, "name": "s", "type": "STRING"}, chars],
                "trimFlag": "BOTH",
            })},
            [_FakeField("s")],
        )
        return reader.read_arrow_batch().column("s").to_pylist()

    def test_multibyte_trim_characters(self):
        self.assertEqual(self._trim_by("。", ["。。x。。", "  y  "]), ["x", "  y  "])

    def test_trim_matches_whole_characters_not_bytes(self):
        self.assertEqual(self._trim_by("、", ["。x。"]), ["。x。"])

    def test_custom_chars_are_treated_as_a_set(self):
        batch = pa.RecordBatch.from_pydict({"s": ["xyzaxyz", "zyxaxyz", None]})
        reader = AuthMaskingReader(
            _FakeBatchReader([batch]),
            {"s": json.dumps({
                "name": "TRIM",
                "inputs": [{"index": 0, "name": "s", "type": "STRING"}, "xyz"],
                "trimFlag": "BOTH",
            })},
            [_FakeField("s")],
        )
        self.assertEqual(
            reader.read_arrow_batch().column("s").to_pylist(), ["a", "a", None]
        )

    def test_chars_read_from_another_field(self):
        self.assertEqual(
            self._mask(
                self._transform("BOTH", [{"index": 1, "name": "chars", "type": "STRING"}])
            ),
            ["x", "y", None],
        )

    def test_literal_null_trim_chars_yields_null(self):
        self.assertEqual(
            self._mask({
                "name": "TRIM",
                "inputs": [{"index": 0, "name": "s", "type": "STRING"}, None],
                "trimFlag": "BOTH",
            }),
            [None, None, None],
        )

    def test_null_trim_chars_yields_null(self):
        batch = pa.RecordBatch.from_arrays(
            [pa.array(["  x  "], type=pa.string()), pa.array([None], type=pa.string())],
            names=["s", "chars"],
        )
        reader = AuthMaskingReader(
            _FakeBatchReader([batch]),
            {"s": json.dumps(
                self._transform("BOTH", [{"index": 1, "name": "chars", "type": "STRING"}])
            )},
            [_FakeField("s"), _FakeField("chars")],
        )
        self.assertEqual(reader.read_arrow_batch().column("s").to_pylist(), [None])

    def test_missing_flag_rejected(self):
        with self.assertRaisesRegex(ValueError, "trimFlag"):
            self._mask({
                "name": "TRIM",
                "inputs": [{"index": 0, "name": "s", "type": "STRING"}],
            })


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

    def test_adopts_reader_metadata(self):
        inner = _FakeBatchReader([])
        inner.file_io = object()
        inner.blob_field_indices = frozenset([1])
        inner.vector_field_indices = frozenset([2])

        reader = AuthFilterReader(inner, lambda batch: None)

        self.assertIs(reader.file_io, inner.file_io)
        self.assertEqual(reader.blob_field_indices, inner.blob_field_indices)
        self.assertEqual(reader.vector_field_indices, inner.vector_field_indices)

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
