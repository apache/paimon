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

"""Exact string filtering on in-memory system-table metadata."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.common.predicate import Predicate
from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.system.system_table_scan import SystemSplit
from pypaimon.tests.system.system_table_test import (
    _ArraySystemTable, _DataSystemTable, _fake_base)


class _StringSystemTable(_DataSystemTable):

    def _build_arrow_table(self):
        return pa.table({
            "id": pa.array(range(1, 11), type=pa.int32()),
            "name": pa.array([
                "skip.orc", "data-1.parquet", "data-2.orc", "other.parquet",
                None, "", "DATA-3.parquet", "metric_100%", "\u6587\u4ef6.parquet", "path\\file",
            ]),
        })


@pytest.fixture
def table():
    return _StringSystemTable(_fake_base())


def _read(builder):
    return builder.new_read().to_arrow(builder.new_scan().plan().splits())


@pytest.mark.parametrize("method,pattern,expected", [
    ("startswith", "data-", [2, 3]),
    ("endswith", ".parquet", [2, 4, 7, 9]),
    ("contains", "parquet", [2, 4, 7, 9]),
    ("like", "data-_.parquet", [2]),
])
@pytest.mark.parametrize("combination", ["leaf", "and", "or"])
def test_string_predicates_and_combinations(table, method, pattern, expected, combination):
    builder = table.new_read_builder()
    pb = builder.new_predicate_builder()
    predicate = getattr(pb, method)("name", pattern)
    if combination == "and":
        predicate = pb.and_predicates([predicate, pb.greater_than("id", 2)])
        expected = [i for i in expected if i > 2]
    elif combination == "or":
        predicate = pb.or_predicates([pb.equal("id", 1), predicate])
        expected = [1] + expected
    result = _read(builder.with_filter(predicate).with_projection(["id"]))
    assert result.column("id").to_pylist() == expected


@pytest.mark.parametrize("method", [
    "equal", "not_equal", "less_than", "less_or_equal", "greater_than", "greater_or_equal",
])
@pytest.mark.parametrize("field", ["id", "name"])
@pytest.mark.parametrize("combination", ["and", "or"])
def test_mixed_null_comparisons_preserve_arrow_semantics(table, method, field, combination):
    builder = table.new_read_builder()
    pb = builder.new_predicate_builder()
    comparison = getattr(pb, method)(field, None)
    assert _read(builder.with_filter(comparison)).num_rows == 0
    predicate = getattr(pb, combination + "_predicates")([
        comparison, pb.contains("name", "parquet"),
    ])
    result = _read(builder.with_filter(predicate).with_projection(["id"]))
    assert result.column("id").to_pylist() == ([] if combination == "and" else [2, 4, 7, 9])


@pytest.mark.parametrize("method,literals,expected", [
    ("between", [None, 3], []),
    ("between", [3, None], []),
    ("between", [None, None], []),
    ("not_between", [None, 3], [4, 5, 6, 7, 8, 9, 10]),
    ("not_between", [3, None], [1, 2]),
    ("not_between", [None, None], []),
    ("is_in", [[None, 2]], [2]),
    ("is_in", [[None]], []),
    ("is_not_in", [[None, 2]], []),
])
@pytest.mark.parametrize("combination", ["and", "or"])
def test_mixed_null_range_and_set_comparisons(table, method, literals, expected, combination):
    builder = table.new_read_builder()
    pb = builder.new_predicate_builder()
    comparison = getattr(pb, method)("id", *literals)
    assert _read(builder.with_filter(comparison)).column("id").to_pylist() == expected
    predicate = getattr(pb, combination + "_predicates")([
        comparison, pb.contains("name", "parquet"),
    ])
    string_ids = {2, 4, 7, 9}
    expected = sorted(set(expected) & string_ids if combination == "and" else set(expected) | string_ids)
    assert _read(builder.with_filter(predicate)).column("id").to_pylist() == expected


@pytest.mark.parametrize("method,pattern,expected", [
    ("startswith", "", [1, 2, 3, 4, 6, 7, 8, 9, 10]),
    ("endswith", "", [1, 2, 3, 4, 6, 7, 8, 9, 10]),
    ("contains", "", [1, 2, 3, 4, 6, 7, 8, 9, 10]),
    ("like", "%", [1, 2, 3, 4, 6, 7, 8, 9, 10]),
    ("like", "", [6]),
    ("contains", "%", [8]),
    ("contains", "_", [8]),
    ("contains", "\u6587\u4ef6", [9]),
    ("like", r"metric\_100\%", [8]),
    ("like", r"path\\file", [10]),
    ("like", "%.parquet", [2, 4, 7, 9]),
    ("like", "missing%", []),
])
def test_string_matching_edge_cases(table, method, pattern, expected):
    builder = table.new_read_builder()
    predicate = getattr(builder.new_predicate_builder(), method)("name", pattern)
    assert _read(builder.with_filter(predicate)).column("id").to_pylist() == expected


@pytest.mark.parametrize("pattern,expected", [("a_b", [1, 2]), ("a%b", [1, 2]), ("a.b", [2])])
def test_like_wildcards_match_newlines_and_escape_regex_characters(table, pattern, expected):
    rows = pa.table({"id": pa.array([1, 2], type=pa.int32()), "name": ["a\nb", "a.b"]})
    with patch.object(table, "_build_arrow_table", return_value=rows):
        builder = table.new_read_builder()
        pb = builder.new_predicate_builder()
        predicate = pb.and_predicates([pb.like("name", pattern), pb.greater_than("id", 0)])
        assert _read(builder.with_filter(predicate)).column("id").to_pylist() == expected


def test_nested_predicate_never_uses_lossy_arrow_conversion(table):
    builder = table.new_read_builder()
    pb = builder.new_predicate_builder()
    predicate = pb.and_predicates([
        pb.or_predicates([pb.contains("name", "parquet"), pb.is_null("name")]),
        pb.greater_than("id", 3),
    ])
    with patch.object(Predicate, "to_arrow", side_effect=AssertionError("Unsafe Arrow conversion")):
        result = _read(builder.with_filter(predicate))
    assert result.column("id").to_pylist() == [4, 5, 7, 9]


def test_combines_multiple_string_predicates(table):
    builder = table.new_read_builder()
    pb = builder.new_predicate_builder()
    predicate = pb.and_predicates([
        pb.or_predicates([pb.startswith("name", "data-"), pb.endswith("name", ".parquet")]),
        pb.contains("name", "parquet"),
    ])
    assert _read(builder.with_filter(predicate)).column("id").to_pylist() == [2, 4, 7, 9]


def test_does_not_convert_unrelated_columns_to_python(table):
    fields = table.row_type().fields + [DataField(2, "event_time", AtomicType("TIMESTAMP(6)"))]
    # Year 10000 fits Arrow timestamps but cannot be converted to datetime.
    rows = table._build_arrow_table().append_column(
        "event_time", pa.array([253402300800000000] * 10, type=pa.timestamp("us")))
    with patch.object(table, "row_type", return_value=RowType(False, fields)):
        with patch.object(table, "_build_arrow_table", return_value=rows):
            builder = table.new_read_builder()
            predicate = builder.new_predicate_builder().startswith("name", "data-")
            result = _read(builder.with_filter(predicate).with_projection(["id"]))
    assert result.to_pylist() == [{"id": 2}, {"id": 3}]


def test_string_filter_precedes_projection_and_global_limit(table):
    builder = table.new_read_builder()
    predicate = builder.new_predicate_builder().endswith("name", ".parquet")
    builder.with_filter(predicate).with_projection(["id"]).with_limit(2)
    rows = table._build_arrow_table()
    splits = [SystemSplit(rows.slice(0, 3)), SystemSplit(rows.slice(3))]
    assert builder.new_read().to_arrow(splits).to_pylist() == [{"id": 2}, {"id": 4}]


def test_projected_predicate_indices_are_rebound_without_mutation(table):
    builder = table.new_read_builder().with_projection(["name", "id"])
    pb = builder.new_predicate_builder()
    string_predicate = pb.startswith("name", "data-")
    predicate = pb.and_predicates([string_predicate, pb.equal("id", 3)])
    assert _read(builder.with_filter(predicate)).to_pylist() == [{"name": "data-2.orc", "id": 3}]
    assert string_predicate.index == 0


@pytest.mark.parametrize("empty_splits", [False, True])
def test_string_filter_on_empty_input_keeps_projected_schema(table, empty_splits):
    builder = table.new_read_builder()
    predicate = builder.new_predicate_builder().contains("name", "parquet")
    builder.with_filter(predicate).with_projection(["id"])
    splits = [] if empty_splits else [SystemSplit(table._build_arrow_table().slice(0, 0))]
    result = builder.new_read().to_arrow(splits)
    assert result.num_rows == 0
    assert result.schema == pa.schema([pa.field("id", pa.int32(), nullable=False)])


@pytest.mark.parametrize("method", ["startswith", "endswith", "contains", "like"])
@pytest.mark.parametrize("field", ["id", "tags"])
def test_string_predicates_require_string_columns(method, field):
    builder = _ArraySystemTable(_fake_base()).new_read_builder()
    predicate = getattr(builder.new_predicate_builder(), method)(field, "a")
    with pytest.raises(NotImplementedError, match="column"):
        _read(builder.with_filter(predicate))


@pytest.mark.parametrize("method", ["startswith", "endswith", "contains", "like"])
@pytest.mark.parametrize("pattern", [None, 1, b"data"])
def test_string_patterns_must_be_strings_even_without_splits(table, method, pattern):
    builder = table.new_read_builder()
    predicate = getattr(builder.new_predicate_builder(), method)("name", pattern)
    with pytest.raises(ValueError, match="string literal"):
        builder.with_filter(predicate).new_read().to_arrow([])


@pytest.mark.parametrize("pattern", ["bad\\", r"bad\q"])
def test_invalid_like_escape_is_rejected_even_in_unused_branch(table, pattern):
    builder = table.new_read_builder()
    pb = builder.new_predicate_builder()
    predicate = pb.or_predicates([pb.is_not_null("id"), pb.like("name", pattern)])
    with pytest.raises(ValueError, match="Invalid escape sequence"):
        _read(builder.with_filter(predicate))


@pytest.mark.parametrize("literals", [None, [], ["a", "b"]])
def test_string_pattern_requires_exactly_one_literal(table, literals):
    builder = table.new_read_builder()
    predicate = builder.new_predicate_builder().contains("name", "a").new_literals(literals)
    with pytest.raises(ValueError, match="single string literal"):
        _read(builder.with_filter(predicate))


def test_unknown_predicate_is_rejected_in_unused_branch(table):
    builder = table.new_read_builder()
    pb = builder.new_predicate_builder()
    predicate = pb.or_predicates([
        pb.contains("name", ""), Predicate("unknown", 0, "id", []),
    ])
    with pytest.raises(ValueError, match="Unsupported predicate method"):
        _read(builder.with_filter(predicate))


@pytest.mark.parametrize("output", ["to_arrow", "to_pandas", "to_iterator", "to_record_batch_iterator"])
def test_string_filter_applies_to_all_outputs(table, output):
    builder = table.new_read_builder()
    predicate = builder.new_predicate_builder().startswith("name", "data-")
    builder.with_filter(predicate).with_projection(["id"]).with_limit(1)
    result = getattr(builder.new_read(), output)(builder.new_scan().plan().splits())
    if output == "to_pandas":
        result = result.to_dict("records")
    elif output == "to_iterator":
        result = list(result)
    elif output == "to_record_batch_iterator":
        result = pa.Table.from_batches(list(result)).to_pylist()
    else:
        result = result.to_pylist()
    assert result == [{"id": 2}]


def test_native_result_handoff_uses_string_filter(table):
    builder = table.new_read_builder()
    predicate = builder.new_predicate_builder().contains("name", "parquet")
    rows = table._build_arrow_table()
    with patch("pypaimon.table.system.native_system_table.try_read_native_system_table", return_value=rows):
        with patch.object(table, "_build_arrow_table", side_effect=AssertionError("Metadata rebuilt")):
            assert _read(builder.with_filter(predicate)).column("id").to_pylist() == [2, 4, 7, 9]
