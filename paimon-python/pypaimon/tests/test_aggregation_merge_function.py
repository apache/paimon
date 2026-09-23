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

"""Direct unit tests for :class:`AggregateMergeFunction` and its
helper functions.

Drives the merge function with synthetic :class:`KeyValue` instances
so the contract is pinned down without going through the full read
pipeline. The end-to-end behaviour on real PK tables is exercised
separately in ``test_aggregation_e2e.py``.
"""

import unittest

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.read.reader.aggregate import create_field_aggregator
from pypaimon.read.reader.aggregation_merge_function import (
    AggregateMergeFunction,
    build_field_aggregators,
    resolve_agg_func_name,
)
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.key_value import KeyValue
from pypaimon.table.row.row_kind import RowKind


def _kv(key, seq, row_kind, value):
    """Build a fresh KeyValue for a (key, sequence, row_kind, value)
    tuple. Same shape as the helper in
    ``test_partial_update_merge_function.py`` so both test files read
    consistently.
    """
    kv = KeyValue(key_arity=len(key), value_arity=len(value))
    kv.replace(tuple(key) + (seq, row_kind.value) + tuple(value))
    return kv


def _result_value(kv):
    return tuple(kv.value.get_field(i) for i in range(kv.value_arity))


def _result_key(kv):
    return tuple(kv.key.get_field(i) for i in range(kv.key_arity))


def _make_agg(identifier, sql_type, field_name="f"):
    return create_field_aggregator(
        AtomicType(sql_type), field_name, identifier, options=None
    )


class AggregateMergeFunctionTest(unittest.TestCase):

    def test_single_insert_returns_aggregated_value(self):
        mf = AggregateMergeFunction(
            key_arity=1, value_arity=1,
            field_aggregators=[_make_agg("sum", "BIGINT")],
        )
        mf.reset()
        mf.add(_kv((1,), 100, RowKind.INSERT, (10,)))
        result = mf.get_result()

        self.assertIsNotNone(result)
        self.assertEqual(_result_key(result), (1,))
        self.assertEqual(_result_value(result), (10,))
        self.assertEqual(result.sequence_number, 100)
        self.assertEqual(result.value_row_kind_byte, RowKind.INSERT.value)

    def test_multi_row_aggregation_across_fields(self):
        # field 0: sum, field 1: max, field 2: last_value
        mf = AggregateMergeFunction(
            key_arity=1, value_arity=3,
            field_aggregators=[
                _make_agg("sum", "BIGINT", "v_sum"),
                _make_agg("max", "INT", "v_max"),
                _make_agg("last_value", "VARCHAR", "v_last"),
            ],
        )
        mf.reset()
        mf.add(_kv((1,), 100, RowKind.INSERT, (10, 5, "a")))
        mf.add(_kv((1,), 101, RowKind.INSERT, (20, 3, "b")))
        mf.add(_kv((1,), 102, RowKind.INSERT, (30, 9, "c")))

        result = mf.get_result()
        self.assertEqual(_result_value(result), (60, 9, "c"))
        # latest sequence is propagated through.
        self.assertEqual(result.sequence_number, 102)

    def test_null_inputs_follow_aggregator_semantics(self):
        # sum drops nulls; last_non_null_value drops nulls; last_value keeps them.
        mf = AggregateMergeFunction(
            key_arity=1, value_arity=3,
            field_aggregators=[
                _make_agg("sum", "BIGINT", "v_sum"),
                _make_agg("last_non_null_value", "VARCHAR", "v_lnn"),
                _make_agg("last_value", "VARCHAR", "v_last"),
            ],
        )
        mf.reset()
        mf.add(_kv((1,), 100, RowKind.INSERT, (10, "x", "x")))
        mf.add(_kv((1,), 101, RowKind.INSERT, (None, None, None)))
        mf.add(_kv((1,), 102, RowKind.INSERT, (5, None, "z")))

        result = mf.get_result()
        # sum: 10 + 5 = 15 (nulls absorbed)
        # last_non_null: 'x' (intermediate nulls preserved earlier value)
        # last_value: 'z' (the very last value, including the prior None)
        self.assertEqual(_result_value(result), (15, "x", "z"))

    def test_update_after_treated_as_insert(self):
        mf = AggregateMergeFunction(
            key_arity=1, value_arity=1,
            field_aggregators=[_make_agg("sum", "BIGINT")],
        )
        mf.reset()
        mf.add(_kv((1,), 100, RowKind.UPDATE_AFTER, (7,)))
        result = mf.get_result()
        self.assertEqual(_result_value(result), (7,))

    def test_delete_row_raises_not_implemented(self):
        mf = AggregateMergeFunction(
            key_arity=1, value_arity=1,
            field_aggregators=[_make_agg("sum", "BIGINT")],
        )
        mf.reset()
        with self.assertRaises(NotImplementedError) as ctx:
            mf.add(_kv((1,), 100, RowKind.DELETE, (5,)))
        self.assertIn("retract", str(ctx.exception))
        self.assertIn("-D", str(ctx.exception))

    def test_update_before_row_raises_not_implemented(self):
        mf = AggregateMergeFunction(
            key_arity=1, value_arity=1,
            field_aggregators=[_make_agg("sum", "BIGINT")],
        )
        mf.reset()
        with self.assertRaises(NotImplementedError) as ctx:
            mf.add(_kv((1,), 100, RowKind.UPDATE_BEFORE, (5,)))
        self.assertIn("-U", str(ctx.exception))

    def test_reset_between_keys_clears_state(self):
        mf = AggregateMergeFunction(
            key_arity=1, value_arity=2,
            field_aggregators=[
                _make_agg("sum", "BIGINT"),
                _make_agg("first_value", "VARCHAR"),
            ],
        )
        # Key group 1.
        mf.reset()
        mf.add(_kv((1,), 100, RowKind.INSERT, (5, "a")))
        mf.add(_kv((1,), 101, RowKind.INSERT, (3, "b")))
        r1 = mf.get_result()
        self.assertEqual(_result_value(r1), (8, "a"))
        # Key group 2 — sum must restart from 0 and first_value must
        # re-arm so the new group's first row wins.
        mf.reset()
        mf.add(_kv((2,), 200, RowKind.INSERT, (10, "x")))
        mf.add(_kv((2,), 201, RowKind.INSERT, (20, "y")))
        r2 = mf.get_result()
        self.assertEqual(_result_key(r2), (2,))
        self.assertEqual(_result_value(r2), (30, "x"))

    def test_get_result_before_any_add_returns_none(self):
        mf = AggregateMergeFunction(
            key_arity=1, value_arity=1,
            field_aggregators=[_make_agg("sum", "BIGINT")],
        )
        mf.reset()
        self.assertIsNone(mf.get_result())

    def test_result_is_decoupled_from_input_kv(self):
        """Critical: upstream KeyValueWrapReader reuses one KeyValue and
        rebinds its row_tuple between iterations. The merge function
        must snapshot its output so the previously-returned result is
        not silently mutated when the source advances.
        """
        mf = AggregateMergeFunction(
            key_arity=1, value_arity=1,
            field_aggregators=[_make_agg("sum", "BIGINT")],
        )
        mf.reset()
        # Build one reusable kv and rebind it twice, like upstream does.
        kv = KeyValue(key_arity=1, value_arity=1)
        kv.replace((1, 100, RowKind.INSERT.value, 5))
        mf.add(kv)
        kv.replace((1, 101, RowKind.INSERT.value, 7))
        mf.add(kv)
        result = mf.get_result()

        # Now mutate the source kv. The previously-captured result must
        # NOT change.
        kv.replace((999, 999, RowKind.INSERT.value, 99999))

        self.assertEqual(_result_key(result), (1,))
        self.assertEqual(_result_value(result), (12,))  # 5 + 7
        self.assertEqual(result.sequence_number, 101)

    def test_value_arity_mismatch_at_construction_raises(self):
        with self.assertRaises(ValueError):
            AggregateMergeFunction(
                key_arity=1, value_arity=2,
                field_aggregators=[_make_agg("sum", "BIGINT")],
            )


class ResolveAggFuncNameTest(unittest.TestCase):

    def test_primary_key_takes_precedence(self):
        name = resolve_agg_func_name(
            "id", primary_keys={"id"}, options_map={
                "fields.id.aggregate-function": "sum",
            }
        )
        self.assertEqual(name, "primary_key")

    def test_field_level_override_wins_over_default(self):
        name = resolve_agg_func_name(
            "v", primary_keys=set(), options_map={
                "fields.v.aggregate-function": "max",
                "fields.default-aggregate-function": "sum",
            }
        )
        self.assertEqual(name, "max")

    def test_table_default_used_when_no_field_override(self):
        name = resolve_agg_func_name(
            "v", primary_keys=set(), options_map={
                "fields.default-aggregate-function": "sum",
            }
        )
        self.assertEqual(name, "sum")

    def test_system_default_when_nothing_configured(self):
        name = resolve_agg_func_name(
            "v", primary_keys=set(), options_map={}
        )
        self.assertEqual(name, "last_non_null_value")


class BuildFieldAggregatorsTest(unittest.TestCase):

    def _make_options(self, raw):
        return CoreOptions(Options(raw))

    def test_builds_aggregators_aligned_with_value_fields(self):
        fields = [
            DataField(0, "id", AtomicType("BIGINT")),
            DataField(1, "amount", AtomicType("BIGINT")),
            DataField(2, "name", AtomicType("VARCHAR")),
        ]
        options = self._make_options({
            "fields.amount.aggregate-function": "sum",
        })
        aggs = build_field_aggregators(
            value_fields=fields,
            primary_keys=["id"],
            core_options=options,
        )
        self.assertEqual(len(aggs), 3)
        self.assertEqual(aggs[0].name, "primary_key")
        self.assertEqual(aggs[1].name, "sum")
        # Falls through to the system default since no override and no
        # fields.default-aggregate-function is set.
        self.assertEqual(aggs[2].name, "last_non_null_value")

    def test_unknown_aggregator_identifier_raises(self):
        fields = [
            DataField(0, "id", AtomicType("BIGINT")),
            DataField(1, "v", AtomicType("BIGINT")),
        ]
        options = self._make_options({
            "fields.v.aggregate-function": "no_such_aggregator",
        })
        with self.assertRaises(ValueError):
            build_field_aggregators(
                value_fields=fields,
                primary_keys=["id"],
                core_options=options,
            )


if __name__ == '__main__':
    unittest.main()
