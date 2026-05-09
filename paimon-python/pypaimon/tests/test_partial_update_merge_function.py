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

"""Direct unit tests for ``PartialUpdateMergeFunction``.

Drives the merge function with synthetic ``KeyValue`` instances so the
contract is pinned down without going through the full read pipeline.
The end-to-end behaviour on real PK tables is exercised separately in
``test_partial_update_e2e.py``.
"""

import unittest

from pypaimon.read.reader.partial_update_merge_function import \
    PartialUpdateMergeFunction
from pypaimon.table.row.key_value import KeyValue
from pypaimon.table.row.row_kind import RowKind


def _kv(key, seq, row_kind, value):
    """Build a fresh KeyValue for a (key, sequence, row_kind, value) tuple.

    ``key`` and ``value`` are tuples of primitives — the helper handles
    layout (key, seq, row_kind_byte, value) so individual tests can stay
    focused on the merge semantics.
    """
    kv = KeyValue(key_arity=len(key), value_arity=len(value))
    kv.replace(tuple(key) + (seq, row_kind.value) + tuple(value))
    return kv


def _result_value(kv):
    """Extract the value tuple out of a KeyValue produced by get_result()."""
    return tuple(kv.value.get_field(i) for i in range(kv.value_arity))


def _result_key(kv):
    return tuple(kv.key.get_field(i) for i in range(kv.key_arity))


class PartialUpdateMergeFunctionTest(unittest.TestCase):

    def test_single_insert_returns_value(self):
        mf = PartialUpdateMergeFunction(key_arity=1, value_arity=2)
        mf.reset()
        mf.add(_kv((1,), 100, RowKind.INSERT, ('a', 'x')))
        result = mf.get_result()

        self.assertIsNotNone(result)
        self.assertEqual(_result_key(result), (1,))
        self.assertEqual(_result_value(result), ('a', 'x'))
        self.assertEqual(result.sequence_number, 100)
        self.assertEqual(result.value_row_kind_byte, RowKind.INSERT.value)

    def test_second_insert_overwrites_non_null(self):
        mf = PartialUpdateMergeFunction(key_arity=1, value_arity=2)
        mf.reset()
        mf.add(_kv((1,), 100, RowKind.INSERT, ('a', None)))
        mf.add(_kv((1,), 101, RowKind.INSERT, ('b', None)))

        result = mf.get_result()
        self.assertEqual(_result_value(result), ('b', None))
        # Sequence number tracks the latest add().
        self.assertEqual(result.sequence_number, 101)

    def test_second_insert_fills_in_null(self):
        mf = PartialUpdateMergeFunction(key_arity=1, value_arity=2)
        mf.reset()
        mf.add(_kv((1,), 100, RowKind.INSERT, ('a', None)))
        mf.add(_kv((1,), 101, RowKind.INSERT, (None, 'x')))

        result = mf.get_result()
        self.assertEqual(_result_value(result), ('a', 'x'))

    def test_third_insert_continues_merge(self):
        mf = PartialUpdateMergeFunction(key_arity=1, value_arity=3)
        mf.reset()
        mf.add(_kv((1,), 100, RowKind.INSERT, ('a', None, None)))
        mf.add(_kv((1,), 101, RowKind.INSERT, (None, 'b', None)))
        mf.add(_kv((1,), 102, RowKind.INSERT, (None, None, 'c')))

        result = mf.get_result()
        self.assertEqual(_result_value(result), ('a', 'b', 'c'))

    def test_later_null_does_not_clobber_earlier_value(self):
        mf = PartialUpdateMergeFunction(key_arity=1, value_arity=2)
        mf.reset()
        mf.add(_kv((1,), 100, RowKind.INSERT, ('a', 'x')))
        mf.add(_kv((1,), 101, RowKind.INSERT, (None, None)))

        result = mf.get_result()
        self.assertEqual(_result_value(result), ('a', 'x'))

    def test_reset_between_keys(self):
        mf = PartialUpdateMergeFunction(key_arity=1, value_arity=2)

        mf.reset()
        mf.add(_kv((1,), 100, RowKind.INSERT, ('a', 'x')))
        first = mf.get_result()
        self.assertEqual(_result_key(first), (1,))
        self.assertEqual(_result_value(first), ('a', 'x'))

        mf.reset()
        mf.add(_kv((2,), 200, RowKind.INSERT, ('b', 'y')))
        second = mf.get_result()
        self.assertEqual(_result_key(second), (2,))
        self.assertEqual(_result_value(second), ('b', 'y'))

    def test_get_result_before_any_add_returns_none(self):
        mf = PartialUpdateMergeFunction(key_arity=1, value_arity=2)
        mf.reset()
        self.assertIsNone(mf.get_result())

    def test_update_after_is_treated_as_insert(self):
        # Java's PartialUpdate accepts UPDATE_AFTER alongside INSERT in
        # non-sequence-group mode (both are "add" kinds). Mirror that.
        mf = PartialUpdateMergeFunction(key_arity=1, value_arity=2)
        mf.reset()
        mf.add(_kv((1,), 100, RowKind.INSERT, ('a', None)))
        mf.add(_kv((1,), 101, RowKind.UPDATE_AFTER, (None, 'x')))

        result = mf.get_result()
        self.assertEqual(_result_value(result), ('a', 'x'))

    def test_delete_row_raises_not_implemented(self):
        mf = PartialUpdateMergeFunction(key_arity=1, value_arity=2)
        mf.reset()
        mf.add(_kv((1,), 100, RowKind.INSERT, ('a', 'x')))
        with self.assertRaises(NotImplementedError) as cm:
            mf.add(_kv((1,), 101, RowKind.DELETE, (None, None)))
        self.assertIn('DELETE', str(cm.exception))
        self.assertIn('ignore-delete', str(cm.exception))

    def test_update_before_row_raises_not_implemented(self):
        mf = PartialUpdateMergeFunction(key_arity=1, value_arity=2)
        mf.reset()
        with self.assertRaises(NotImplementedError) as cm:
            mf.add(_kv((1,), 100, RowKind.UPDATE_BEFORE, (None, None)))
        self.assertIn('UPDATE_BEFORE', str(cm.exception))

    def test_result_is_decoupled_from_input_kv(self):
        """The merge function must build a fresh result tuple — upstream
        readers reuse a single KeyValue instance and call ``replace`` on
        each iteration, so holding a reference to the input is unsafe.
        """
        mf = PartialUpdateMergeFunction(key_arity=1, value_arity=2)
        mf.reset()
        kv = _kv((1,), 100, RowKind.INSERT, ('a', 'x'))
        mf.add(kv)
        result = mf.get_result()

        # Mutate the input's underlying tuple to simulate a reused
        # KeyValue being rebound to a different row.
        kv.replace((999, 999, RowKind.INSERT.value, 'evil', 'evil'))

        # The previously-returned result must NOT be affected.
        self.assertEqual(_result_key(result), (1,))
        self.assertEqual(_result_value(result), ('a', 'x'))

    # -- NOT-NULL input validation (mirrors Java's updateNonNullFields) ----

    def test_first_insert_with_null_for_not_null_field_raises(self):
        """If the very first row writes null to a NOT NULL field, raise —
        same input-validation Java does in updateNonNullFields()."""
        mf = PartialUpdateMergeFunction(
            key_arity=1, value_arity=2, nullables=[True, False])
        mf.reset()
        with self.assertRaises(ValueError) as cm:
            mf.add(_kv((1,), 1, RowKind.INSERT, ('a', None)))
        self.assertIn("Field 1", str(cm.exception))

    def test_subsequent_insert_with_null_for_not_null_field_raises(self):
        """A later null on a NOT NULL field must also raise — Java checks
        on every add(), not just the first one."""
        mf = PartialUpdateMergeFunction(
            key_arity=1, value_arity=2, nullables=[True, False])
        mf.reset()
        mf.add(_kv((1,), 1, RowKind.INSERT, ('a', 'x')))
        with self.assertRaises(ValueError) as cm:
            mf.add(_kv((1,), 2, RowKind.INSERT, (None, None)))
        self.assertIn("Field 1", str(cm.exception))

    def test_null_for_nullable_field_is_absorbed(self):
        """A null input on a nullable field is silently absorbed (existing
        accumulator value wins) — the standard partial-update semantic."""
        mf = PartialUpdateMergeFunction(
            key_arity=1, value_arity=2, nullables=[True, True])
        mf.reset()
        mf.add(_kv((1,), 1, RowKind.INSERT, ('a', 'x')))
        mf.add(_kv((1,), 2, RowKind.INSERT, (None, 'y')))
        result = mf.get_result()
        self.assertEqual(_result_value(result), ('a', 'y'))

    def test_nullables_length_mismatch_raises(self):
        with self.assertRaises(ValueError):
            PartialUpdateMergeFunction(
                key_arity=1, value_arity=2, nullables=[True])

    def test_no_nullables_arg_skips_check(self):
        """Backward-compat: callers that don't pass ``nullables`` get the
        previous behaviour (no NOT-NULL validation)."""
        mf = PartialUpdateMergeFunction(key_arity=1, value_arity=2)
        mf.reset()
        # Would have raised had we declared the second field NOT NULL.
        mf.add(_kv((1,), 1, RowKind.INSERT, ('a', None)))
        result = mf.get_result()
        self.assertEqual(_result_value(result), ('a', None))


if __name__ == '__main__':
    unittest.main()
