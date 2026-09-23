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

"""Direct unit tests for ``FirstRowMergeFunction``.

Drives the merge function with synthetic ``KeyValue`` instances so the
contract is pinned down without going through the full read pipeline.
"""

import unittest

from pypaimon.read.reader.first_row_merge_function import \
    FirstRowMergeFunction
from pypaimon.table.row.key_value import KeyValue
from pypaimon.table.row.row_kind import RowKind


def _kv(key, seq, row_kind, value):
    kv = KeyValue(key_arity=len(key), value_arity=len(value))
    kv.replace(tuple(key) + (seq, row_kind.value) + tuple(value))
    return kv


def _result_value(kv):
    return tuple(kv.value.get_field(i) for i in range(kv.value_arity))


def _result_key(kv):
    return tuple(kv.key.get_field(i) for i in range(kv.key_arity))


class FirstRowMergeFunctionTest(unittest.TestCase):

    def test_single_insert_returns_value(self):
        mf = FirstRowMergeFunction()
        mf.reset()
        mf.add(_kv((1,), 1, RowKind.INSERT, (10, "a")))
        result = mf.get_result()
        self.assertIsNotNone(result)
        self.assertEqual(_result_key(result), (1,))
        self.assertEqual(_result_value(result), (10, "a"))

    def test_keeps_first_row_not_latest(self):
        mf = FirstRowMergeFunction()
        mf.reset()
        mf.add(_kv((1,), 1, RowKind.INSERT, (10, "first")))
        mf.add(_kv((1,), 2, RowKind.INSERT, (20, "second")))
        mf.add(_kv((1,), 3, RowKind.UPDATE_AFTER, (30, "third")))
        result = mf.get_result()
        self.assertEqual(_result_value(result), (10, "first"))

    def test_keeps_first_row_when_kv_is_pooled(self):
        # The writer's fold (KeyValueDataWriter._merge_pending_by_pk) reuses
        # a single KeyValue and replace()s it per row. add() must snapshot
        # the first row; otherwise get_result tracks the pooled kv's last
        # replace() and returns the LAST row -- silently turning first-row
        # into last-row. This is the case the per-row _kv() tests miss.
        mf = FirstRowMergeFunction()
        mf.reset()
        pooled = KeyValue(key_arity=1, value_arity=2)
        pooled.replace((1, 1, RowKind.INSERT.value, 10, "first"))
        mf.add(pooled)
        pooled.replace((1, 2, RowKind.INSERT.value, 20, "second"))
        mf.add(pooled)
        result = mf.get_result()
        self.assertEqual(_result_value(result), (10, "first"))

    def test_reset_clears_state(self):
        mf = FirstRowMergeFunction()
        mf.reset()
        mf.add(_kv((1,), 1, RowKind.INSERT, (10,)))
        self.assertIsNotNone(mf.get_result())

        mf.reset()
        self.assertIsNone(mf.get_result())

        mf.add(_kv((2,), 2, RowKind.INSERT, (20,)))
        result = mf.get_result()
        self.assertEqual(_result_key(result), (2,))
        self.assertEqual(_result_value(result), (20,))

    def test_empty_returns_none(self):
        mf = FirstRowMergeFunction()
        mf.reset()
        self.assertIsNone(mf.get_result())

    def test_delete_raises_by_default(self):
        mf = FirstRowMergeFunction(ignore_delete=False)
        mf.reset()
        with self.assertRaises(ValueError):
            mf.add(_kv((1,), 1, RowKind.DELETE, (10,)))

    def test_update_before_raises_by_default(self):
        mf = FirstRowMergeFunction(ignore_delete=False)
        mf.reset()
        with self.assertRaises(ValueError):
            mf.add(_kv((1,), 1, RowKind.UPDATE_BEFORE, (10,)))

    def test_ignore_delete_skips_retract(self):
        mf = FirstRowMergeFunction(ignore_delete=True)
        mf.reset()
        mf.add(_kv((1,), 1, RowKind.DELETE, (10,)))
        mf.add(_kv((1,), 2, RowKind.INSERT, (20,)))
        result = mf.get_result()
        self.assertIsNotNone(result)
        self.assertEqual(_result_value(result), (20,))

    def test_ignore_delete_skips_update_before(self):
        mf = FirstRowMergeFunction(ignore_delete=True)
        mf.reset()
        mf.add(_kv((1,), 1, RowKind.UPDATE_BEFORE, (10,)))
        self.assertIsNone(mf.get_result())

    def test_ignore_delete_only_retract_returns_none(self):
        mf = FirstRowMergeFunction(ignore_delete=True)
        mf.reset()
        mf.add(_kv((1,), 1, RowKind.DELETE, (10,)))
        mf.add(_kv((1,), 2, RowKind.UPDATE_BEFORE, (20,)))
        self.assertIsNone(mf.get_result())

    def test_update_after_accepted_as_first(self):
        mf = FirstRowMergeFunction()
        mf.reset()
        mf.add(_kv((1,), 1, RowKind.UPDATE_AFTER, (10,)))
        result = mf.get_result()
        self.assertIsNotNone(result)
        self.assertEqual(_result_value(result), (10,))


if __name__ == "__main__":
    unittest.main()
