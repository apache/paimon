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

"""Unit tests for ``pypaimon.common.merge_engine_dispatch``.

Pins down the table-option parsing the dispatch uses to decide whether
``partial-update`` should run or be rejected. The key contract: strict
``"true"``-only boolean parsing aligned with the table-option parser
used elsewhere in Paimon, so an option string the rest of the
toolchain treats as ``false`` is not silently elevated to ``true``
here.
"""

import unittest

from pypaimon.common.merge_engine_dispatch import (
    _option_is_truthy,
    build_merge_function,
    partial_update_unsupported_options,
)
from pypaimon.common.options.core_options import MergeEngine
from pypaimon.read.reader.partial_update_merge_function import \
    PartialUpdateMergeFunction


class OptionIsTruthyTest(unittest.TestCase):
    """``_option_is_truthy`` delegates to ``OptionsUtils.convert_to_boolean``
    (the parser behind ``CoreOptions.ignore_delete()``): ``true`` / ``1`` /
    ``yes`` / ``on`` are true and ``false`` / ``0`` / ``no`` / ``off`` are
    false (case-insensitive), unset is false, and an unrecognized spelling
    raises -- so a table option the rest of the toolchain treats as true is
    honored here too, not silently downgraded.
    """

    def test_canonical_true_spellings(self):
        for v in ("true", "TRUE", "True", "tRuE", "1", "yes", "YES", "on",
                  "ON", "  true  ", "  yes  "):
            self.assertTrue(_option_is_truthy(v), v)

    def test_canonical_false_spellings(self):
        for v in ("false", "FALSE", "False", "0", "no", "NO", "off", "OFF",
                  "  false  "):
            self.assertFalse(_option_is_truthy(v), v)

    def test_python_bool_true_is_truthy(self):
        self.assertTrue(_option_is_truthy(True))

    def test_python_bool_false_is_falsey(self):
        self.assertFalse(_option_is_truthy(False))

    def test_none_is_falsey(self):
        self.assertFalse(_option_is_truthy(None))

    def test_unrecognized_spelling_raises(self):
        # convert_to_boolean rejects a non-canonical spelling rather than
        # silently treating it as false, matching CoreOptions.ignore_delete().
        for v in ("y", "t", "maybe", ""):
            with self.assertRaises(ValueError):
                _option_is_truthy(v)


class PartialUpdateUnsupportedOptionsTest(unittest.TestCase):

    def test_ignore_delete_yes_is_not_flagged(self):
        # ignore-delete is a supported option (retract rows are skipped), so
        # it is never flagged regardless of spelling. ``yes`` in particular
        # is canonical-true and must wire ignore_delete=True downstream (see
        # BuildMergeFunctionTest), not be silently downgraded.
        unsupported = partial_update_unsupported_options(
            {"partial-update.ignore-delete": "yes"})
        self.assertEqual(unsupported, set())

    def test_ignore_delete_true_is_not_flagged(self):
        # ignore-delete is now supported (retract rows are skipped), so it
        # must not force the dispatch to refuse the table.
        unsupported = partial_update_unsupported_options(
            {"partial-update.ignore-delete": "true"})
        self.assertEqual(unsupported, set())
        self.assertEqual(
            partial_update_unsupported_options({"ignore-delete": "true"}),
            set())

    def test_remove_record_on_delete_is_still_flagged(self):
        unsupported = partial_update_unsupported_options(
            {"partial-update.remove-record-on-delete": "true"})
        self.assertEqual(
            unsupported, {"partial-update.remove-record-on-delete"})
        # A canonical-true spelling other than "true" must also be flagged --
        # the guard now shares the boolean parser, so "yes" is not silently
        # treated as false and allowed through.
        self.assertEqual(
            partial_update_unsupported_options(
                {"partial-update.remove-record-on-delete": "yes"}),
            {"partial-update.remove-record-on-delete"})

    def test_sequence_group_is_flagged(self):
        unsupported = partial_update_unsupported_options(
            {"fields.a.sequence-group": "b"})
        self.assertEqual(unsupported, {"fields.a.sequence-group"})

    def test_unrelated_options_are_not_flagged(self):
        unsupported = partial_update_unsupported_options(
            {"bucket": "1", "merge-engine": "partial-update"})
        self.assertEqual(unsupported, set())


class BuildMergeFunctionTest(unittest.TestCase):
    """``build_merge_function`` forwards ``value_field_names`` to
    ``PartialUpdateMergeFunction`` so the NOT-NULL error message can
    surface the offending column name. This is the only behavioural
    contract the dispatch adds on top of routing.
    """

    def test_partial_update_forwards_field_names(self):
        mf = build_merge_function(
            engine=MergeEngine.PARTIAL_UPDATE,
            raw_options={},
            key_arity=1,
            value_arity=2,
            value_field_nullables=[True, True],
            value_field_names=['col_a', 'col_b'],
        )
        self.assertIsInstance(mf, PartialUpdateMergeFunction)
        self.assertEqual(mf._value_field_names, ['col_a', 'col_b'])

    def test_partial_update_without_field_names_keeps_none(self):
        mf = build_merge_function(
            engine=MergeEngine.PARTIAL_UPDATE,
            raw_options={},
            key_arity=1,
            value_arity=2,
            value_field_nullables=[True, True],
        )
        self.assertIsInstance(mf, PartialUpdateMergeFunction)
        self.assertIsNone(mf._value_field_names)

    def test_partial_update_wires_ignore_delete(self):
        # ignore-delete on the table flows into the merge function, which
        # then skips retract rows instead of raising.
        from pypaimon.table.row.key_value import KeyValue
        from pypaimon.table.row.row_kind import RowKind

        mf = build_merge_function(
            engine=MergeEngine.PARTIAL_UPDATE,
            raw_options={"partial-update.ignore-delete": "true"},
            key_arity=1,
            value_arity=1,
            value_field_nullables=[True],
        )
        self.assertTrue(mf._ignore_delete)
        mf.reset()
        delete = KeyValue(key_arity=1, value_arity=1)
        delete.replace((1, 100, RowKind.DELETE.value, 'x'))
        mf.add(delete)  # must not raise
        self.assertIsNone(mf.get_result())

    def test_partial_update_ignore_delete_canonical_true_spellings(self):
        # Regression: ignore-delete=yes/1/on are canonical-true, so each must
        # wire ignore_delete=True. Pre-fix the strict "true"-only parser built
        # ignore_delete=False for these, and the first retract then raised.
        for spelling in ("yes", "1", "on", "TRUE"):
            mf = build_merge_function(
                engine=MergeEngine.PARTIAL_UPDATE,
                raw_options={"partial-update.ignore-delete": spelling},
                key_arity=1,
                value_arity=1,
                value_field_nullables=[True],
            )
            self.assertTrue(mf._ignore_delete, spelling)

    def test_partial_update_ignore_delete_canonical_false_spellings(self):
        for spelling in ("no", "0", "off", "false"):
            mf = build_merge_function(
                engine=MergeEngine.PARTIAL_UPDATE,
                raw_options={"partial-update.ignore-delete": spelling},
                key_arity=1,
                value_arity=1,
                value_field_nullables=[True],
            )
            self.assertFalse(mf._ignore_delete, spelling)

    def test_partial_update_ignore_delete_default_off(self):
        mf = build_merge_function(
            engine=MergeEngine.PARTIAL_UPDATE,
            raw_options={},
            key_arity=1,
            value_arity=2,
            value_field_nullables=[True, True],
        )
        self.assertFalse(mf._ignore_delete)


if __name__ == '__main__':
    unittest.main()
