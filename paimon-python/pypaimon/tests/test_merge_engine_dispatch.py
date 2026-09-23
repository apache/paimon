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
    """``_option_is_truthy`` accepts only ``"true"`` (case-insensitive)
    as truthy; every other string -- including ``"1"``, ``"yes"``,
    ``"on"`` -- is falsey. Matches the table-option parser used
    elsewhere in Paimon.
    """

    def test_true_string_is_truthy(self):
        self.assertTrue(_option_is_truthy("true"))

    def test_true_string_is_case_insensitive(self):
        for v in ("TRUE", "True", "tRuE"):
            self.assertTrue(_option_is_truthy(v), v)

    def test_true_string_tolerates_surrounding_whitespace(self):
        self.assertTrue(_option_is_truthy("  true  "))

    def test_python_bool_true_is_truthy(self):
        self.assertTrue(_option_is_truthy(True))

    def test_python_bool_false_is_falsey(self):
        self.assertFalse(_option_is_truthy(False))

    def test_none_is_falsey(self):
        self.assertFalse(_option_is_truthy(None))

    def test_non_true_strings_are_falsey(self):
        # The table-option parser elsewhere in Paimon returns false
        # for every one of these. pypaimon must do the same so a
        # user-set "yes" is not silently elevated to true here while
        # the rest of the toolchain treats it as false.
        for v in ("1", "yes", "on", "Yes", "ON", "y", "t", "0", "no", "off",
                  "false", "FALSE", ""):
            self.assertFalse(_option_is_truthy(v), v)


class PartialUpdateUnsupportedOptionsTest(unittest.TestCase):

    def test_ignore_delete_yes_is_not_flagged(self):
        # ``yes`` is falsey under the upstream table-option parser,
        # so partial-update must NOT be blocked here. Pre-fix pypaimon
        # rejected this; the fix aligns the dispatch with the parser.
        unsupported = partial_update_unsupported_options(
            {"partial-update.ignore-delete": "yes"})
        self.assertEqual(unsupported, set())

    def test_ignore_delete_true_is_flagged(self):
        unsupported = partial_update_unsupported_options(
            {"partial-update.ignore-delete": "true"})
        self.assertEqual(unsupported, {"partial-update.ignore-delete"})

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


if __name__ == '__main__':
    unittest.main()
