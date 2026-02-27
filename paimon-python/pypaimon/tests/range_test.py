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

import unittest

from pypaimon.common.predicate import Predicate
from pypaimon.utils.range import Range
from pypaimon.read.scanner.file_scanner import _row_ranges_from_predicate
from pypaimon.table.special_fields import SpecialFields


class RangeTest(unittest.TestCase):

    def test_to_ranges(self):
        assert Range.to_ranges([]) == []
        assert Range.to_ranges([5]) == [Range(5, 5)]
        assert Range.to_ranges([1, 2, 3]) == [Range(1, 3)]
        assert Range.to_ranges([1, 3, 5]) == [
            Range(1, 1), Range(3, 3), Range(5, 5)
        ]
        assert Range.to_ranges([1, 1, 2]) == [Range(1, 1), Range(1, 2)]

    def test_row_ranges_from_predicate(self):
        assert _row_ranges_from_predicate(None) is None

        pred_eq = Predicate('equal', 0, SpecialFields.ROW_ID.name, [5])
        assert _row_ranges_from_predicate(pred_eq) == [Range(5, 5)]

        pred_in = Predicate('in', 0, SpecialFields.ROW_ID.name, [10])
        assert _row_ranges_from_predicate(pred_in) == [Range(10, 10)]
        pred_in_multi = Predicate('in', 0, SpecialFields.ROW_ID.name, [1, 2, 3, 5, 6])
        assert _row_ranges_from_predicate(pred_in_multi) == [Range(1, 3), Range(5, 6)]

        pred_between = Predicate('between', 0, SpecialFields.ROW_ID.name, [10, 20])
        result = _row_ranges_from_predicate(pred_between)
        assert result is not None and result == [Range(10, 20)]

        pred_other = Predicate('equal', 0, 'other_field', [5])
        assert _row_ranges_from_predicate(pred_other) is None

        pred_gt = Predicate('greaterThan', 0, SpecialFields.ROW_ID.name, [10])
        assert _row_ranges_from_predicate(pred_gt) is None

        assert _row_ranges_from_predicate(
            Predicate('equal', 0, SpecialFields.ROW_ID.name, [])
        ) == []
        assert _row_ranges_from_predicate(
            Predicate('in', 0, SpecialFields.ROW_ID.name, [])
        ) == []
        assert _row_ranges_from_predicate(
            Predicate('between', 0, SpecialFields.ROW_ID.name, [10])
        ) == []

        pred_eq5 = Predicate('equal', 0, SpecialFields.ROW_ID.name, [5])
        pred_between_1_10 = Predicate('between', 0, SpecialFields.ROW_ID.name, [1, 10])
        pred_and = Predicate('and', None, None, [pred_eq5, pred_between_1_10])
        assert _row_ranges_from_predicate(pred_and) == [Range(5, 5)]
        pred_eq3 = Predicate('equal', 0, SpecialFields.ROW_ID.name, [3])
        pred_and_no = Predicate('and', None, None, [pred_eq5, pred_eq3])
        assert _row_ranges_from_predicate(pred_and_no) == []

        pred_or = Predicate('or', None, None, [pred_eq5, pred_eq3])
        assert _row_ranges_from_predicate(pred_or) == [Range(3, 3), Range(5, 5)]
        pred_between_1_5 = Predicate('between', 0, SpecialFields.ROW_ID.name, [1, 5])
        pred_between_3_7 = Predicate('between', 0, SpecialFields.ROW_ID.name, [3, 7])
        pred_or_overlap = Predicate('or', None, None, [pred_between_1_5, pred_between_3_7])
        assert _row_ranges_from_predicate(pred_or_overlap) == [Range(1, 7)]


if __name__ == '__main__':
    unittest.main()
