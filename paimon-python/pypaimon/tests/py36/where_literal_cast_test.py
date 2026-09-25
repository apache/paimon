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

"""WHERE literal casting on the Python 3.6/3.7 lane.

The full ``where_parser_test`` is excluded from the 3.6/3.7 subset, yet DATE and
TIME casting must stay off ``date.fromisoformat`` / ``time.fromisoformat`` (both
3.7+). These pure-casting checks run on that lane to guard the floor.
"""

import datetime
import decimal
import unittest

from pypaimon.common.where_parser import _cast_literal


class WhereLiteralCastPy36Test(unittest.TestCase):

    def test_cast_date(self):
        self.assertEqual(_cast_literal('2024-01-01', 'DATE'),
                         datetime.date(2024, 1, 1))

    def test_cast_time(self):
        self.assertEqual(_cast_literal('12:30:00', 'TIME(0)'),
                         datetime.time(12, 30, 0))

    def test_cast_time_with_fraction(self):
        self.assertEqual(_cast_literal('12:30:00.5', 'TIME(3)'),
                         datetime.time(12, 30, 0, 500000))

    def test_cast_time_rejects_offset(self):
        with self.assertRaises(ValueError):
            _cast_literal('12:30:00+01:00', 'TIME(0)')

    def test_cast_date_rejects_malformed(self):
        with self.assertRaises(ValueError):
            _cast_literal('not-a-date', 'DATE')

    def test_cast_high_precision_decimal_rescales(self):
        value = _cast_literal('123456789012345678901234567890123456',
                              'DECIMAL(38, 2)')
        self.assertEqual(value.as_tuple().exponent, -2)
        self.assertEqual(
            value, decimal.Decimal('123456789012345678901234567890123456.00'))


if __name__ == '__main__':
    unittest.main()
