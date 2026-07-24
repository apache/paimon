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

"""Regression tests for the scalar global-index search mode default.

Scalar (sorted/bitmap) global-index queries must default to a coverage-aware
mode (``full``) so that data rows whose row-id range is not yet covered by the
global index are still read and residual-filtered, instead of being silently
pruned (which made ``WHERE <indexed_col> = X`` return incomplete results when
the index build lagged behind writes). Vector/full-text queries keep the
performance-oriented ``fast`` default.
"""

import unittest
from types import SimpleNamespace

from pypaimon.common.options.core_options import CoreOptions, GlobalIndexSearchMode
from pypaimon.common.options.options import Options
from pypaimon.globalindex.data_evolution_global_index_coverage import (
    DataEvolutionGlobalIndexCoverage,
)
from pypaimon.globalindex.data_evolution_global_index_scanner import (
    DataEvolutionGlobalIndexScanner,
)


def _ranges(result):
    inner = result.results() if hasattr(result, "results") else result
    for attr in ("to_range_list", "to_ranges", "ranges"):
        if hasattr(inner, attr):
            value = getattr(inner, attr)
            value = value() if callable(value) else value
            return [(r.from_, r.to) for r in value]
    raise AssertionError("cannot extract ranges from %r" % (result,))


def _coverage(options):
    # Global index covers row-ids [0, 99] for field 1; the table holds data up
    # to next_row_id = 200, so [100, 199] is committed but not yet indexed.
    meta = SimpleNamespace(
        row_range_start=0, row_range_end=99, index_field_id=1, extra_field_ids=None)
    snapshot = SimpleNamespace(next_row_id=200)
    table = SimpleNamespace(options=options)
    return DataEvolutionGlobalIndexCoverage(
        table, snapshot, None, [SimpleNamespace(global_index_meta=meta)])


class ScalarGlobalIndexSearchModeTest(unittest.TestCase):

    def test_default_values(self):
        options = CoreOptions(Options.from_none())
        self.assertEqual(
            GlobalIndexSearchMode.FULL, options.global_index_scalar_search_mode())
        self.assertEqual(
            GlobalIndexSearchMode.FAST, options.global_index_search_mode())

    def test_scalar_option_override(self):
        options = CoreOptions(Options({"scalar-index.search-mode": "detail"}))
        self.assertEqual(
            GlobalIndexSearchMode.DETAIL, options.global_index_scalar_search_mode())

    def test_inherits_explicit_global_index_search_mode(self):
        # No scalar key set, but the old global-index key is explicit -> inherit.
        options = CoreOptions(Options({"global-index.search-mode": "detail"}))
        self.assertEqual(
            GlobalIndexSearchMode.DETAIL, options.global_index_scalar_search_mode())

    def test_scalar_key_wins_over_global_index_key(self):
        options = CoreOptions(Options({
            "scalar-index.search-mode": "detail",
            "global-index.search-mode": "fast",
        }))
        self.assertEqual(
            GlobalIndexSearchMode.DETAIL, options.global_index_scalar_search_mode())

    def test_coverage_honours_search_mode_override(self):
        coverage = _coverage(CoreOptions(Options.from_none()))
        # fast -> no fallback (uncovered rows would be pruned)
        self.assertEqual(
            [], coverage.unindexed_ranges(1, search_mode=GlobalIndexSearchMode.FAST))
        # full -> uncovered [100, 199] recovered
        full = coverage.unindexed_ranges(1, search_mode=GlobalIndexSearchMode.FULL)
        self.assertEqual([(100, 199)], [(r.from_, r.to) for r in full])
        # no override -> keeps the general search-mode (still FAST) for back-compat
        self.assertEqual([], coverage.unindexed_ranges(1))

    def test_scanner_applies_passed_scalar_mode(self):
        # Pure scalar callers pass the scalar mode explicitly; FULL recovers the
        # uncovered rows.
        coverage = _coverage(CoreOptions(Options.from_none()))
        scanner = SimpleNamespace(_coverage=coverage, _fields=[1])
        result = DataEvolutionGlobalIndexScanner.unindexed_rows(
            scanner, None, search_mode=GlobalIndexSearchMode.FULL)
        self.assertEqual([(100, 199)], _ranges(result))

    def test_scanner_default_is_general_mode(self):
        # Vector/full-text callers leave search_mode unset -> general (fast) mode,
        # so the scan is not widened.
        coverage = _coverage(CoreOptions(Options.from_none()))
        scanner = SimpleNamespace(_coverage=coverage, _fields=[1])
        result = DataEvolutionGlobalIndexScanner.unindexed_rows(scanner, None)
        self.assertEqual([], _ranges(result))


if __name__ == "__main__":
    unittest.main()
