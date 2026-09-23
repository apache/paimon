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

import unittest
import warnings
from unittest.mock import Mock

from pypaimon.common.options.core_options import CoreOptions, StartupMode
from pypaimon.read.table_scan import TableScan


def _scan(options):
    scan = TableScan.__new__(TableScan)
    scan.table = Mock()
    scan.table.options = CoreOptions.from_dict(options)
    return scan


class TableScanModeTest(unittest.TestCase):

    def test_from_timestamp_requires_timestamp_option(self):
        scan = _scan({
            CoreOptions.SCAN_MODE.key(): StartupMode.FROM_TIMESTAMP.value,
        })

        with self.assertRaisesRegex(
                ValueError,
                "neither scan.timestamp-millis nor scan.timestamp is set"):
            scan._validate_scan_mode()

    def test_latest_conflicts_with_snapshot_id(self):
        scan = _scan({
            CoreOptions.SCAN_MODE.key(): StartupMode.LATEST.value,
            CoreOptions.SCAN_SNAPSHOT_ID.key(): "1",
        })

        with self.assertRaisesRegex(ValueError, "scan.snapshot-id"):
            scan._validate_scan_mode()

    def test_default_with_timestamp_millis_resolves_to_from_timestamp(self):
        options = CoreOptions.from_dict({
            CoreOptions.SCAN_MODE.key(): StartupMode.DEFAULT.value,
            CoreOptions.SCAN_TIMESTAMP_MILLIS.key(): "123",
        })

        self.assertEqual(options.startup_mode(), StartupMode.FROM_TIMESTAMP)
        _scan(options.options.to_map())._validate_scan_mode()

    def test_default_with_snapshot_id_resolves_to_from_snapshot(self):
        options = CoreOptions.from_dict({
            CoreOptions.SCAN_MODE.key(): StartupMode.DEFAULT.value,
            CoreOptions.SCAN_SNAPSHOT_ID.key(): "1",
        })

        self.assertEqual(options.startup_mode(), StartupMode.FROM_SNAPSHOT)
        _scan(options.options.to_map())._validate_scan_mode()

    def test_unsupported_scan_modes_raise_value_error(self):
        scan = _scan({
            CoreOptions.SCAN_MODE.key(): StartupMode.COMPACTED_FULL.value,
        })

        with self.assertRaisesRegex(ValueError, "not yet supported"):
            scan._validate_scan_mode()

    def test_full_mode_maps_to_latest_full_with_deprecation_warning(self):
        options = CoreOptions.from_dict({
            CoreOptions.SCAN_MODE.key(): StartupMode.FULL.value,
        })

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.assertEqual(options.startup_mode(), StartupMode.LATEST_FULL)

        self.assertEqual(len(caught), 1)
        self.assertTrue(issubclass(caught[0].category, DeprecationWarning))


if __name__ == '__main__':
    unittest.main()
