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

from pypaimon.common.options.options import Options
from pypaimon.snapshot.time_travel_util import SCAN_KEYS, TimeTravelUtil


class _StubSnapshot:
    def __init__(self, snapshot_id, schema_id=0):
        self.id = snapshot_id
        self.schema_id = schema_id


class _StubSnapshotManager:
    def __init__(self, snapshots):
        self._snapshots = {s.id: s for s in snapshots}

    def get_snapshot_by_id(self, snapshot_id):
        return self._snapshots.get(snapshot_id)


class _StubTagManager:
    def __init__(self, tags):
        self._tags = tags

    def get(self, name):
        return self._tags.get(name)


class TimeTravelUtilTest(unittest.TestCase):

    def test_returns_none_when_no_scan_option_set(self):
        result = TimeTravelUtil.try_travel_to_snapshot(
            Options({}), _StubTagManager({}), _StubSnapshotManager([])
        )
        self.assertIsNone(result)

    def test_resolves_snapshot_id(self):
        snap2 = _StubSnapshot(2)
        result = TimeTravelUtil.try_travel_to_snapshot(
            Options({'scan.snapshot-id': '2'}),
            _StubTagManager({}),
            _StubSnapshotManager([_StubSnapshot(1), snap2]),
        )
        self.assertIs(result, snap2)

    def test_unknown_snapshot_id_raises(self):
        with self.assertRaises(ValueError):
            TimeTravelUtil.try_travel_to_snapshot(
                Options({'scan.snapshot-id': '99'}),
                _StubTagManager({}),
                _StubSnapshotManager([_StubSnapshot(1)]),
            )

    def test_snapshot_id_without_snapshot_manager_raises(self):
        with self.assertRaises(ValueError):
            TimeTravelUtil.try_travel_to_snapshot(
                Options({'scan.snapshot-id': '1'}),
                _StubTagManager({}),
                None,
            )

    def test_rejects_setting_snapshot_id_and_tag_name_together(self):
        # Defence-in-depth: even if a caller bypasses read_paimon's check,
        # the util-level guard still complains.
        with self.assertRaises(ValueError):
            TimeTravelUtil.try_travel_to_snapshot(
                Options({'scan.snapshot-id': '1', 'scan.tag-name': 'v1'}),
                _StubTagManager({}),
                _StubSnapshotManager([_StubSnapshot(1)]),
            )

    def test_scan_keys_contains_both_options(self):
        # Sanity check: SCAN_KEYS must enumerate both time-travel modes,
        # otherwise the mutual-exclusion guard above would not trigger.
        self.assertIn('scan.snapshot-id', SCAN_KEYS)
        self.assertIn('scan.tag-name', SCAN_KEYS)


if __name__ == '__main__':
    unittest.main()
