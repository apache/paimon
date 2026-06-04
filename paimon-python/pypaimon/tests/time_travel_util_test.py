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
    def __init__(self, snapshot_id, schema_id=0, time_millis=0, watermark=None):
        self.id = snapshot_id
        self.schema_id = schema_id
        self.time_millis = time_millis
        self.watermark = watermark


class _StubSnapshotManager:
    def __init__(self, snapshots):
        self._snapshots = {s.id: s for s in snapshots}

    def get_snapshot_by_id(self, snapshot_id):
        return self._snapshots.get(snapshot_id)

    def try_get_earliest_snapshot(self):
        if not self._snapshots:
            return None
        return self._snapshots[min(self._snapshots.keys())]

    def get_latest_snapshot(self):
        if not self._snapshots:
            return None
        return self._snapshots[max(self._snapshots.keys())]

    def earlier_or_equal_time_mills(self, timestamp):
        result = None
        for snap in sorted(self._snapshots.values(), key=lambda s: s.id):
            if snap.time_millis <= timestamp:
                result = snap
            else:
                break
        return result

    def later_or_equal_watermark(self, watermark):
        for snap in sorted(self._snapshots.values(), key=lambda s: s.id):
            if snap.watermark is not None and snap.watermark >= watermark:
                return snap
        return None


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
        # Sanity check: SCAN_KEYS must enumerate all time-travel modes,
        # otherwise the mutual-exclusion guard above would not trigger.
        self.assertIn('scan.snapshot-id', SCAN_KEYS)
        self.assertIn('scan.tag-name', SCAN_KEYS)
        self.assertIn('scan.timestamp-millis', SCAN_KEYS)
        self.assertIn('scan.timestamp', SCAN_KEYS)
        self.assertIn('scan.watermark', SCAN_KEYS)

    def test_resolves_timestamp_millis(self):
        snap1 = _StubSnapshot(1, time_millis=1000)
        snap2 = _StubSnapshot(2, time_millis=2000)
        snap3 = _StubSnapshot(3, time_millis=3000)
        mgr = _StubSnapshotManager([snap1, snap2, snap3])
        result = TimeTravelUtil.try_travel_to_snapshot(
            Options({'scan.timestamp-millis': '2500'}),
            _StubTagManager({}),
            mgr,
        )
        self.assertIs(result, snap2)

    def test_resolves_timestamp_millis_exact_match(self):
        snap1 = _StubSnapshot(1, time_millis=1000)
        snap2 = _StubSnapshot(2, time_millis=2000)
        mgr = _StubSnapshotManager([snap1, snap2])
        result = TimeTravelUtil.try_travel_to_snapshot(
            Options({'scan.timestamp-millis': '2000'}),
            _StubTagManager({}),
            mgr,
        )
        self.assertIs(result, snap2)

    def test_timestamp_millis_no_match_raises(self):
        snap1 = _StubSnapshot(1, time_millis=5000)
        mgr = _StubSnapshotManager([snap1])
        with self.assertRaises(ValueError):
            TimeTravelUtil.try_travel_to_snapshot(
                Options({'scan.timestamp-millis': '1000'}),
                _StubTagManager({}),
                mgr,
            )

    def test_timestamp_millis_without_snapshot_manager_raises(self):
        with self.assertRaises(ValueError):
            TimeTravelUtil.try_travel_to_snapshot(
                Options({'scan.timestamp-millis': '1000'}),
                _StubTagManager({}),
                None,
            )

    def test_resolves_timestamp_string(self):
        # Compute expected millis in local timezone, consistent with Java behavior
        from datetime import datetime
        expected_millis = int(datetime(2023, 12, 1, 0, 0, 0).timestamp() * 1000)
        snap1 = _StubSnapshot(1, time_millis=expected_millis)
        snap2 = _StubSnapshot(2, time_millis=expected_millis + 100000)
        mgr = _StubSnapshotManager([snap1, snap2])
        result = TimeTravelUtil.try_travel_to_snapshot(
            Options({'scan.timestamp': '2023-12-01 00:00:00'}),
            _StubTagManager({}),
            mgr,
        )
        self.assertIs(result, snap1)

    def test_timestamp_string_invalid_format_raises(self):
        snap1 = _StubSnapshot(1, time_millis=1000)
        mgr = _StubSnapshotManager([snap1])
        with self.assertRaises(ValueError):
            TimeTravelUtil.try_travel_to_snapshot(
                Options({'scan.timestamp': 'not-a-timestamp'}),
                _StubTagManager({}),
                mgr,
            )

    def test_resolves_watermark(self):
        snap1 = _StubSnapshot(1, watermark=100)
        snap2 = _StubSnapshot(2, watermark=200)
        snap3 = _StubSnapshot(3, watermark=300)
        mgr = _StubSnapshotManager([snap1, snap2, snap3])
        result = TimeTravelUtil.try_travel_to_snapshot(
            Options({'scan.watermark': '200'}),
            _StubTagManager({}),
            mgr,
        )
        self.assertIs(result, snap2)

    def test_watermark_no_match_raises(self):
        snap1 = _StubSnapshot(1, watermark=100)
        mgr = _StubSnapshotManager([snap1])
        with self.assertRaises(ValueError):
            TimeTravelUtil.try_travel_to_snapshot(
                Options({'scan.watermark': '500'}),
                _StubTagManager({}),
                mgr,
            )

    def test_watermark_without_snapshot_manager_raises(self):
        with self.assertRaises(ValueError):
            TimeTravelUtil.try_travel_to_snapshot(
                Options({'scan.watermark': '100'}),
                _StubTagManager({}),
                None,
            )

    def test_rejects_multiple_time_travel_options(self):
        mgr = _StubSnapshotManager([_StubSnapshot(1, time_millis=1000)])
        with self.assertRaises(ValueError):
            TimeTravelUtil.try_travel_to_snapshot(
                Options({'scan.timestamp-millis': '1000', 'scan.snapshot-id': '1'}),
                _StubTagManager({}),
                mgr,
            )


if __name__ == '__main__':
    unittest.main()
