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

"""Unit tests for Tag TTL serialization and the temporal codecs.

These pin down the on-disk JSON shape against the Java side
(``org.apache.paimon.tag.Tag`` / ``TagTest``) so tag files stay
interoperable across the Java and Python SDKs.
"""

import json
import unittest
from datetime import datetime, timedelta

from pypaimon.common.json_util import JSON
from pypaimon.common.time_utils import (
    duration_to_iso8601,
    duration_to_json_seconds,
    json_array_to_local_datetime,
    json_seconds_to_duration,
    local_datetime_to_json_array,
    local_datetime_to_millis,
)
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.tag.tag import Tag


def _snapshot():
    return Snapshot(
        version=3,
        id=0,
        schema_id=0,
        base_manifest_list=None,
        delta_manifest_list=None,
        total_record_count=0,
        delta_record_count=0,
        commit_user=None,
        commit_identifier=0,
        commit_kind="APPEND",
        time_millis=1000,
    )


class TemporalCodecTest(unittest.TestCase):

    def test_local_datetime_array_round_trip(self):
        dt = datetime(2024, 6, 25, 10, 30, 45, 123456)
        arr = local_datetime_to_json_array(dt)
        # [year, month, day, hour, minute, second, nanoOfSecond]
        self.assertEqual([2024, 6, 25, 10, 30, 45, 123456000], arr)
        self.assertEqual(dt, json_array_to_local_datetime(arr))

    def test_local_datetime_array_pads_missing_components(self):
        # Jackson omits trailing zero components; missing ones default to 0.
        self.assertEqual(
            datetime(1969, 1, 1, 0, 0, 0, 0),
            json_array_to_local_datetime([1969, 1, 1]),
        )

    def test_local_datetime_truncates_java_nanos_to_micros(self):
        # Java nanoOfSecond (9 digits) is truncated to microseconds.
        dt = json_array_to_local_datetime([1969, 1, 1, 0, 0, 0, 123456789])
        self.assertEqual(123456, dt.microsecond)

    def test_duration_seconds_round_trip(self):
        td = timedelta(days=1)
        self.assertEqual(86400.0, duration_to_json_seconds(td))
        self.assertEqual(td, json_seconds_to_duration(86400.0))
        # Java writes "5.000000000"; json parses it as 5.0.
        self.assertEqual(timedelta(seconds=5), json_seconds_to_duration(5.0))

    def test_duration_to_iso8601(self):
        self.assertEqual("PT24H", duration_to_iso8601(timedelta(days=1)))
        self.assertEqual("PT30M", duration_to_iso8601(timedelta(minutes=30)))
        self.assertEqual("PT5S", duration_to_iso8601(timedelta(seconds=5)))
        self.assertEqual("PT0S", duration_to_iso8601(timedelta(0)))
        self.assertEqual(
            "PT1H30M15S",
            duration_to_iso8601(timedelta(hours=1, minutes=30, seconds=15)),
        )
        self.assertEqual(
            "PT0.5S", duration_to_iso8601(timedelta(milliseconds=500)))
        # Fractional seconds combined with hour/minute components.
        self.assertEqual(
            "PT1H30M0.5S",
            duration_to_iso8601(
                timedelta(hours=1, minutes=30, milliseconds=500)),
        )
        self.assertEqual(
            "PT12H", duration_to_iso8601(timedelta(hours=12)))

    def test_local_datetime_to_millis_pre_epoch_floors(self):
        # Pre-1970 with sub-millisecond is floored (matching Java
        # Instant.toEpochMilli), not truncated toward zero.
        dt = datetime(1969, 12, 31, 23, 59, 59, 500000)
        self.assertEqual(-500, local_datetime_to_millis(dt))

    def test_local_datetime_to_millis(self):
        dt = datetime(1970, 1, 1, 0, 0, 1)
        self.assertEqual(1000, local_datetime_to_millis(dt))


class TagSerdeTest(unittest.TestCase):

    def test_no_ttl_serializes_as_plain_snapshot(self):
        # A Tag without TTL must not emit the tag-specific fields.
        snap = _snapshot()
        parsed = json.loads(JSON.to_json(snap))
        self.assertNotIn("tagCreateTime", parsed)
        self.assertNotIn("tagTimeRetained", parsed)

    def test_ttl_tag_matches_java_on_disk_shape(self):
        tag = Tag.from_snapshot_and_tag_ttl(
            _snapshot(),
            timedelta(seconds=5),
            datetime(1969, 1, 1, 0, 0, 0, 123456),
        )
        parsed = json.loads(JSON.to_json(tag))
        self.assertEqual([1969, 1, 1, 0, 0, 0, 123456000],
                         parsed["tagCreateTime"])
        self.assertEqual(5.0, parsed["tagTimeRetained"])

    def test_round_trip(self):
        tag = Tag.from_snapshot_and_tag_ttl(
            _snapshot(), timedelta(days=1), datetime(2024, 6, 25, 10, 30, 45))
        restored = JSON.from_json(JSON.to_json(tag), Tag)
        self.assertEqual(datetime(2024, 6, 25, 10, 30, 45),
                         restored.tag_create_time)
        self.assertEqual(timedelta(days=1), restored.tag_time_retained)

    def test_reads_java_written_tag(self):
        # A tag file produced by Java: array create-time + decimal seconds.
        java_json = json.dumps({
            "version": 3, "id": 0, "schemaId": 0,
            "baseManifestList": None, "deltaManifestList": None,
            "totalRecordCount": 0, "deltaRecordCount": 0,
            "commitUser": None, "commitIdentifier": 0,
            "commitKind": "APPEND", "timeMillis": 1000,
            "tagCreateTime": [1969, 1, 1, 0, 0, 0, 123456789],
            "tagTimeRetained": 5.000000000,
        })
        tag = JSON.from_json(java_json, Tag)
        self.assertEqual(
            datetime(1969, 1, 1, 0, 0, 0, 123456), tag.tag_create_time)
        self.assertEqual(timedelta(seconds=5), tag.tag_time_retained)

    def test_reads_legacy_plain_snapshot_tag(self):
        # Old tag files (plain Snapshot JSON) load with None TTL fields.
        tag = JSON.from_json(JSON.to_json(_snapshot()), Tag)
        self.assertIsNone(tag.tag_create_time)
        self.assertIsNone(tag.tag_time_retained)

    def test_trim_to_snapshot_drops_tag_fields(self):
        tag = Tag.from_snapshot_and_tag_ttl(
            _snapshot(), timedelta(days=1), datetime(2024, 1, 1))
        snap = tag.trim_to_snapshot()
        self.assertNotIsInstance(snap, Tag)
        self.assertFalse(hasattr(snap, "tag_create_time"))


if __name__ == "__main__":
    unittest.main()
