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

import dataclasses
import json
import os
import time
import unittest
from datetime import datetime, timedelta, timezone

from pypaimon.common.json_util import JSON
from pypaimon.common.time_utils import (
    duration_to_iso8601,
    duration_to_json_seconds,
    json_array_to_local_datetime,
    json_seconds_to_duration,
    local_datetime_to_json_array,
    local_datetime_to_millis,
    local_datetime_to_system_zone_millis,
    parse_duration_nanos,
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


def _full_snapshot():
    """A Snapshot with every optional field set to a distinctive non-None value,
    so a trim that silently drops any of them is caught."""
    return Snapshot(
        version=3,
        id=7,
        schema_id=2,
        base_manifest_list="base-list",
        delta_manifest_list="delta-list",
        total_record_count=100,
        delta_record_count=10,
        commit_user="u",
        commit_identifier=42,
        commit_kind="APPEND",
        time_millis=1000,
        base_manifest_list_size=111,
        delta_manifest_list_size=222,
        changelog_manifest_list="changelog-list",
        changelog_manifest_list_size=333,
        index_manifest="index-manifest",
        changelog_record_count=5,
        watermark=123456789,
        statistics="stats",
        next_row_id=900,
        properties={"k": "v"},
    )


class TemporalCodecTest(unittest.TestCase):

    def test_local_datetime_array_round_trip(self):
        dt = datetime(2024, 6, 25, 10, 30, 45, 123456)
        arr = local_datetime_to_json_array(dt)
        # [year, month, day, hour, minute, second, nanoOfSecond]
        self.assertEqual([2024, 6, 25, 10, 30, 45, 123456000], arr)
        self.assertEqual(dt, json_array_to_local_datetime(arr))

    def test_local_datetime_array_omits_trailing_zero_components(self):
        # Match Jackson's LocalDateTimeSerializer byte-for-byte: a whole-minute
        # time emits 5 components, a whole-second time emits 6, and only a
        # non-zero nanoOfSecond produces the 7th. All must still round-trip.
        whole_minute = datetime(2024, 6, 25, 10, 30, 0, 0)
        self.assertEqual([2024, 6, 25, 10, 30],
                         local_datetime_to_json_array(whole_minute))
        self.assertEqual(whole_minute,
                         json_array_to_local_datetime([2024, 6, 25, 10, 30]))

        whole_second = datetime(2024, 6, 25, 10, 30, 45, 0)
        self.assertEqual([2024, 6, 25, 10, 30, 45],
                         local_datetime_to_json_array(whole_second))
        self.assertEqual(whole_second,
                         json_array_to_local_datetime([2024, 6, 25, 10, 30, 45]))

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

    @staticmethod
    def _run_in_tz(tz_name, fn):
        """Run ``fn`` with the process TZ forced to ``tz_name`` and restored."""
        prev = os.environ.get("TZ")
        os.environ["TZ"] = tz_name
        time.tzset()
        try:
            return fn()
        finally:
            if prev is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = prev
            time.tzset()

    @unittest.skipUnless(hasattr(time, "tzset"), "requires time.tzset (POSIX)")
    def test_system_zone_millis_follows_host_timezone(self):
        # A timezone-less create-time (Java LocalDateTime, made with the local
        # now()). The REST GetTagResponse interprets it in the host's default
        # zone, so the same wall-clock yields different epoch millis per TZ.
        dt = datetime(2024, 1, 1, 12, 0, 0)  # DST-free in Asia/Shanghai (UTC+8)

        sh = self._run_in_tz(
            "Asia/Shanghai", lambda: local_datetime_to_system_zone_millis(dt))
        utc = self._run_in_tz(
            "UTC", lambda: local_datetime_to_system_zone_millis(dt))

        # Expected values derived from tz-aware references (not hard-coded).
        expected_sh = int(datetime(
            2024, 1, 1, 12, tzinfo=timezone(timedelta(hours=8))).timestamp() * 1000)
        expected_utc = int(datetime(
            2024, 1, 1, 12, tzinfo=timezone.utc).timestamp() * 1000)
        self.assertEqual(expected_sh, sh)
        self.assertEqual(expected_utc, utc)
        # Shanghai noon is 8h earlier in absolute time than UTC noon.
        self.assertEqual(8 * 3600 * 1000, utc - sh)

        # The zone-less $tags conversion must stay fixed regardless of host TZ.
        zoneless = self._run_in_tz(
            "Asia/Shanghai", lambda: local_datetime_to_millis(dt))
        self.assertEqual(expected_utc, zoneless)

    def test_parse_duration_nanos_keeps_full_precision(self):
        # Unlike parse_duration (rounded milliseconds), the nanos variant keeps
        # sub-millisecond units exactly instead of rounding them to zero.
        self.assertEqual(1, parse_duration_nanos("1ns"))
        self.assertEqual(1_000, parse_duration_nanos("1micro"))
        self.assertEqual(500_000, parse_duration_nanos("500micro"))
        self.assertEqual(1_000_000, parse_duration_nanos("1ms"))
        self.assertEqual(1_000_000_000, parse_duration_nanos("1s"))
        self.assertEqual(86_400_000_000_000, parse_duration_nanos("1d"))
        # A bare number means milliseconds, matching parse_duration.
        self.assertEqual(5_000_000, parse_duration_nanos("5"))

    def test_parse_duration_nanos_rejects_unknown_unit(self):
        with self.assertRaises(ValueError):
            parse_duration_nanos("1x")

    def test_sub_millisecond_retention_round_trips(self):
        # 500 microseconds is representable and must survive serialization at
        # microsecond precision (0.0005s / PT0.0005S), not collapse to zero.
        td = timedelta(microseconds=parse_duration_nanos("500micro") // 1000)
        self.assertEqual(timedelta(microseconds=500), td)
        self.assertEqual(0.0005, duration_to_json_seconds(td))
        self.assertEqual(td, json_seconds_to_duration(0.0005))
        self.assertEqual("PT0.0005S", duration_to_iso8601(td))


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

    def test_trim_to_snapshot_preserves_all_snapshot_fields(self):
        # trim_to_snapshot must copy every Snapshot field (matching Java
        # Tag.trimToSnapshot); a prior version silently dropped
        # base/delta/changelog_manifest_list_size and properties, so
        # FileSystemCatalog.get_tag() lost them for TTL tags.
        original = _full_snapshot()
        tag = Tag.from_snapshot_and_tag_ttl(
            original, timedelta(days=1), datetime(2024, 1, 1))

        snap = tag.trim_to_snapshot()

        self.assertNotIsInstance(snap, Tag)
        # Iterate over every declared Snapshot field so adding a field later
        # without updating trim_to_snapshot fails this test.
        for f in dataclasses.fields(Snapshot):
            self.assertEqual(
                getattr(original, f.name), getattr(snap, f.name),
                f"trim_to_snapshot dropped/changed Snapshot field '{f.name}'")


if __name__ == "__main__":
    unittest.main()
