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

"""Unit tests for partition expiration module."""

import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from pypaimon.partition.partition_time_extractor import PartitionTimeExtractor
from pypaimon.partition.partition_expire_strategy import (
    PartitionEntry,
    PartitionValuesTimeExpireStrategy,
    PartitionUpdateTimeExpireStrategy,
)
from pypaimon.partition.partition_expire import PartitionExpire, _parse_duration


class TestPartitionTimeExtractor(unittest.TestCase):
    """Tests for PartitionTimeExtractor."""

    def test_extract_simple_date_no_pattern(self):
        """Extract date from first partition value without pattern."""
        extractor = PartitionTimeExtractor()
        result = extractor.extract(["dt"], ["2024-01-15"])
        self.assertEqual(result, datetime(2024, 1, 15, 0, 0, 0))

    def test_extract_timestamp_no_pattern(self):
        """Extract full timestamp from first partition value."""
        extractor = PartitionTimeExtractor()
        result = extractor.extract(["dt"], ["2024-01-15 10:30:00"])
        self.assertEqual(result, datetime(2024, 1, 15, 10, 30, 0))

    def test_extract_with_pattern(self):
        """Extract timestamp using pattern with multiple partition keys."""
        extractor = PartitionTimeExtractor(pattern="$dt $hour:00:00")
        result = extractor.extract(["dt", "hour"], ["2024-01-15", "10"])
        self.assertEqual(result, datetime(2024, 1, 15, 10, 0, 0))

    def test_extract_with_formatter(self):
        """Extract using custom formatter."""
        extractor = PartitionTimeExtractor(formatter="%Y%m%d")
        result = extractor.extract(["dt"], ["20240115"])
        self.assertEqual(result, datetime(2024, 1, 15, 0, 0, 0))

    def test_extract_with_pattern_and_formatter(self):
        """Extract using both pattern and formatter."""
        extractor = PartitionTimeExtractor(pattern="$year-$month-$day", formatter="%Y-%m-%d")
        result = extractor.extract(["year", "month", "day"], ["2024", "01", "15"])
        self.assertEqual(result, datetime(2024, 1, 15, 0, 0, 0))

    def test_extract_from_spec(self):
        """Extract from a partition spec dictionary."""
        extractor = PartitionTimeExtractor()
        result = extractor.extract_from_spec({"dt": "2024-03-20"})
        self.assertEqual(result, datetime(2024, 3, 20, 0, 0, 0))

    def test_extract_invalid_format_raises(self):
        """Raise ValueError for unparseable timestamps."""
        extractor = PartitionTimeExtractor()
        with self.assertRaises(ValueError):
            extractor.extract(["dt"], ["not-a-date"])

    def test_extract_single_digit_month_day(self):
        """Parse dates with single-digit month and day."""
        extractor = PartitionTimeExtractor()
        result = extractor.extract(["dt"], ["2024-1-5"])
        self.assertEqual(result, datetime(2024, 1, 5, 0, 0, 0))

    def test_formatter_date_only_fallback(self):
        """Formatter with time components should fall back to date-only parsing."""
        extractor = PartitionTimeExtractor(formatter="yyyy-MM-dd HH:mm:ss")
        result = extractor.extract(["dt"], ["2024-03-15"])
        self.assertEqual(result, datetime(2024, 3, 15, 0, 0, 0))


class TestPartitionValuesTimeExpireStrategy(unittest.TestCase):
    """Tests for PartitionValuesTimeExpireStrategy."""

    def test_select_expired_partitions(self):
        """Partitions older than threshold should be selected."""
        strategy = PartitionValuesTimeExpireStrategy(
            partition_keys=["dt"],
            partition_default_name="__DEFAULT_PARTITION__",
        )
        entries = [
            PartitionEntry(spec={"dt": "2024-01-01"}, record_count=100),
            PartitionEntry(spec={"dt": "2024-01-10"}, record_count=200),
            PartitionEntry(spec={"dt": "2024-01-20"}, record_count=300),
        ]
        # Expire anything before 2024-01-15
        expiration_time = datetime(2024, 1, 15)
        expired = strategy.select_expired_partitions(entries, expiration_time)
        self.assertEqual(len(expired), 2)
        self.assertEqual(expired[0].spec["dt"], "2024-01-01")
        self.assertEqual(expired[1].spec["dt"], "2024-01-10")

    def test_no_expired_partitions(self):
        """No partitions should be selected if all are newer."""
        strategy = PartitionValuesTimeExpireStrategy(
            partition_keys=["dt"],
        )
        entries = [
            PartitionEntry(spec={"dt": "2024-06-01"}, record_count=100),
        ]
        expiration_time = datetime(2024, 1, 1)
        expired = strategy.select_expired_partitions(entries, expiration_time)
        self.assertEqual(len(expired), 0)

    def test_unparseable_partitions_skipped(self):
        """Partitions with unparseable values should be skipped with a warning."""
        strategy = PartitionValuesTimeExpireStrategy(
            partition_keys=["dt"],
        )
        entries = [
            PartitionEntry(spec={"dt": "invalid-date"}, record_count=100),
            PartitionEntry(spec={"dt": "2024-01-01"}, record_count=200),
        ]
        expiration_time = datetime(2024, 6, 1)
        expired = strategy.select_expired_partitions(entries, expiration_time)
        # Only the parseable one should be expired
        self.assertEqual(len(expired), 1)
        self.assertEqual(expired[0].spec["dt"], "2024-01-01")

    def test_with_custom_pattern(self):
        """Test with custom timestamp pattern."""
        strategy = PartitionValuesTimeExpireStrategy(
            partition_keys=["year", "month", "day"],
            timestamp_pattern="$year-$month-$day",
        )
        entries = [
            PartitionEntry(spec={"year": "2024", "month": "01", "day": "01"}),
            PartitionEntry(spec={"year": "2024", "month": "06", "day": "15"}),
        ]
        expiration_time = datetime(2024, 3, 1)
        expired = strategy.select_expired_partitions(entries, expiration_time)
        self.assertEqual(len(expired), 1)


class TestPartitionUpdateTimeExpireStrategy(unittest.TestCase):
    """Tests for PartitionUpdateTimeExpireStrategy."""

    def test_select_expired_by_update_time(self):
        """Partitions with old file creation times should be selected."""
        strategy = PartitionUpdateTimeExpireStrategy(
            partition_keys=["dt"],
        )
        now = datetime(2024, 6, 1)
        old_time_millis = int((now - timedelta(days=30)).timestamp() * 1000)
        recent_time_millis = int((now - timedelta(days=1)).timestamp() * 1000)

        entries = [
            PartitionEntry(spec={"dt": "2024-05-01"}, last_file_creation_time=old_time_millis),
            PartitionEntry(spec={"dt": "2024-05-30"}, last_file_creation_time=recent_time_millis),
        ]

        # Expire partitions older than 7 days from now
        expiration_time = now - timedelta(days=7)
        expired = strategy.select_expired_partitions(entries, expiration_time)
        self.assertEqual(len(expired), 1)
        self.assertEqual(expired[0].spec["dt"], "2024-05-01")

    def test_no_expired_when_all_recent(self):
        """No partitions expired when all have recent file creation times."""
        strategy = PartitionUpdateTimeExpireStrategy(
            partition_keys=["dt"],
        )
        now = datetime(2024, 6, 1)
        recent_time = int((now - timedelta(hours=1)).timestamp() * 1000)

        entries = [
            PartitionEntry(spec={"dt": "2024-05-31"}, last_file_creation_time=recent_time),
        ]
        expiration_time = now - timedelta(days=7)
        expired = strategy.select_expired_partitions(entries, expiration_time)
        self.assertEqual(len(expired), 0)

    def test_zero_creation_time_skipped(self):
        """Partitions with last_file_creation_time=0 should be skipped (unknown state)."""
        strategy = PartitionUpdateTimeExpireStrategy(
            partition_keys=["dt"],
        )
        now = datetime(2024, 6, 1)
        old_time_millis = int((now - timedelta(days=30)).timestamp() * 1000)

        entries = [
            PartitionEntry(spec={"dt": "2024-05-01"}, last_file_creation_time=old_time_millis),
            PartitionEntry(spec={"dt": "2024-04-01"}, last_file_creation_time=0),
        ]

        expiration_time = now - timedelta(days=7)
        expired = strategy.select_expired_partitions(entries, expiration_time)
        self.assertEqual(len(expired), 1)
        self.assertEqual(expired[0].spec["dt"], "2024-05-01")


class TestPartitionExpire(unittest.TestCase):
    """Tests for PartitionExpire orchestration class."""

    def test_expire_drops_old_partitions(self):
        """Test that expired partitions are dropped."""
        entries = [
            PartitionEntry(spec={"dt": "2024-01-01"}, record_count=100),
            PartitionEntry(spec={"dt": "2024-01-10"}, record_count=200),
            PartitionEntry(spec={"dt": "2024-06-01"}, record_count=300),
        ]
        dropped = []

        def mock_dropper(partitions, commit_id):
            dropped.extend(partitions)

        strategy = PartitionValuesTimeExpireStrategy(partition_keys=["dt"])
        expire = PartitionExpire(
            expiration_time=timedelta(days=30),
            strategy=strategy,
            partition_reader=lambda: entries,
            partition_dropper=mock_dropper,
        )

        now = datetime(2024, 6, 15)
        result = expire.expire(now=now)

        # Partitions before 2024-05-16 should be expired
        self.assertEqual(len(result), 2)
        self.assertIn({"dt": "2024-01-01"}, result)
        self.assertIn({"dt": "2024-01-10"}, result)
        self.assertEqual(len(dropped), 2)

    def test_expire_respects_check_interval(self):
        """Test that check interval prevents too-frequent expiration checks."""
        entries = [
            PartitionEntry(spec={"dt": "2024-01-01"}, record_count=100),
        ]
        dropped = []

        def mock_dropper(partitions, commit_id):
            dropped.extend(partitions)

        strategy = PartitionValuesTimeExpireStrategy(partition_keys=["dt"])
        expire = PartitionExpire(
            expiration_time=timedelta(days=7),
            strategy=strategy,
            partition_reader=lambda: entries,
            partition_dropper=mock_dropper,
            check_interval=timedelta(hours=1),
        )

        now = datetime(2024, 6, 15, 10, 0, 0)
        result1 = expire.expire(now=now)
        self.assertEqual(len(result1), 1)

        # Second call within check interval should return empty
        result2 = expire.expire(now=now + timedelta(minutes=30))
        self.assertEqual(len(result2), 0)

        # After interval passes, should expire again
        # (but entries already dropped, so reader returns same)
        result3 = expire.expire(now=now + timedelta(hours=2))
        self.assertEqual(len(result3), 1)

    def test_expire_max_num(self):
        """Test that max_expire_num limits the number of dropped partitions."""
        entries = [
            PartitionEntry(spec={"dt": f"2024-01-{i:02d}"}, record_count=100)
            for i in range(1, 11)  # 10 partitions
        ]
        dropped = []

        def mock_dropper(partitions, commit_id):
            dropped.extend(partitions)

        strategy = PartitionValuesTimeExpireStrategy(partition_keys=["dt"])
        expire = PartitionExpire(
            expiration_time=timedelta(days=7),
            strategy=strategy,
            partition_reader=lambda: entries,
            partition_dropper=mock_dropper,
            max_expire_num=3,
        )

        now = datetime(2024, 6, 15)
        result = expire.expire(now=now)
        self.assertEqual(len(result), 3)
        self.assertEqual(len(dropped), 3)

    def test_expire_empty_partitions(self):
        """Test that no error occurs when there are no partitions."""
        strategy = PartitionValuesTimeExpireStrategy(partition_keys=["dt"])
        expire = PartitionExpire(
            expiration_time=timedelta(days=7),
            strategy=strategy,
            partition_reader=lambda: [],
            partition_dropper=lambda p, c: None,
        )
        result = expire.expire()
        self.assertEqual(result, [])

    def test_expire_none_when_not_configured(self):
        """Test that from_table returns None when expiration is not configured."""
        mock_table = MagicMock()
        mock_table.table_schema.options = {}
        mock_table.partition_keys = ["dt"]

        result = PartitionExpire.from_table(mock_table)
        self.assertIsNone(result)

    def test_expire_none_for_non_partitioned_table(self):
        """Test that from_table returns None for non-partitioned tables."""
        mock_table = MagicMock()
        mock_table.table_schema.options = {"partition.expiration-time": "7d"}
        mock_table.partition_keys = []

        result = PartitionExpire.from_table(mock_table)
        self.assertIsNone(result)


class TestParseDuration(unittest.TestCase):
    """Tests for _parse_duration utility."""

    def test_parse_days(self):
        self.assertEqual(_parse_duration("7d"), timedelta(days=7))
        self.assertEqual(_parse_duration("7 days"), timedelta(days=7))

    def test_parse_hours(self):
        self.assertEqual(_parse_duration("24h"), timedelta(hours=24))
        self.assertEqual(_parse_duration("2 hours"), timedelta(hours=2))

    def test_parse_minutes(self):
        self.assertEqual(_parse_duration("30m"), timedelta(minutes=30))
        self.assertEqual(_parse_duration("30 min"), timedelta(minutes=30))

    def test_parse_seconds(self):
        self.assertEqual(_parse_duration("60s"), timedelta(seconds=60))

    def test_parse_milliseconds(self):
        self.assertEqual(_parse_duration("500ms"), timedelta(milliseconds=500))

    def test_parse_combined(self):
        self.assertEqual(_parse_duration("1d 2h"), timedelta(days=1, hours=2))

    def test_parse_empty_returns_none(self):
        self.assertIsNone(_parse_duration(""))
        self.assertIsNone(_parse_duration(None))

    def test_parse_invalid_returns_none(self):
        self.assertIsNone(_parse_duration("not-a-duration"))


if __name__ == "__main__":
    unittest.main()
