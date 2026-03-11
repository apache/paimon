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
#  limitations under the License.
################################################################################

"""
Test cases for StreamReadBuilder bucket filtering functionality.

Tests the with_bucket_filter() and with_buckets() methods
that enable parallel consumption across multiple consumer processes.
"""

from unittest.mock import MagicMock

import pytest

from pypaimon.read.stream_read_builder import StreamReadBuilder


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture
def mock_table():
    """Create a mock table for unit tests."""
    table = MagicMock()
    table.fields = []
    table.options.row_tracking_enabled.return_value = False
    return table


@pytest.fixture
def builder(mock_table):
    """Create a StreamReadBuilder with mock table."""
    return StreamReadBuilder(mock_table)


class MockEntry:
    """Mock manifest entry for testing bucket filtering."""

    def __init__(self, bucket):
        self.bucket = bucket


# -----------------------------------------------------------------------------
# Unit Tests: StreamReadBuilder Validation
# -----------------------------------------------------------------------------

class TestStreamReadBuilderValidation:
    """Unit tests for StreamReadBuilder method validation."""

    def test_with_bucket_filter_valid(self, builder):
        """Test with_bucket_filter() accepts valid filter function."""
        filter_fn = lambda b: b % 2 == 0
        result = builder.with_bucket_filter(filter_fn)
        assert result is builder
        assert builder._bucket_filter is filter_fn

    @pytest.mark.parametrize("bucket_ids,expected_true,expected_false", [
        ([0, 2, 4], [0, 2, 4], [1, 3, 5]),
        ([], [], [0, 1, 2]),
        ([5], [5], [0, 1, 4, 6]),
    ])
    def test_with_buckets(self, builder, bucket_ids, expected_true, expected_false):
        """Test with_buckets() creates correct filter."""
        builder.with_buckets(bucket_ids)
        for b in expected_true:
            assert builder._bucket_filter(b), f"Bucket {b} should be included"
        for b in expected_false:
            assert not builder._bucket_filter(b), f"Bucket {b} should be excluded"

    def test_method_chaining(self, builder):
        """Test method chaining works correctly."""
        result = (builder
                  .with_poll_interval_ms(500)
                  .with_bucket_filter(lambda b: b % 2 == 0)
                  .with_include_row_kind(True))
        assert result is builder
        assert builder._poll_interval_ms == 500
        assert builder._bucket_filter is not None
        assert builder._include_row_kind is True


# -----------------------------------------------------------------------------
# Unit Tests: Bucket Filtering Logic
# -----------------------------------------------------------------------------

class TestBucketFilteringLogic:
    """Test bucket filtering logic used in scans."""

    @pytest.mark.parametrize("shard_idx,shard_count,expected_buckets", [
        (0, 4, [0, 4]),
        (1, 4, [1, 5]),
        (2, 4, [2, 6]),
        (3, 4, [3, 7]),
        (0, 2, [0, 2, 4, 6]),
        (1, 2, [1, 3, 5, 7]),
    ])
    def test_shard_filtering(self, shard_idx, shard_count, expected_buckets):
        """Test shard-based bucket filtering."""
        entries = [MockEntry(b) for b in range(8)]
        filtered = [e for e in entries if e.bucket % shard_count == shard_idx]
        assert [e.bucket for e in filtered] == expected_buckets

    @pytest.mark.parametrize("num_buckets,num_consumers", [(8, 4), (7, 3), (10, 3), (5, 5)])
    def test_shards_cover_all_buckets(self, num_buckets, num_consumers):
        """Test that all shards together cover all buckets exactly once."""
        all_buckets = set()
        for shard_idx in range(num_consumers):
            shard_buckets = {b for b in range(num_buckets) if b % num_consumers == shard_idx}
            assert not (all_buckets & shard_buckets), "Shards should not overlap"
            all_buckets.update(shard_buckets)
        assert all_buckets == set(range(num_buckets)), "All buckets should be covered"


# -----------------------------------------------------------------------------
# Unit Tests: AsyncStreamingTableScan
# -----------------------------------------------------------------------------

class TestAsyncStreamingTableScanFiltering:
    """Test AsyncStreamingTableScan._filter_entries_for_shard()."""

    @pytest.fixture
    def mock_scan_table(self):
        """Create mock table for AsyncStreamingTableScan."""
        table = MagicMock()
        table.options.changelog_producer.return_value = MagicMock()
        table.file_io = MagicMock()
        table.table_path = "/tmp/test"
        return table

    def test_filter_with_bucket_filter(self, mock_scan_table):
        """Test _filter_entries_for_shard with custom bucket filter."""
        from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan

        scan = AsyncStreamingTableScan(
            table=mock_scan_table,
            bucket_filter=lambda b: b % 2 == 0
        )
        entries = [MockEntry(b) for b in range(8)]
        filtered = scan._filter_entries_for_shard(entries)
        assert [e.bucket for e in filtered] == [0, 2, 4, 6]

    def test_filter_no_filter_returns_all(self, mock_scan_table):
        """Test _filter_entries_for_shard with no filter returns all entries."""
        from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan

        scan = AsyncStreamingTableScan(table=mock_scan_table)
        entries = [MockEntry(b) for b in range(8)]
        filtered = scan._filter_entries_for_shard(entries)
        assert [e.bucket for e in filtered] == list(range(8))


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
