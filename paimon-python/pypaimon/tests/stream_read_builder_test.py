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

"""Tests for StreamReadBuilder."""

from unittest.mock import MagicMock

import pytest

from pypaimon.read.stream_read_builder import StreamReadBuilder
from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan


class MockEntry:
    """Mock manifest entry for testing bucket filtering."""

    def __init__(self, bucket):
        self.bucket = bucket


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


@pytest.fixture
def mock_scan_table():
    """Create mock table for AsyncStreamingTableScan."""
    table = MagicMock()
    table.options.changelog_producer.return_value = MagicMock()
    table.file_io = MagicMock()
    table.table_path = "/tmp/test"
    table.fields = []
    table.options.row_tracking_enabled.return_value = False
    return table


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

    def test_with_consumer_id(self, builder):
        """Test with_consumer_id() stores consumer_id and returns self."""
        result = builder.with_consumer_id("my-consumer")
        assert result is builder
        assert builder._consumer_id == "my-consumer"

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


class TestAsyncStreamingTableScanFiltering:
    """Test AsyncStreamingTableScan._filter_entries_for_shard()."""

    def test_filter_with_bucket_filter(self, mock_scan_table):
        """Test _filter_entries_for_shard with custom bucket filter."""
        scan = AsyncStreamingTableScan(
            table=mock_scan_table,
            bucket_filter=lambda b: b % 2 == 0
        )
        entries = [MockEntry(b) for b in range(8)]
        filtered = scan._filter_entries_for_shard(entries)
        assert [e.bucket for e in filtered] == [0, 2, 4, 6]

    def test_filter_no_filter_returns_all(self, mock_scan_table):
        """Test _filter_entries_for_shard with no filter returns all entries."""
        scan = AsyncStreamingTableScan(table=mock_scan_table)
        entries = [MockEntry(b) for b in range(8)]
        filtered = scan._filter_entries_for_shard(entries)
        assert [e.bucket for e in filtered] == list(range(8))


class TestWithScanFrom:
    """Unit tests for StreamReadBuilder.with_scan_from()."""

    def test_with_scan_from_earliest(self, builder):
        """with_scan_from('earliest') stores value and returns self."""
        result = builder.with_scan_from("earliest")
        assert result is builder
        assert builder._scan_from == "earliest"

    def test_with_scan_from_latest(self, builder):
        """with_scan_from('latest') stores value and returns self."""
        result = builder.with_scan_from("latest")
        assert result is builder
        assert builder._scan_from == "latest"

    def test_with_scan_from_integer(self, builder):
        """with_scan_from(42) stores integer value and returns self."""
        result = builder.with_scan_from(42)
        assert result is builder
        assert builder._scan_from == 42

    def test_with_scan_from_passes_to_scan(self, mock_scan_table):
        """with_scan_from() value is passed through to AsyncStreamingTableScan."""
        builder = StreamReadBuilder(mock_scan_table)
        builder.with_scan_from("earliest")
        scan = builder.new_streaming_scan()
        assert scan._scan_from == "earliest"

    def test_with_scan_from_integer_passes_to_scan(self, mock_scan_table):
        """with_scan_from(42) passes integer to AsyncStreamingTableScan."""
        builder = StreamReadBuilder(mock_scan_table)
        builder.with_scan_from(42)
        scan = builder.new_streaming_scan()
        assert scan._scan_from == 42

    def test_with_scan_from_none_by_default(self, mock_scan_table):
        """Without calling with_scan_from(), _scan_from is None."""
        builder = StreamReadBuilder(mock_scan_table)
        scan = builder.new_streaming_scan()
        assert scan._scan_from is None

    def test_method_chaining_with_scan_from(self, builder):
        """with_scan_from() chains correctly with other builder methods."""
        result = builder.with_scan_from("earliest").with_poll_interval_ms(500)
        assert result is builder
        assert builder._scan_from == "earliest"
        assert builder._poll_interval_ms == 500


class TestConsumerIdPassthrough:
    """Test that consumer_id passes through to AsyncStreamingTableScan."""

    def test_new_streaming_scan_passes_consumer_id(self, mock_scan_table):
        """new_streaming_scan() should pass consumer_id to AsyncStreamingTableScan."""
        builder = StreamReadBuilder(mock_scan_table)
        builder.with_consumer_id("test-consumer")

        scan = builder.new_streaming_scan()

        assert scan._consumer_id == "test-consumer"
        assert scan._consumer_manager is not None

    def test_new_streaming_scan_no_consumer_by_default(self, mock_scan_table):
        """Without with_consumer_id(), scan should have no consumer."""
        builder = StreamReadBuilder(mock_scan_table)

        scan = builder.new_streaming_scan()

        assert scan._consumer_id is None
        assert scan._consumer_manager is None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
