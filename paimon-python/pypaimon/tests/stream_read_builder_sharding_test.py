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
Test cases for StreamReadBuilder sharding functionality.

Tests the with_shard(), with_bucket_filter(), and with_buckets() methods
that enable parallel consumption across multiple consumer processes.
"""

import os
import shutil
import tempfile
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
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
    """Unit tests for StreamReadBuilder sharding method validation."""

    @pytest.mark.parametrize("index,count", [(0, 4), (1, 4), (2, 4), (3, 4), (0, 1), (99, 100)])
    def test_with_shard_valid_params(self, builder, index, count):
        """Test with_shard() accepts valid parameters."""
        result = builder.with_shard(index, count)
        assert result is builder
        assert builder._shard_index == index
        assert builder._shard_count == count

    @pytest.mark.parametrize("index,count,error_msg", [
        (-1, 4, "index_of_this_subtask must be >= 0"),
        (0, 0, "number_of_parallel_subtasks must be > 0"),
        (0, -1, "number_of_parallel_subtasks must be > 0"),
        (4, 4, "index_of_this_subtask must be < number_of_parallel_subtasks"),
        (5, 4, "index_of_this_subtask must be < number_of_parallel_subtasks"),
    ])
    def test_with_shard_invalid_params(self, builder, index, count, error_msg):
        """Test with_shard() raises error for invalid parameters."""
        with pytest.raises(ValueError, match=error_msg):
            builder.with_shard(index, count)

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

    def test_mutual_exclusion_shard_then_bucket_filter(self, builder):
        """Test with_shard() and with_bucket_filter() are mutually exclusive."""
        builder.with_shard(0, 4)
        with pytest.raises(ValueError, match="cannot be used with with_shard"):
            builder.with_bucket_filter(lambda b: True)

    def test_mutual_exclusion_bucket_filter_then_shard(self, builder):
        """Test with_bucket_filter() and with_shard() are mutually exclusive."""
        builder.with_bucket_filter(lambda b: True)
        with pytest.raises(ValueError, match="cannot be used with with_bucket_filter"):
            builder.with_shard(0, 4)

    def test_method_chaining(self, builder):
        """Test method chaining works correctly."""
        result = (builder
                  .with_poll_interval_ms(500)
                  .with_consumer_id("test-consumer")
                  .with_shard(1, 3)
                  .with_include_row_kind(True))
        assert result is builder
        assert builder._poll_interval_ms == 500
        assert builder._consumer_id == "test-consumer"
        assert builder._shard_index == 1
        assert builder._shard_count == 3


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

    @pytest.mark.parametrize("shard_idx,shard_count,expected", [
        (0, 4, [0, 4]),
        (1, 4, [1, 5]),
        (None, None, list(range(8))),  # No filtering
    ])
    def test_filter_entries_for_shard(self, mock_scan_table, shard_idx, shard_count, expected):
        """Test _filter_entries_for_shard with various configurations."""
        from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan

        scan = AsyncStreamingTableScan(
            table=mock_scan_table,
            shard_index=shard_idx,
            shard_count=shard_count
        )
        entries = [MockEntry(b) for b in range(8)]
        filtered = scan._filter_entries_for_shard(entries)
        assert [e.bucket for e in filtered] == expected

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


# -----------------------------------------------------------------------------
# Integration Tests
# -----------------------------------------------------------------------------

@pytest.fixture(scope="module")
def catalog_env():
    """Create a catalog environment for integration tests."""
    tempdir = tempfile.mkdtemp()
    warehouse = os.path.join(tempdir, 'warehouse')
    catalog = CatalogFactory.create({'warehouse': warehouse})
    catalog.create_database('default', False)
    yield {"catalog": catalog, "tempdir": tempdir}
    shutil.rmtree(tempdir, ignore_errors=True)


def create_bucketed_table(catalog, table_name: str, num_buckets: int):
    """Create a table with specified number of buckets."""
    pa_schema = pa.schema([('id', pa.int64()), ('value', pa.string())])
    schema = Schema.from_pyarrow_schema(
        pa_schema, primary_keys=['id'], options={'bucket': str(num_buckets)}
    )
    catalog.create_table(f'default.{table_name}', schema, False)
    return catalog.get_table(f'default.{table_name}')


def write_data(table, ids: list):
    """Write data with given IDs to table."""
    pa_schema = pa.schema([('id', pa.int64()), ('value', pa.string())])
    data = {'id': ids, 'value': [f'v{i}' for i in ids]}
    write_builder = table.new_batch_write_builder()
    writer = write_builder.new_write()
    commit = write_builder.new_commit()
    try:
        batch = pa.Table.from_pydict(data, schema=pa_schema)
        writer.write_arrow(batch)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


class TestIntegration:
    """Integration tests with real tables."""

    def test_bucket_count_property(self, catalog_env):
        """Test bucket_count property returns correct value."""
        table = create_bucketed_table(catalog_env["catalog"], 'test_bucket_count', 8)
        assert table.bucket_count == 8

    def test_stream_builder_passes_shard_to_scan(self, catalog_env):
        """Test StreamReadBuilder passes shard config to scan."""
        table = create_bucketed_table(catalog_env["catalog"], 'test_shard_pass', 4)
        write_data(table, list(range(100)))

        scan = table.new_stream_read_builder().with_shard(0, 4).new_streaming_scan()
        assert scan._shard_index == 0
        assert scan._shard_count == 4

    def test_file_scanner_filters_by_shard(self, catalog_env):
        """Test FileScanner respects shard filtering."""
        from pypaimon.manifest.manifest_list_manager import ManifestListManager
        from pypaimon.read.scanner.file_scanner import FileScanner
        from pypaimon.snapshot.snapshot_manager import SnapshotManager

        table = create_bucketed_table(catalog_env["catalog"], 'test_fss_shard', 4)
        write_data(table, list(range(100)))

        manifest_list_manager = ManifestListManager(table)
        snapshot_manager = SnapshotManager(table)

        def all_manifests():
            snapshot = snapshot_manager.get_latest_snapshot()
            return manifest_list_manager.read_all(snapshot)

        scanner = FileScanner(table, all_manifests, predicate=None, limit=None, shard_index=0, shard_count=4)
        splits = scanner.scan().splits()

        for split in splits:
            assert split.bucket % 4 == 0, f"Bucket {split.bucket} should be in shard 0"

    def test_sharded_reads_cover_all_buckets(self, catalog_env):
        """Test that sharded reads across all consumers cover all data exactly once."""
        from pypaimon.manifest.manifest_list_manager import ManifestListManager
        from pypaimon.read.scanner.file_scanner import FileScanner
        from pypaimon.snapshot.snapshot_manager import SnapshotManager

        table = create_bucketed_table(catalog_env["catalog"], 'test_coverage', 4)
        write_data(table, list(range(100)))

        manifest_list_manager = ManifestListManager(table)
        snapshot_manager = SnapshotManager(table)

        def all_manifests():
            snapshot = snapshot_manager.get_latest_snapshot()
            return manifest_list_manager.read_all(snapshot)

        all_buckets = set()
        for consumer_idx in range(4):
            scanner = FileScanner(
                table, all_manifests, predicate=None, limit=None,
                shard_index=consumer_idx, shard_count=4
            )
            consumer_buckets = {s.bucket for s in scanner.scan().splits()}

            # Verify correct assignment and no overlap
            for b in consumer_buckets:
                assert b % 4 == consumer_idx
            assert not (all_buckets & consumer_buckets), "No overlap between consumers"
            all_buckets.update(consumer_buckets)

        assert all_buckets == set(range(4)), "All buckets covered"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
