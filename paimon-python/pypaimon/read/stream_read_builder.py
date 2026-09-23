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

"""
StreamReadBuilder for building streaming table scans and reads.

This module provides a builder for configuring streaming reads from Paimon
tables, similar to ReadBuilder but for continuous streaming use cases.
"""

from typing import Callable, List, Optional, Set

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.read_builder import ReadBuilder
from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan
from pypaimon.read.table_read import TableRead
from pypaimon.schema.data_types import DataField


class StreamReadBuilder:
    """
    Builder for streaming reads from Paimon tables.

    Usage:
        stream_builder = table.new_stream_read_builder()
        stream_builder.with_poll_interval_ms(500)

        scan = stream_builder.new_streaming_scan()
        table_read = stream_builder.new_read()

        async for plan in scan.stream():
            arrow_table = table_read.to_arrow(plan.splits())
            process(arrow_table)
    """

    def __init__(self, table):
        """Initialize the StreamReadBuilder."""
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self._predicate: Optional[Predicate] = None
        self._projection: Optional[List[str]] = None
        self._poll_interval_ms: int = 1000
        self._include_row_kind: bool = False
        self._bucket_filter: Optional[Callable[[int], bool]] = None
        self._consumer_id: Optional[str] = None

    def with_filter(self, predicate: Predicate) -> 'StreamReadBuilder':
        """Set a filter predicate for the streaming read."""
        self._predicate = predicate
        return self

    def with_projection(self, projection: List[str]) -> 'StreamReadBuilder':
        """Set column projection for the streaming read."""
        self._projection = projection
        return self

    def with_poll_interval_ms(self, poll_interval_ms: int) -> 'StreamReadBuilder':
        """Set the poll interval in ms for checking new snapshots (default: 1000)."""
        self._poll_interval_ms = poll_interval_ms
        return self

    def with_include_row_kind(self, include: bool = True) -> 'StreamReadBuilder':
        """Include row kind column (_row_kind) in the output.

        When enabled, the output will include a _row_kind column as the first
        column with values: +I (insert), -U (update before), +U (update after),
        -D (delete).
        """
        self._include_row_kind = include
        return self

    def with_consumer_id(self, consumer_id: str) -> 'StreamReadBuilder':
        """Set a consumer ID for persisting streaming read progress."""
        self._consumer_id = consumer_id
        return self

    def with_bucket_filter(
        self,
        bucket_filter: Callable[[int], bool]
    ) -> 'StreamReadBuilder':
        """Push bucket filter for parallel consumption.

        Example:
            builder.with_bucket_filter(lambda b: b % 2 == 0)
            builder.with_bucket_filter(lambda b: b < 4)
        """
        self._bucket_filter = bucket_filter
        return self

    def with_buckets(self, bucket_ids: List[int]) -> 'StreamReadBuilder':
        """Convenience method to read only specific buckets.

        Example:
            builder.with_buckets([0, 1, 2])
            builder.with_buckets([3, 4, 5])
        """
        bucket_set: Set[int] = set(bucket_ids)
        return self.with_bucket_filter(lambda bucket: bucket in bucket_set)

    def new_streaming_scan(self) -> AsyncStreamingTableScan:
        """Create a new AsyncStreamingTableScan with this builder's settings."""
        projection = self._projection_builder()
        projection._validate_map_key_filter()
        scan = AsyncStreamingTableScan(
            table=self.table,
            predicate=self._predicate,
            poll_interval_ms=self._poll_interval_ms,
            bucket_filter=self._bucket_filter,
            consumer_id=self._consumer_id
        )
        scan._read_type = projection._scan_read_type()
        return scan

    def new_read(self) -> TableRead:
        """Create a new TableRead with this builder's settings."""
        projection = self._projection_builder()
        projection._validate_map_key_filter()
        return TableRead(
            table=self.table,
            predicate=self._predicate,
            read_type=projection.read_type(),
            nested_name_paths=projection._nested_name_paths(),
            include_row_kind=self._include_row_kind
        )

    def new_predicate_builder(self) -> PredicateBuilder:
        """Create a PredicateBuilder for building filter predicates."""
        return self._projection_builder().new_predicate_builder()

    def read_type(self) -> List[DataField]:
        """Get the read schema fields, applying projection if set."""
        return self._projection_builder().read_type()

    def _nested_name_paths(self) -> Optional[List[List[str]]]:
        """Return nested paths resolved with the batch read-builder rules."""
        return self._projection_builder()._nested_name_paths()

    def _projection_builder(self) -> ReadBuilder:
        """Share projection and validation semantics with batch reads."""
        builder = ReadBuilder(self.table)
        if self._projection is not None:
            builder.with_projection(self._projection)
        if self._predicate is not None:
            builder.with_filter(self._predicate)
        return builder
