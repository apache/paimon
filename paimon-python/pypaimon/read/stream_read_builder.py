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
# limitations under the License.
################################################################################
"""
StreamReadBuilder for building streaming table scans and reads.

This module provides a builder for configuring streaming reads from Paimon
tables, similar to ReadBuilder but for continuous streaming use cases.
"""

from typing import Callable, List, Optional, Set

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan
from pypaimon.read.table_read import TableRead
from pypaimon.schema.data_types import DataField
from pypaimon.table.special_fields import SpecialFields


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
        """
        Initialize the StreamReadBuilder.

        Args:
            table: The FileStoreTable to build streaming reads for
        """
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self._predicate: Optional[Predicate] = None
        self._projection: Optional[List[str]] = None
        self._poll_interval_ms: int = 1000
        self._include_row_kind: bool = False
        self._bucket_filter: Optional[Callable[[int], bool]] = None

    def with_filter(self, predicate: Predicate) -> 'StreamReadBuilder':
        """
        Set a filter predicate for the streaming read.

        Args:
            predicate: The predicate to filter data

        Returns:
            This builder for method chaining
        """
        self._predicate = predicate
        return self

    def with_projection(self, projection: List[str]) -> 'StreamReadBuilder':
        """
        Set column projection for the streaming read.

        Args:
            projection: List of column names to read

        Returns:
            This builder for method chaining
        """
        self._projection = projection
        return self

    def with_poll_interval_ms(self, poll_interval_ms: int) -> 'StreamReadBuilder':
        """
        Set the poll interval for checking new snapshots.

        Args:
            poll_interval_ms: Interval in milliseconds (default: 1000)

        Returns:
            This builder for method chaining
        """
        self._poll_interval_ms = poll_interval_ms
        return self

    def with_include_row_kind(self, include: bool = True) -> 'StreamReadBuilder':
        """
        Include row kind column (_row_kind) in the output.

        When enabled, the output will include a _row_kind column as the first
        column with values: +I (insert), -U (update before), +U (update after),
        -D (delete).

        This is useful for changelog streams where you need to distinguish
        between different types of changes.

        Args:
            include: Whether to include row kind (default: True)

        Returns:
            This builder for method chaining
        """
        self._include_row_kind = include
        return self

    def with_bucket_filter(
        self,
        bucket_filter: Callable[[int], bool]
    ) -> 'StreamReadBuilder':
        """
        Push bucket filter for parallel consumption.

        The filter function is called with each bucket ID and should return
        True for buckets to include.

        Example:
            # Read only even buckets
            builder.with_bucket_filter(lambda b: b % 2 == 0)

            # Read buckets 0-3
            builder.with_bucket_filter(lambda b: b < 4)

        Args:
            bucket_filter: Function that takes bucket ID and returns True to include

        Returns:
            This builder for method chaining
        """
        self._bucket_filter = bucket_filter
        return self

    def with_buckets(self, bucket_ids: List[int]) -> 'StreamReadBuilder':
        """
        Convenience method to read only specific buckets.

        This is a convenience wrapper around with_bucket_filter() for when
        you have a specific list of bucket IDs to read.

        Example:
            # Consumer 0 reads buckets 0, 1, 2
            builder.with_buckets([0, 1, 2])

            # Consumer 1 reads buckets 3, 4, 5
            builder.with_buckets([3, 4, 5])

        Args:
            bucket_ids: List of bucket IDs to read

        Returns:
            This builder for method chaining
        """
        bucket_set: Set[int] = set(bucket_ids)
        return self.with_bucket_filter(lambda bucket: bucket in bucket_set)

    def new_streaming_scan(self) -> AsyncStreamingTableScan:
        """
        Create a new AsyncStreamingTableScan for continuous streaming reads.

        Returns:
            AsyncStreamingTableScan configured with this builder's settings
        """
        return AsyncStreamingTableScan(
            table=self.table,
            predicate=self._predicate,
            poll_interval_ms=self._poll_interval_ms,
            bucket_filter=self._bucket_filter
        )

    def new_read(self) -> TableRead:
        """
        Create a new TableRead for reading splits.

        Returns:
            TableRead configured with this builder's settings
        """
        return TableRead(
            table=self.table,
            predicate=self._predicate,
            read_type=self.read_type(),
            include_row_kind=self._include_row_kind
        )

    def new_predicate_builder(self) -> PredicateBuilder:
        """
        Create a PredicateBuilder for building filter predicates.

        Returns:
            PredicateBuilder for constructing predicates
        """
        return PredicateBuilder(self.read_type())

    def read_type(self) -> List[DataField]:
        """
        Get the read type (schema fields) based on projection.

        Returns:
            List of DataField for the columns to read
        """
        table_fields = self.table.fields

        if not self._projection:
            return table_fields
        else:
            if self.table.options.row_tracking_enabled():
                table_fields = SpecialFields.row_type_with_row_tracking(table_fields)
            field_map = {field.name: field for field in table_fields}
            return [field_map[name] for name in self._projection if name in field_map]
