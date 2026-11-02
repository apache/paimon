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

"""Public streaming scans selecting Python or Native execution."""

import asyncio
from typing import AsyncGenerator, Callable, Iterator, Optional

from pypaimon.common.predicate import Predicate
from pypaimon.read.plan import Plan


class _StreamingScan:
    """Public lifecycle and polling over one prepared streaming executor."""

    def _initialize(self, table, predicate, read_type, poll_interval_ms,
                    bucket_filter, consumer_id, prefetch_enabled=True,
                    diff_threshold=10):
        self.table = table
        self.predicate = predicate
        self._read_type = read_type
        self.poll_interval = poll_interval_ms / 1000.0
        self._bucket_filter = bucket_filter
        self._consumer_id = consumer_id
        self._pending_consumer_snapshot = None
        self._pending_consumer_owner = None
        self._closed = False
        self._generation = 0
        from pypaimon.read.read_dispatch import create_streaming_scan
        self._execution = create_streaming_scan(
            self, poll_interval_ms, prefetch_enabled, diff_threshold)

    @property
    def next_snapshot_id(self):
        return self.checkpoint()

    @next_snapshot_id.setter
    def next_snapshot_id(self, next_snapshot_id):
        self.restore(next_snapshot_id)

    def checkpoint(self):
        """Return the next snapshot to consume, without acknowledging it."""
        return self._execution.checkpoint()

    def watermark(self):
        """Return the watermark of the last successfully planned frame."""
        return self._execution.watermark()

    def restore(self, next_snapshot_id=None):
        """Restore a checkpoint, or reset to the configured starting position."""
        self._require_open()
        self._execution.restore(next_snapshot_id)
        self._generation += 1
        self._pending_consumer_snapshot = None
        self._pending_consumer_owner = None

    def notify_checkpoint_complete(self, next_snapshot):
        """Persist progress only after the caller has processed its plan."""
        self._require_open()
        self._execution.notify_checkpoint_complete(next_snapshot)

    def plan(self):
        """Plan one available frame without changing execution backends."""
        self._require_open()
        generation = self._generation
        plan = self._execution.plan()
        self._require_open()
        if generation != self._generation:
            raise RuntimeError("stream scan was restored during planning")
        return plan

    def _flush_pending_consumer(self):
        if self._pending_consumer_snapshot is not None:
            self.notify_checkpoint_complete(self._pending_consumer_snapshot)
            self._pending_consumer_snapshot = None
            self._pending_consumer_owner = None

    async def stream(self) -> AsyncGenerator[Plan, None]:
        """Poll public controls and acknowledge a yielded frame on resume."""
        owner = object()
        try:
            while True:
                self._flush_pending_consumer()
                checkpoint = self.checkpoint()
                plan = self.plan()
                if plan is None:
                    next_snapshot = self.checkpoint()
                    if next_snapshot is not None and next_snapshot != checkpoint:
                        self._pending_consumer_snapshot = next_snapshot
                        self._pending_consumer_owner = owner
                        self._flush_pending_consumer()
                    await asyncio.sleep(self.poll_interval)
                    continue
                self._pending_consumer_snapshot = self.checkpoint()
                self._pending_consumer_owner = owner
                yield plan
        except (GeneratorExit, asyncio.CancelledError):
            if self._pending_consumer_owner is owner:
                self._pending_consumer_snapshot = None
                self._pending_consumer_owner = None
                self._execution.cancel_prefetch()
            raise

    def stream_sync(self) -> Iterator[Plan]:
        """Synchronously poll this scan and close its asynchronous iterator."""
        loop = asyncio.new_event_loop()
        iterator = self.stream()
        try:
            while True:
                yield loop.run_until_complete(iterator.__anext__())
        finally:
            try:
                loop.run_until_complete(iterator.aclose())
            finally:
                loop.close()

    def _require_open(self):
        if self._closed:
            raise RuntimeError("stream scan is closed")

    def close(self):
        """Stop polling without acknowledging rows still owned by the caller."""
        if self._closed:
            return
        self._closed = True
        self._generation += 1
        self._pending_consumer_snapshot = None
        self._pending_consumer_owner = None
        self._execution.close()


class AsyncStreamingTableScan(_StreamingScan):
    """Continuous async or synchronous polling with the configured backend.

    ``prefetch_enabled`` controls Python's speculative planning;
    Native plans frames on demand.
    ``diff_threshold`` is retained without replacing physical change events
    with a net snapshot diff.
    """

    def __init__(
        self,
        table,
        predicate: Optional[Predicate] = None,
        poll_interval_ms: int = 1000,
        bucket_filter: Optional[Callable[[int], bool]] = None,
        prefetch_enabled: bool = True,
        diff_threshold: int = 10,
        consumer_id: Optional[str] = None
    ):
        self._initialize(
            table, predicate, None, poll_interval_ms, bucket_filter, consumer_id,
            prefetch_enabled, diff_threshold)


class StreamTableScan(_StreamingScan):
    """Continuous scan with a canonical reader type and configured backend."""

    def __init__(self, table, predicate=None, read_type=None, poll_interval_ms=1000,
                 bucket_filter=None, consumer_id=None):
        self._initialize(table, predicate, read_type, poll_interval_ms,
                         bucket_filter, consumer_id)


__all__ = ['AsyncStreamingTableScan', 'StreamTableScan']
