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

"""Polling adapters for Native stateful streaming scans."""

import asyncio
from copy import copy
from typing import AsyncGenerator, Iterator

from pypaimon.execution.native.read.plan import native_stream_scan, _from_native_plan
from pypaimon.read.plan import Plan


class StreamTableScan:
    """Python iterator adapter for Rust's stateful StreamTableScan.

    Rust owns snapshot selection, split planning, checkpoints and consumer
    persistence. Python only polls, converts splits and acknowledges a yielded
    plan when the caller resumes the iterator after processing it.
    """

    def __init__(self, table, predicate=None, read_type=None, poll_interval_ms=1000,
                 bucket_filter=None, consumer_id=None):
        self.table = table
        self.poll_interval = poll_interval_ms / 1000.0
        self._scan = native_stream_scan(
            table, predicate=predicate, read_type=read_type,
            bucket_filter=bucket_filter, consumer_id=consumer_id)
        self._pending_consumer_snapshot = None
        self._pending_consumer_owner = None
        self._closed = False
        self._generation = 0

    @property
    def next_snapshot_id(self):
        return self.checkpoint()

    @next_snapshot_id.setter
    def next_snapshot_id(self, next_snapshot_id):
        self.restore(next_snapshot_id)

    def checkpoint(self):
        return self._scan.checkpoint()

    def watermark(self):
        return self._scan.watermark()

    def restore(self, next_snapshot_id=None):
        self._require_open()
        self._scan.restore(next_snapshot_id)
        self._generation += 1
        self._pending_consumer_snapshot = None
        self._pending_consumer_owner = None

    def notify_checkpoint_complete(self, next_snapshot):
        self._require_open()
        self._scan.notify_checkpoint_complete(next_snapshot)

    def plan(self):
        self._require_open()
        # Decode before returning or acknowledging progress. A failed conversion
        # must leave the selected frame available for retry.
        previous = copy(self._scan)
        generation = self._generation
        native_plan = self._scan.plan()
        self._check_generation(generation)
        if native_plan is None:
            return None
        try:
            plan = _from_native_plan(self.table, native_plan)
            self._check_generation(generation)
            return plan
        except Exception:
            if not self._closed and generation == self._generation:
                self._scan = previous
            raise

    def _flush_pending_consumer(self):
        if self._pending_consumer_snapshot is not None:
            self.notify_checkpoint_complete(self._pending_consumer_snapshot)
            self._pending_consumer_snapshot = None
            self._pending_consumer_owner = None

    async def stream(self) -> AsyncGenerator[Plan, None]:
        owner = object()
        try:
            while True:
                self._flush_pending_consumer()
                checkpoint = self.checkpoint()
                plan = self.plan()
                if plan is None:
                    next_snapshot = self.checkpoint()
                    if next_snapshot is not None and next_snapshot != checkpoint:
                        # Skipped frames have no rows awaiting caller processing.
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
            raise

    def stream_sync(self) -> Iterator[Plan]:
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

    def _check_generation(self, generation):
        self._require_open()
        if generation != self._generation:
            raise RuntimeError("stream scan was restored during planning")

    def close(self):
        """Stop polling without acknowledging a plan still owned by the caller."""
        self._closed = True
        self._generation += 1
        self._pending_consumer_snapshot = None
        self._pending_consumer_owner = None
