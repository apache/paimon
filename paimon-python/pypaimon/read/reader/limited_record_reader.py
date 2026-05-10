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

"""Row-level limit wrapper for any ``RecordReader`` chain.

Currently used at the outermost stage of the PK merge-on-read pipeline
so the merge output is short-circuited at the row level instead of
running to completion, but the wrapper itself is generic and may be
reused on other reader chains.
"""

from typing import Optional

from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader


class LimitedRecordReader(RecordReader):
    """Stop emitting rows once ``limit`` rows have been delivered."""

    def __init__(self, inner: RecordReader, limit: int):
        if limit < 0:
            raise ValueError("limit must be non-negative, got %d" % limit)
        self._inner = inner
        self._limit = limit
        # Public so the iterator can read/write the shared counter without
        # going through accessor calls per row.
        self.count = 0

    def read_batch(self) -> Optional[RecordIterator]:
        if self.count >= self._limit:
            return None
        batch = self._inner.read_batch()
        if batch is None:
            return None
        return _LimitedRecordIterator(batch, self)

    def close(self) -> None:
        self._inner.close()


class _LimitedRecordIterator(RecordIterator):

    def __init__(self, inner: RecordIterator, limiter: LimitedRecordReader):
        self._inner = inner
        self._limiter = limiter

    def next(self):
        if self._limiter.count >= self._limiter._limit:
            return None
        row = self._inner.next()
        if row is None:
            return None
        self._limiter.count += 1
        return row
