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

"""Default merge function for primary-key tables.

Mirrors Java ``DeduplicateMergeFunction`` -- for a run of KVs sharing
the same primary key, keep only the one with the highest sequence
number (by virtue of ``add`` being called in sequence-number order).
"""

from typing import Optional

from pypaimon.table.row.key_value import KeyValue


class DeduplicateMergeFunction:
    """Keep only the latest KV per primary key.

    Used by both the read path (``SortMergeReaderWithMinHeap``) and the
    write path (``KeyValueDataWriter`` in-memory merge buffer) -- the
    latter is what enforces the LSM "PK unique within a file"
    invariant on flush.
    """

    def __init__(self):
        self.latest_kv: Optional[KeyValue] = None

    def reset(self) -> None:
        self.latest_kv = None

    def add(self, kv: KeyValue) -> None:
        self.latest_kv = kv

    def get_result(self) -> Optional[KeyValue]:
        return self.latest_kv
