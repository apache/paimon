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

from dataclasses import dataclass, field
from typing import Set, Tuple


@dataclass
class ScanStats:
    """Counters accumulated by :class:`FileScanner` when it is asked to
    track stats for ``ReadBuilder.explain()``.

    The scanner mutates these counters in place; consumers should treat
    instances as immutable once the scan returns. Default factory values
    keep the dataclass usable both as a blank "no tracking" sentinel and
    as a live accumulator.
    """

    manifest_files_total: int = 0
    manifest_files_after_partition: int = 0

    # ``entries_potential_total`` is the row count we would have processed if no
    # filtering occurred — derived from manifest-file metadata before the
    # manifest-level partition skip. ``entries_total`` is what actually reached
    # ``_filter_manifest_entry``; the gap between the two captures
    # manifest-level pruning.
    entries_potential_total: int = 0
    entries_total: int = 0
    entries_after_partition: int = 0
    entries_after_bucket: int = 0
    entries_after_stats: int = 0

    partition_keys_before: Set[Tuple] = field(default_factory=set)
    partition_keys_after: Set[Tuple] = field(default_factory=set)

    buckets_seen: Set[Tuple[Tuple, int]] = field(default_factory=set)
    buckets_after_pruning: Set[Tuple[Tuple, int]] = field(default_factory=set)
