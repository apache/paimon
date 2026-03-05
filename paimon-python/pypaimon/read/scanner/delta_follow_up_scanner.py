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
DeltaFollowUpScanner for append-only streaming reads.

This scanner only processes APPEND commits, skipping compaction, overwrite,
and other maintenance operations. It is the Python equivalent of Java's
DeltaFollowUpScanner used when changelog producer is NONE.
"""

from pypaimon.read.scanner.follow_up_scanner import FollowUpScanner
from pypaimon.snapshot.snapshot import Snapshot


class DeltaFollowUpScanner(FollowUpScanner):
    """
    FollowUpScanner for changelog producer NONE.

    This scanner only scans APPEND commits, which contain new data files.
    Other commit types (COMPACT, OVERWRITE, EXPIRE, ANALYZE) are skipped
    as they don't add new user data.
    """

    def should_scan(self, snapshot: Snapshot) -> bool:
        """
        Determine whether to scan the snapshot based on its commit kind.

        Only APPEND commits are scanned, as they contain new data.
        Other commit types are maintenance operations that don't add user data.

        Args:
            snapshot: The snapshot to evaluate

        Returns:
            True if snapshot.commit_kind == "APPEND", False otherwise
        """
        return snapshot.commit_kind == "APPEND"
