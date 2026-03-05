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
FollowUpScanner interface for streaming table scans.

This is the Python equivalent of Java's FollowUpScanner interface used in
DataTableStreamScan for follow-up planning after the initial scan.
"""

from abc import ABC, abstractmethod

from pypaimon.snapshot.snapshot import Snapshot


class FollowUpScanner(ABC):
    """
    Helper class for the follow-up planning of streaming table scans.

    After the initial scan (handled by StartingScanner), the FollowUpScanner
    determines which subsequent snapshots should be scanned and how to read
    their delta data.
    """

    @abstractmethod
    def should_scan(self, snapshot: Snapshot) -> bool:
        """
        Determine whether the given snapshot should be scanned.

        Args:
            snapshot: The snapshot to evaluate

        Returns:
            True if the snapshot should be scanned, False to skip it
        """
