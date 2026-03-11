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
"""ChangelogFollowUpScanner for tables with changelog-producer settings."""

from pypaimon.read.scanner.follow_up_scanner import FollowUpScanner
from pypaimon.snapshot.snapshot import Snapshot


class ChangelogFollowUpScanner(FollowUpScanner):
    """Scans any commit that has a changelog_manifest_list."""

    def should_scan(self, snapshot: Snapshot) -> bool:
        changelog_list = snapshot.changelog_manifest_list
        return changelog_list is not None and changelog_list != ""
