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
Index file handler for managing index manifest entries.
"""

from typing import List, Optional, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from pypaimon.snapshot.snapshot import Snapshot
    from pypaimon.manifest.index_manifest_entry import IndexManifestEntry


class IndexFileHandler:
    """
    Handle index files.

    Following Java's org.apache.paimon.index.IndexFileHandler.
    """

    def __init__(
        self,
        table
    ):
        self._table = table
        self._snapshot_manager = table.snapshot_manager()

    def scan(
        self,
        snapshot: Optional['Snapshot'],
        entry_filter: Optional[Callable[['IndexManifestEntry'], bool]] = None
    ) -> List['IndexManifestEntry']:
        if snapshot is None:
            snapshot = self._snapshot_manager.latest_snapshot()

        if snapshot is None:
            return []

        index_manifest = snapshot.index_manifest
        if not index_manifest:
            return []

        # Use IndexManifestFile to read Avro format
        from pypaimon.manifest.index_manifest_file import IndexManifestFile

        manifest_file = IndexManifestFile(self._table)
        entries = manifest_file.read(index_manifest)

        if entry_filter is not None:
            entries = [e for e in entries if entry_filter(e)]

        return entries
