#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
"""TableSnapshot wrapper class."""


from pypaimon.snapshot.snapshot import Snapshot


class TableSnapshot:
    """Wrapper for snapshot that includes table metadata.
    
    This class wraps a Snapshot and provides additional table-level information.
    """

    def __init__(self, snapshot: Snapshot):
        """Initialize TableSnapshot with a Snapshot.
        
        Args:
            snapshot: The Snapshot instance to wrap
        """
        self._snapshot = snapshot

    def snapshot(self) -> Snapshot:
        """Get the underlying Snapshot.
        
        Returns:
            The wrapped Snapshot instance
        """
        return self._snapshot

    def snapshot_json(self) -> str:
        """Get the snapshot as JSON string.
        
        Returns:
            JSON string representation of the snapshot
        """
        from pypaimon.common.json_util import JSON
        return JSON.to_json(self._snapshot)

    @property
    def id(self) -> int:
        """Get the snapshot ID.
        
        Returns:
            The snapshot ID
        """
        return self._snapshot.id

    @property
    def schema_id(self) -> int:
        """Get the schema ID.
        
        Returns:
            The schema ID
        """
        return self._snapshot.schema_id

    @property
    def time_millis(self) -> int:
        """Get the snapshot time in milliseconds.
        
        Returns:
            The snapshot time
        """
        return self._snapshot.time_millis
