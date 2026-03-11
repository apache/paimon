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
"""TableSnapshot class with snapshot and table statistics."""

from dataclasses import dataclass

from pypaimon.common.json_util import json_field
from pypaimon.snapshot.snapshot import Snapshot


@dataclass
class TableSnapshot:
    """Snapshot of a table, including basic statistics of this table.

    This class wraps a Snapshot and provides additional table-level statistics
    such as record count, file size, file count, and last file creation time.
    """

    # Required fields
    snapshot: Snapshot = json_field("snapshot")
    record_count: int = json_field("recordCount")
    file_size_in_bytes: int = json_field("fileSizeInBytes")
    file_count: int = json_field("fileCount")
    last_file_creation_time: int = json_field("lastFileCreationTime")

    @property
    def id(self) -> int:
        """Get the snapshot ID.

        Returns:
            The snapshot ID
        """
        return self.snapshot.id

    @property
    def schema_id(self) -> int:
        """Get the schema ID.

        Returns:
            The schema ID
        """
        return self.snapshot.schema_id

    @property
    def time_millis(self) -> int:
        """Get the snapshot time in milliseconds.

        Returns:
            The snapshot time
        """
        return self.snapshot.time_millis