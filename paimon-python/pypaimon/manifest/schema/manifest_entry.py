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

from dataclasses import dataclass

from pypaimon.manifest.schema.data_file_meta import (DATA_FILE_META_SCHEMA,
                                                     DataFileMeta)
from pypaimon.table.row.generic_row import GenericRow


@dataclass
class ManifestEntry:
    kind: int
    partition: GenericRow
    bucket: int
    total_buckets: int
    file: DataFileMeta

    def assign_first_row_id(self, first_row_id: int) -> 'ManifestEntry':
        """Create a new ManifestEntry with the assigned first_row_id."""
        return ManifestEntry(
            kind=self.kind,
            partition=self.partition,
            bucket=self.bucket,
            total_buckets=self.total_buckets,
            file=self.file.assign_first_row_id(first_row_id)
        )

    def assign_sequence_number(self, min_sequence_number: int, max_sequence_number: int) -> 'ManifestEntry':
        """Create a new ManifestEntry with the assigned sequence numbers."""
        return ManifestEntry(
            kind=self.kind,
            partition=self.partition,
            bucket=self.bucket,
            total_buckets=self.total_buckets,
            file=self.file.assign_sequence_number(min_sequence_number, max_sequence_number)
        )


MANIFEST_ENTRY_SCHEMA = {
    "type": "record",
    "name": "ManifestEntry",
    "fields": [
        {"name": "_VERSION", "type": "int"},
        {"name": "_KIND", "type": "int"},
        {"name": "_PARTITION", "type": "bytes"},
        {"name": "_BUCKET", "type": "int"},
        {"name": "_TOTAL_BUCKETS", "type": "int"},
        {"name": "_FILE", "type": DATA_FILE_META_SCHEMA}
    ]
}
