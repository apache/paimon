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

from dataclasses import dataclass

from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.table.row.generic_row import GenericRow


@dataclass
class IndexManifestEntry:
    """Manifest entry for index file."""

    kind: int  # 0 for ADD, 1 for DELETE
    partition: GenericRow
    bucket: int
    index_file: IndexFileMeta

    def __eq__(self, other):
        if not isinstance(other, IndexManifestEntry):
            return False
        return (self.kind == other.kind and
                self.partition == other.partition and
                self.bucket == other.bucket and
                self.index_file == other.index_file)

    def __hash__(self):
        return hash((self.kind, tuple(self.partition.values),
                     self.bucket, self.index_file))


INDEX_MANIFEST_ENTRY = {
    "type": "record",
    "name": "IndexManifestEntry",
    "fields": [
        {"name": "_VERSION", "type": "int"},
        {"name": "_KIND", "type": "byte"},
        {"name": "_PARTITION", "type": "bytes"},
        {"name": "_BUCKET", "type": "int"},
        {"name": "_INDEX_TYPE", "type": "string"},
        {"name": "_FILE_NAME", "type": "string"},
        {"name": "_FILE_SIZE", "type": "long"},
        {"name": "_ROW_COUNT", "type": "long"},
        {"name": "_DELETIONS_VECTORS_RANGES", "type": {"type": "array", "elementType": "DeletionVectorMeta"}},
        {"name": "_EXTERNAL_PATH", "type": ["null", "string"]},
        {"name": "_GLOBAL_INDEX", "type": "GlobalIndexMeta"}
    ]
}
