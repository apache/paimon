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
Index file metadata.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional

from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.index.deletion_vector_meta import DeletionVectorMeta


@dataclass
class IndexFileMeta:
    """
    Metadata for an index file.
    """
    index_type: str
    file_name: str
    file_size: int
    row_count: int
    # For deletion vector index - maps data file name to DeletionVectorMeta
    dv_ranges: Optional[Dict[str, DeletionVectorMeta]] = field(default=None)
    # External path for the index file (optional)
    external_path: Optional[str] = None
    # For global index (e.g., FAISS vector index)
    global_index_meta: Optional[GlobalIndexMeta] = None

    def __eq__(self, other):
        if not isinstance(other, IndexFileMeta):
            return False
        return (
            self.file_name == other.file_name and
            self.file_size == other.file_size and
            self.row_count == other.row_count and
            self.index_type == other.index_type
        )

    def __hash__(self):
        return hash((self.file_name, self.file_size, self.row_count, self.index_type))
