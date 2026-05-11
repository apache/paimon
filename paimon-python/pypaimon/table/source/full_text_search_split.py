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

"""Split of full-text search."""

from dataclasses import dataclass
from typing import List

from pypaimon.index.index_file_meta import IndexFileMeta


@dataclass
class FullTextSearchSplit:
    """Split of full-text search."""

    row_range_start: int
    row_range_end: int
    full_text_index_files: List[IndexFileMeta]

    def __eq__(self, other):
        if not isinstance(other, FullTextSearchSplit):
            return False
        return (
            self.row_range_start == other.row_range_start
            and self.row_range_end == other.row_range_end
            and self.full_text_index_files == other.full_text_index_files
        )

    def __hash__(self):
        return hash((self.row_range_start, self.row_range_end, tuple(self.full_text_index_files)))

    def __repr__(self):
        return (
            f"FullTextSearchSplit("
            f"row_range_start={self.row_range_start}, "
            f"row_range_end={self.row_range_end}, "
            f"full_text_index_files={self.full_text_index_files})"
        )
