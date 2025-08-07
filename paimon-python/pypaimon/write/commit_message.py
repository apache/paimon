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

from typing import List, Tuple

from pypaimon.manifest.schema.data_file_meta import DataFileMeta


class CommitMessage:
    """Python implementation of CommitMessage"""

    def __init__(self, partition: Tuple, bucket: int, new_files: List[DataFileMeta]):
        self._partition = partition
        self._bucket = bucket
        self._new_files = new_files or []

    def partition(self) -> Tuple:
        """Get the partition of this commit message."""
        return self._partition

    def bucket(self) -> int:
        """Get the bucket of this commit message."""
        return self._bucket

    def new_files(self) -> List[DataFileMeta]:
        """Get the list of new files."""
        return self._new_files

    def is_empty(self):
        return not self._new_files
