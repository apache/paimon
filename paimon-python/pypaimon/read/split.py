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
from typing import List

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.table.row.generic_row import GenericRow


@dataclass
class Split:
    """Implementation of Split for native Python reading."""
    files: List[DataFileMeta]
    partition: GenericRow
    bucket: int
    _file_paths: List[str]
    _row_count: int
    _file_size: int
    split_start_row: int = None
    split_end_row: int = None
    raw_convertible: bool = False

    @property
    def row_count(self) -> int:
        return self._row_count

    @property
    def file_size(self) -> int:
        return self._file_size

    @property
    def file_paths(self) -> List[str]:
        return self._file_paths
