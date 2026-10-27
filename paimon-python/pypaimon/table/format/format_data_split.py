# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from dataclasses import dataclass
from typing import Dict, Optional, Any


@dataclass(frozen=True)
class FormatDataSplit:
    """Split for format table: one file (or future: byte range) per split."""

    file_path: str
    file_size: int
    partition: Optional[Dict[str, Any]] = None  # partition column name -> value
    # Earlier PyPaimon versions named directories with unescaped values: the directory's values read that
    # way, when they differ, and whether the directory is only an earlier version's, such as pt=a=b.
    legacy_partition: Optional[Dict[str, Any]] = None
    legacy_directory: bool = False
    # The partition filter whose rows a read keeps, for files that keep their partition columns, and whether
    # the directory's own values match it.
    row_filter: Optional[Dict[str, Any]] = None
    directory_matches: bool = True

    def data_path(self) -> str:
        return self.file_path
