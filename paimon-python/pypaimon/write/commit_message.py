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

from dataclasses import dataclass, field
from typing import List, Tuple, Optional

from pypaimon.manifest.schema.data_file_meta import DataFileMeta


@dataclass
class CommitMessage:
    partition: Tuple
    bucket: int
    new_files: List[DataFileMeta]
    check_from_snapshot: Optional[int] = -1
    changelog_files: List[DataFileMeta] = field(default_factory=list)

    def is_empty(self):
        return not self.new_files and not self.changelog_files
