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

from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.split import Split


@dataclass
class Plan:
    """Implementation of Plan for native Python reading."""
    _files: List[ManifestEntry]
    _splits: List[Split]

    def files(self) -> List[ManifestEntry]:
        return self._files

    def splits(self) -> List[Split]:
        return self._splits
