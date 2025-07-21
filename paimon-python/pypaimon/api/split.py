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
#################################################################################

from abc import ABC, abstractmethod

from typing import Iterator


class Split(ABC):
    """An input split for reading. The most important subclass is DataSplit."""

    @abstractmethod
    def row_count(self) -> int:
        """Return the total row count of the split."""

    def file_size(self) -> int:
        """Return the total file size of the split."""

    def file_paths(self) -> Iterator[str]:
        """Return the paths of all raw files in the split."""
