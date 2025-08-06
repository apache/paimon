"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from pathlib import Path
from typing import Optional

from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier


class RESTTokenFileIO(FileIO):

    def __init__(self, identifier: Identifier, path: Path, warehouse: Optional[str] = None,
                 catalog_options: Optional[dict] = None):
        super().__init__(warehouse, catalog_options)
        self.identifier = identifier
        self.path = path

    def exists(self, path: Path) -> bool:
        pass
