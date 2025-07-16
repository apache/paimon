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

from abc import ABC
from dataclasses import dataclass
from typing import Dict, List

from .rest_json import json_field


class RESTRequest(ABC):
    pass


@dataclass
class CreateDatabaseRequest(RESTRequest):
    FIELD_NAME = "name"
    FIELD_OPTIONS = "options"

    name: str = json_field(FIELD_NAME)
    options: Dict[str, str] = json_field(FIELD_OPTIONS)


@dataclass
class AlterDatabaseRequest(RESTRequest):
    FIELD_REMOVALS = "removals"
    FIELD_UPDATES = "updates"

    removals: List[str] = json_field(FIELD_REMOVALS)
    updates: Dict[str, str] = json_field(FIELD_UPDATES)
