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
from typing import Dict, List, Optional

from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import json_field
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import PartitionStatistics


class RESTRequest(ABC):
    """RESTRequest"""


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


@dataclass
class RenameTableRequest(RESTRequest):
    FIELD_SOURCE = "source"
    FIELD_DESTINATION = "destination"

    source: Identifier = json_field(FIELD_SOURCE)
    destination: Identifier = json_field(FIELD_DESTINATION)


@dataclass
class CreateTableRequest(RESTRequest):
    FIELD_IDENTIFIER = "identifier"
    FIELD_SCHEMA = "schema"

    identifier: Identifier = json_field(FIELD_IDENTIFIER)
    schema: Schema = json_field(FIELD_SCHEMA)


@dataclass
class CommitTableRequest(RESTRequest):
    FIELD_TABLE_UUID = "tableUuid"
    FIELD_SNAPSHOT = "snapshot"
    FIELD_STATISTICS = "statistics"

    table_uuid: Optional[str] = json_field(FIELD_TABLE_UUID)
    snapshot: Snapshot = json_field(FIELD_SNAPSHOT)
    statistics: List[PartitionStatistics] = json_field(FIELD_STATISTICS)


@dataclass
class AlterTableRequest(RESTRequest):
    FIELD_CHANGES = "changes"

    changes: List[SchemaChange] = json_field(FIELD_CHANGES)
