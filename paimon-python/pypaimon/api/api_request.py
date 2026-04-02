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
from pypaimon.function.function_change import FunctionChange
from pypaimon.function.function_definition import FunctionDefinition
from pypaimon.schema.data_types import DataField
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import PartitionStatistics
from pypaimon.table.instant import Instant


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
    FIELD_TABLE_ID = "tableId"
    FIELD_SNAPSHOT = "snapshot"
    FIELD_STATISTICS = "statistics"

    table_id: Optional[str] = json_field(FIELD_TABLE_ID)
    snapshot: Snapshot = json_field(FIELD_SNAPSHOT)
    statistics: List[PartitionStatistics] = json_field(FIELD_STATISTICS)


@dataclass
class AlterTableRequest(RESTRequest):
    FIELD_CHANGES = "changes"

    changes: List[SchemaChange] = json_field(FIELD_CHANGES)


@dataclass
class RollbackTableRequest(RESTRequest):
    FIELD_INSTANT = "instant"
    FIELD_FROM_SNAPSHOT = "fromSnapshot"

    instant: Instant = json_field(FIELD_INSTANT)
    from_snapshot: Optional[int] = json_field(FIELD_FROM_SNAPSHOT)


@dataclass
class CreateFunctionRequest(RESTRequest):
    FIELD_NAME = "name"
    FIELD_INPUT_PARAMS = "inputParams"
    FIELD_RETURN_PARAMS = "returnParams"
    FIELD_DETERMINISTIC = "deterministic"
    FIELD_DEFINITIONS = "definitions"
    FIELD_COMMENT = "comment"
    FIELD_OPTIONS = "options"

    name: str = json_field(FIELD_NAME)
    input_params: Optional[List[DataField]] = json_field(FIELD_INPUT_PARAMS, default=None)
    return_params: Optional[List[DataField]] = json_field(FIELD_RETURN_PARAMS, default=None)
    deterministic: bool = json_field(FIELD_DETERMINISTIC, default=False)
    definitions: Optional[Dict[str, FunctionDefinition]] = json_field(FIELD_DEFINITIONS, default=None)
    comment: Optional[str] = json_field(FIELD_COMMENT, default=None)
    options: Optional[Dict[str, str]] = json_field(FIELD_OPTIONS, default=None)

    def to_dict(self) -> Dict:
        result = {
            self.FIELD_NAME: self.name,
            self.FIELD_DETERMINISTIC: self.deterministic,
        }
        if self.input_params is not None:
            result[self.FIELD_INPUT_PARAMS] = [
                p.to_dict() if hasattr(p, 'to_dict') else p for p in self.input_params
            ]
        else:
            result[self.FIELD_INPUT_PARAMS] = None
        if self.return_params is not None:
            result[self.FIELD_RETURN_PARAMS] = [
                p.to_dict() if hasattr(p, 'to_dict') else p for p in self.return_params
            ]
        else:
            result[self.FIELD_RETURN_PARAMS] = None
        if self.definitions is not None:
            result[self.FIELD_DEFINITIONS] = {
                k: v.to_dict() if hasattr(v, 'to_dict') else v
                for k, v in self.definitions.items()
            }
        else:
            result[self.FIELD_DEFINITIONS] = None
        result[self.FIELD_COMMENT] = self.comment
        result[self.FIELD_OPTIONS] = self.options
        return result


@dataclass
class AlterFunctionRequest(RESTRequest):
    FIELD_CHANGES = "changes"

    changes: List[FunctionChange] = json_field(FIELD_CHANGES)

    def to_dict(self) -> Dict:
        return {
            self.FIELD_CHANGES: [c.to_dict() for c in self.changes]
        }
