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

"""Backend-independent MERGE clauses and column references."""

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence, Union

SetSpec = Union[str, Mapping[str, Any]]
OnSpec = Union[Sequence[str], Mapping[str, str]]


@dataclass(frozen=True)
class SourceColumnRef:
    column: str


@dataclass(frozen=True)
class TargetColumnRef:
    column: str


@dataclass(frozen=True)
class LiteralValue:
    value: Any


def source_col(name: str) -> SourceColumnRef:
    return SourceColumnRef(name)


def target_col(name: str) -> TargetColumnRef:
    return TargetColumnRef(name)


def lit(value: Any) -> LiteralValue:
    return LiteralValue(value)


class WhenMatched:
    """Matched UPDATE/DELETE clause; callable SET values consume t./s. Arrow columns."""

    def __init__(
            self,
            action: str,
            *,
            update: Optional[SetSpec] = None,
            condition: Optional[str] = None):
        if action not in ("update", "delete"):
            raise ValueError("WhenMatched action must be 'update' or 'delete'.")
        if action == "update" and update is None:
            raise ValueError("WhenMatched.update requires an update spec.")
        if action == "delete" and update is not None:
            raise ValueError("WhenMatched.delete must not specify update.")
        self.update = update
        self.condition = condition
        self.delete = action == "delete"

    @classmethod
    def update(
            cls,
            update: SetSpec = "*",
            condition: Optional[str] = None):
        return cls("update", update=update, condition=condition)

    @classmethod
    def delete(cls, condition: Optional[str] = None):
        return cls("delete", condition=condition)


@dataclass
class WhenNotMatched:
    """INSERT clause; callable values consume s. Arrow columns and return Arrow arrays."""

    insert: SetSpec
    condition: Optional[str] = None


@dataclass
class _NormalizedClause:
    spec: Dict[str, Any]
    condition: Optional[str] = None
    delete: bool = False
