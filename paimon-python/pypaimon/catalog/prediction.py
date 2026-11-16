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

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import optional_json_field


class PredictionState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    TERMINATING = "TERMINATING"
    TERMINATED = "TERMINATED"


class PredictionTerminationCode(str, Enum):
    SUCCESS = "SUCCESS"
    SUCCESS_WITH_FAILURES = "SUCCESS_WITH_FAILURES"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


@dataclass
class PredictionTerminationDetails:
    """Result and diagnostic message available after termination."""

    code: PredictionTerminationCode
    message: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.code, str):
            self.code = PredictionTerminationCode(self.code)
        if not isinstance(self.code, PredictionTerminationCode):
            raise ValueError("code must be a PredictionTerminationCode")


@dataclass
class PredictionStatus:
    """Lifecycle and optional termination details of a prediction."""

    state: PredictionState
    termination_details: Optional[PredictionTerminationDetails] = \
        optional_json_field("terminationDetails", "non_null")

    def __post_init__(self):
        if isinstance(self.state, str):
            self.state = PredictionState(self.state)
        if not isinstance(self.state, PredictionState):
            raise ValueError("state must be a PredictionState")
        terminating = self.state in {
            PredictionState.TERMINATING,
            PredictionState.TERMINATED,
        }
        if terminating != (self.termination_details is not None):
            raise ValueError(
                "termination_details must be present only for "
                "terminating states")
        if self.termination_details is not None and not isinstance(
                self.termination_details, PredictionTerminationDetails):
            raise ValueError(
                "termination_details must be PredictionTerminationDetails")

    @classmethod
    def from_dict(cls, value):
        details = value.get("terminationDetails")
        return cls(
            state=value.get("state"),
            termination_details=(
                PredictionTerminationDetails(**details)
                if details is not None else None),
        )

    def to_dict(self):
        result = {"state": self.state.value}
        if self.termination_details is not None:
            result["terminationDetails"] = {
                "code": self.termination_details.code.value,
            }
            if self.termination_details.message is not None:
                result["terminationDetails"]["message"] = \
                    self.termination_details.message
        return result

    @property
    def terminal(self) -> bool:
        return self.state is PredictionState.TERMINATED


@dataclass
class PredictionSpec:
    """Provider-neutral input for an asynchronous table prediction."""

    model: str
    inputs: Dict[str, str]
    instructions: str
    output_schema: Dict[str, str]
    merge_key_columns: List[str] = field(default_factory=list)
    name: Optional[str] = None
    where: Optional[str] = None
    snapshot_id: Optional[int] = None
    tag_name: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        _nonblank(self.model, "model")
        _nonblank(self.instructions, "instructions")
        for value, name in (
                (self.name, "name"),
                (self.where, "where"),
                (self.tag_name, "tag_name"),
        ):
            if value is not None:
                _nonblank(value, name)
        if self.snapshot_id is not None and (
                type(self.snapshot_id) is not int or self.snapshot_id <= 0):
            raise ValueError("snapshot_id must be a positive integer")
        if self.snapshot_id is not None and self.tag_name is not None:
            raise ValueError("snapshot_id and tag_name are mutually exclusive")
        self.inputs = _copy_string_mapping(self.inputs, "inputs", True)
        self.merge_key_columns = _copy_string_list(
            self.merge_key_columns, "merge_key_columns")
        self.output_schema = _copy_string_mapping(
            self.output_schema, "output_schema", True)
        if not isinstance(self.options, dict):
            raise ValueError("options must be a mapping")
        self.options = dict(self.options)


@dataclass
class PredictionOutput:
    """Known output locations of an asynchronous prediction."""

    table: Optional[Identifier] = None

    def __post_init__(self):
        if self.table is not None and not isinstance(self.table, Identifier):
            raise ValueError("table must be an Identifier")


@dataclass
class Prediction:
    """Durable metadata for one asynchronous prediction job."""

    prediction_id: str
    source: Identifier
    status: PredictionStatus
    snapshot_id: int
    model: str
    name: Optional[str] = None
    output: Optional[PredictionOutput] = None
    start_time: Optional[int] = None
    end_time: Optional[int] = None

    def __post_init__(self):
        _nonblank(self.prediction_id, "prediction_id")
        if not isinstance(self.source, Identifier):
            raise ValueError("source must be an Identifier")
        if not isinstance(self.status, PredictionStatus):
            raise ValueError("status must be a PredictionStatus")
        if type(self.snapshot_id) is not int or self.snapshot_id <= 0:
            raise ValueError("snapshot_id must be a positive integer")
        _nonblank(self.model, "model")
        if self.output is not None and not isinstance(
                self.output, PredictionOutput):
            raise ValueError("output must be a PredictionOutput")
        for name in ("start_time", "end_time"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(
                    "{} must be a non-negative integer".format(name))


def _nonblank(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("{} must be a non-empty string".format(name))


def _copy_string_mapping(values, name, required):
    if not isinstance(values, dict) or (required and not values):
        raise ValueError("{} must be a non-empty mapping".format(name))
    result = {}
    for key, value in values.items():
        _nonblank(key, "{} key".format(name))
        _nonblank(value, "{} value".format(name))
        result[key] = value
    return result


def _copy_string_list(values, name):
    if not isinstance(values, list):
        raise ValueError("{} must be a list".format(name))
    result = []
    for value in values:
        _nonblank(value, "{} value".format(name))
        if value in result:
            raise ValueError("{} must not contain duplicates".format(name))
        result.append(value)
    return result
