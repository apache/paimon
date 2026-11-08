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


class PredictionStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def terminal(self) -> bool:
        return self in {
            PredictionStatus.SUCCEEDED,
            PredictionStatus.PARTIAL_SUCCESS,
            PredictionStatus.FAILED,
            PredictionStatus.CANCELLED,
        }


@dataclass
class PredictionSpec:
    """Provider-neutral input for an asynchronous table prediction."""

    model: str
    inputs: Dict[str, str]
    result_columns: List[str]
    prompt: str
    output_schema: Dict[str, str]
    name: Optional[str] = None
    request_id: Optional[str] = None
    where: Optional[str] = None
    snapshot_id: Optional[int] = None
    tag_name: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        _nonblank(self.model, "model")
        _nonblank(self.prompt, "prompt")
        for value, name in (
                (self.name, "name"),
                (self.request_id, "request_id"),
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
        self.result_columns = _copy_string_list(
            self.result_columns, "result_columns")
        self.output_schema = _copy_string_mapping(
            self.output_schema, "output_schema", True)
        if not isinstance(self.options, dict):
            raise ValueError("options must be a mapping")
        self.options = dict(self.options)


@dataclass
class Prediction:
    """Durable metadata for one asynchronous table prediction."""

    job_id: str
    status: PredictionStatus
    snapshot_id: int
    model: str
    model_version: str
    output_table: str
    created_at: int
    name: Optional[str] = None
    started_at: Optional[int] = None
    finished_at: Optional[int] = None
    error: Optional[str] = None
    total: Optional[int] = None
    succeeded: Optional[int] = None
    failed: Optional[int] = None

    def __post_init__(self):
        _nonblank(self.job_id, "job_id")
        if isinstance(self.status, str):
            self.status = PredictionStatus(self.status)
        if not isinstance(self.status, PredictionStatus):
            raise ValueError("status must be a PredictionStatus")
        if type(self.snapshot_id) is not int or self.snapshot_id <= 0:
            raise ValueError("snapshot_id must be a positive integer")
        _nonblank(self.model, "model")
        _nonblank(self.model_version, "model_version")
        _nonblank(self.output_table, "output_table")
        if type(self.created_at) is not int or self.created_at < 0:
            raise ValueError("created_at must be a non-negative integer")
        for name in (
                "started_at", "finished_at", "total", "succeeded", "failed"):
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
