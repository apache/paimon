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
from typing import Any, Dict, List, Optional

from pypaimon.api.api_request import RESTRequest
from pypaimon.api.api_response import PagedResponse, RESTResponse
from pypaimon.catalog.prediction import (
    Prediction,
    PredictionSpec,
    PredictionStatus,
    _copy_string_list,
    _copy_string_mapping,
    _nonblank,
)
from pypaimon.common.json_util import json_field, optional_json_field


@dataclass
class SubmitPredictionRequest(RESTRequest):
    model: str
    inputs: Dict[str, str]
    result_columns: List[str] = json_field("resultColumns")
    prompt: str
    output_schema: Dict[str, str] = json_field("outputSchema")
    name: Optional[str] = optional_json_field("name", "non_null")
    request_id: Optional[str] = optional_json_field("requestId", "non_null")
    where: Optional[str] = optional_json_field("where", "non_null")
    snapshot_id: Optional[int] = optional_json_field("snapshotId", "non_null")
    tag_name: Optional[str] = optional_json_field("tagName", "non_null")
    options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        for value, field_name in (
                (self.model, "model"),
                (self.prompt, "prompt"),
        ):
            _nonblank(value, field_name)
        for value, field_name in (
                (self.name, "name"),
                (self.request_id, "request_id"),
                (self.where, "where"),
                (self.tag_name, "tag_name"),
        ):
            if value is not None:
                _nonblank(value, field_name)
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

    @classmethod
    def from_spec(cls, spec: PredictionSpec) -> "SubmitPredictionRequest":
        return cls(
            name=spec.name,
            request_id=spec.request_id,
            model=spec.model,
            inputs=spec.inputs,
            result_columns=spec.result_columns,
            prompt=spec.prompt,
            output_schema=spec.output_schema,
            where=spec.where,
            snapshot_id=spec.snapshot_id,
            tag_name=spec.tag_name,
            options=spec.options,
        )


@dataclass
class CancelPredictionRequest(RESTRequest):
    """Empty body for a prediction cancellation request."""

    pass


@dataclass
class GetPredictionResponse(RESTResponse):
    job_id: str = json_field("jobId")
    status: str = ""
    snapshot_id: int = json_field("snapshotId", default=0)
    model: str = ""
    model_version: str = json_field("modelVersion", default="")
    output_table: str = json_field("outputTable", default="")
    created_at: int = json_field("createdAt", default=0)
    name: Optional[str] = optional_json_field("name", "non_null")
    started_at: Optional[int] = optional_json_field("startedAt", "non_null")
    finished_at: Optional[int] = optional_json_field("finishedAt", "non_null")
    error: Optional[str] = optional_json_field("error", "non_null")
    total: Optional[int] = optional_json_field("total", "non_null")
    succeeded: Optional[int] = optional_json_field("succeeded", "non_null")
    failed: Optional[int] = optional_json_field("failed", "non_null")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GetPredictionResponse":
        return cls(
            job_id=data.get("jobId"),
            name=data.get("name"),
            status=data.get("status"),
            snapshot_id=data.get("snapshotId"),
            model=data.get("model"),
            model_version=data.get("modelVersion"),
            output_table=data.get("outputTable"),
            created_at=data.get("createdAt"),
            started_at=data.get("startedAt"),
            finished_at=data.get("finishedAt"),
            error=data.get("error"),
            total=data.get("total"),
            succeeded=data.get("succeeded"),
            failed=data.get("failed"),
        )

    def to_prediction(self) -> Prediction:
        return Prediction(
            job_id=self.job_id,
            name=self.name,
            status=PredictionStatus(self.status),
            snapshot_id=self.snapshot_id,
            model=self.model,
            model_version=self.model_version,
            output_table=self.output_table,
            created_at=self.created_at,
            started_at=self.started_at,
            finished_at=self.finished_at,
            error=self.error,
            total=self.total,
            succeeded=self.succeeded,
            failed=self.failed,
        )


@dataclass
class ListPredictionsResponse(PagedResponse[GetPredictionResponse]):
    predictions: List[GetPredictionResponse] = field(default_factory=list)
    next_page_token: Optional[str] = json_field("nextPageToken", default=None)

    def __post_init__(self):
        if self.predictions is None:
            self.predictions = []

    def data(self) -> List[GetPredictionResponse]:
        return self.predictions

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ListPredictionsResponse":
        return cls(
            predictions=[
                GetPredictionResponse.from_dict(value)
                if isinstance(value, dict) else value
                for value in (data.get("predictions") or [])
            ],
            next_page_token=data.get("nextPageToken"),
        )
