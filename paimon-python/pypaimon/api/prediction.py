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

import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pypaimon.api.api_request import RESTRequest
from pypaimon.api.api_response import PagedResponse, RESTResponse
from pypaimon.catalog.prediction import (
    Prediction,
    PredictionOutput as CatalogPredictionOutput,
    PredictionSpec,
    PredictionStatus,
    _copy_string_list,
    _copy_string_mapping,
    _nonblank,
)
from pypaimon.common.json_util import json_field, optional_json_field
from pypaimon.common.identifier import Identifier


@dataclass
class PredictionSource:
    database: str
    table: str

    def __post_init__(self):
        _nonblank(self.database, "database")
        _nonblank(self.table, "table")

    @classmethod
    def from_identifier(cls, identifier: Identifier) -> "PredictionSource":
        return cls(
            identifier.get_database_name(), identifier.get_object_name())

    def to_identifier(self) -> Identifier:
        return Identifier.create(self.database, self.table)


@dataclass
class PredictionOutput:
    table: Optional[PredictionSource] = optional_json_field(
        "table", "non_null")

    def __post_init__(self):
        if self.table is not None and not isinstance(
                self.table, PredictionSource):
            raise ValueError("table must be a PredictionSource")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PredictionOutput":
        table = data.get("table")
        return cls(
            table=(PredictionSource(**table)
                   if isinstance(table, dict) else table),
        )

    def to_catalog_output(self) -> CatalogPredictionOutput:
        return CatalogPredictionOutput(
            table=(self.table.to_identifier()
                   if self.table is not None else None),
        )


@dataclass
class SubmitPredictionRequest(RESTRequest):
    source: PredictionSource
    model: str
    inputs: Dict[str, str]
    result_columns: List[str] = json_field("resultColumns")
    prompt: str
    output_schema: Dict[str, str] = json_field("outputSchema")
    name: Optional[str] = optional_json_field("name", "non_null")
    idempotency_key: Optional[str] = optional_json_field(
        "idempotencyKey", "non_null")
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
                (self.where, "where"),
                (self.tag_name, "tag_name"),
        ):
            if value is not None:
                _nonblank(value, field_name)
        if self.idempotency_key is None:
            self.idempotency_key = uuid.uuid4().hex
        else:
            _nonblank(self.idempotency_key, "idempotency_key")
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
    def from_spec(
            cls, source: Identifier, spec: PredictionSpec
    ) -> "SubmitPredictionRequest":
        return cls(
            source=PredictionSource.from_identifier(source),
            name=spec.name,
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
    prediction_id: str = json_field("predictionId")
    source: PredictionSource = None
    status: PredictionStatus = None
    snapshot_id: int = json_field("snapshotId", default=0)
    model: str = ""
    model_version: str = json_field("modelVersion", default="")
    name: Optional[str] = optional_json_field("name", "non_null")
    output: Optional[PredictionOutput] = optional_json_field(
        "output", "non_null")
    start_time: Optional[int] = optional_json_field("startTime", "non_null")
    end_time: Optional[int] = optional_json_field("endTime", "non_null")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GetPredictionResponse":
        return cls(
            prediction_id=data.get("predictionId"),
            source=(
                PredictionSource(**data.get("source"))
                if isinstance(data.get("source"), dict)
                else data.get("source")
            ),
            name=data.get("name"),
            status=PredictionStatus.from_dict(data.get("status")),
            snapshot_id=data.get("snapshotId"),
            model=data.get("model"),
            model_version=data.get("modelVersion"),
            output=(PredictionOutput.from_dict(data.get("output"))
                    if isinstance(data.get("output"), dict)
                    else data.get("output")),
            start_time=data.get("startTime"),
            end_time=data.get("endTime"),
        )

    def to_prediction(self) -> Prediction:
        return Prediction(
            prediction_id=self.prediction_id,
            source=self.source.to_identifier(),
            name=self.name,
            status=self.status,
            snapshot_id=self.snapshot_id,
            model=self.model,
            model_version=self.model_version,
            output=(self.output.to_catalog_output()
                    if self.output is not None else None),
            start_time=self.start_time,
            end_time=self.end_time,
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
