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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pypaimon.api.api_request import RESTRequest
from pypaimon.api.api_response import PagedResponse, RESTResponse
from pypaimon.catalog.model import (
    Model,
    ModelFields,
    _nonblank,
    _validate_model_fields,
    model_fields_from_wire,
    model_fields_to_wire,
)
from pypaimon.common.identifier import Identifier


@dataclass
class CreateModelRequest(RESTRequest):
    name: str
    provider: str
    model: str
    inputs: ModelFields
    outputs: Optional[ModelFields] = None
    properties: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        _nonblank(self.name, "name")
        _nonblank(self.provider, "provider")
        _nonblank(self.model, "model")
        self.inputs = _validate_model_fields(self.inputs, "inputs", True)
        self.outputs = _validate_model_fields(self.outputs, "outputs", False)
        if not isinstance(self.properties, dict):
            raise ValueError("properties must be a mapping")
        for key in self.properties:
            _nonblank(key, "property name")
        self.properties = dict(self.properties)

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "name": self.name,
            "provider": self.provider,
            "model": self.model,
            "inputs": model_fields_to_wire(self.inputs),
            "properties": dict(self.properties),
        }
        if self.outputs is not None:
            result["outputs"] = model_fields_to_wire(self.outputs)
        return result


@dataclass
class GetModelResponse(RESTResponse):
    name: str
    provider: str
    model: str
    inputs: ModelFields
    outputs: Optional[ModelFields] = None
    properties: Dict[str, Any] = field(default_factory=dict)

    def to_model(self, database_name: str) -> Model:
        return Model(
            identifier=Identifier.create(database_name, self.name),
            provider=self.provider,
            model=self.model,
            inputs=self.inputs,
            outputs=self.outputs,
            properties=self.properties,
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GetModelResponse":
        return cls(
            name=data.get("name"),
            provider=data.get("provider"),
            model=data.get("model"),
            inputs=model_fields_from_wire(data.get("inputs")),
            outputs=model_fields_from_wire(data.get("outputs")),
            properties=data.get("properties") or {},
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "provider": self.provider,
            "model": self.model,
            "inputs": model_fields_to_wire(self.inputs),
            "outputs": model_fields_to_wire(self.outputs),
            "properties": dict(self.properties),
        }


@dataclass
class ListModelsResponse(PagedResponse[GetModelResponse]):
    models: Optional[List[GetModelResponse]] = None
    next_page_token: Optional[str] = None

    def data(self) -> Optional[List[GetModelResponse]]:
        return self.models

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ListModelsResponse":
        models = data.get("models")
        if models is not None:
            models = [
                GetModelResponse.from_dict(model) if isinstance(model, dict) else model
                for model in models
            ]
        return cls(models=models, next_page_token=data.get("nextPageToken"))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "models": (
                [model.to_dict() for model in self.models]
                if self.models is not None else None
            ),
            "nextPageToken": self.next_page_token,
        }
