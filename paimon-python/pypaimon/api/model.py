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
from typing import Any, Dict, List, Optional, Union

from pypaimon.api.api_request import RESTRequest
from pypaimon.api.api_response import PagedResponse, RESTResponse
from pypaimon.catalog.model import (
    Model,
    ModelDefinition,
    model_definition_from_dict,
)
from pypaimon.common.identifier import Identifier


@dataclass
class CreateModelRequest(RESTRequest):
    name: str
    definition: Union[ModelDefinition, Dict[str, Any]]
    comment: Optional[str] = None

    def __post_init__(self):
        _validate_name_and_comment(self.name, self.comment)
        self.definition = model_definition_from_dict(self.definition)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CreateModelRequest":
        return cls(
            name=data.get("name"),
            comment=data.get("comment"),
            definition=model_definition_from_dict(
                data.get("definition"), wire=True),
        )

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "name": self.name,
            "definition": self.definition.to_wire(),
        }
        if self.comment is not None:
            result["comment"] = self.comment
        return result


@dataclass
class GetModelResponse(RESTResponse):
    name: str
    definition: Union[ModelDefinition, Dict[str, Any]]
    comment: Optional[str] = None

    def __post_init__(self):
        _validate_name_and_comment(self.name, self.comment)
        self.definition = model_definition_from_dict(self.definition)

    def to_model(self, database_name: str) -> Model:
        return Model(
            identifier=Identifier.create(database_name, self.name),
            comment=self.comment,
            definition=self.definition,
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GetModelResponse":
        return cls(
            name=data.get("name"),
            comment=data.get("comment"),
            definition=model_definition_from_dict(
                data.get("definition"), wire=True),
        )

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "name": self.name,
            "definition": self.definition.to_wire(),
        }
        if self.comment is not None:
            result["comment"] = self.comment
        return result


@dataclass
class ListModelsResponse(PagedResponse[GetModelResponse]):
    models: List[GetModelResponse] = field(default_factory=list)
    next_page_token: Optional[str] = None

    def __post_init__(self):
        if self.models is None:
            self.models = []

    def data(self) -> List[GetModelResponse]:
        return self.models

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ListModelsResponse":
        models = [
            GetModelResponse.from_dict(model)
            if isinstance(model, dict)
            else model
            for model in (data.get("models") or [])
        ]
        return cls(models=models, next_page_token=data.get("nextPageToken"))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "models": [model.to_dict() for model in self.models],
            "nextPageToken": self.next_page_token,
        }


def _validate_name_and_comment(name, comment):
    if not isinstance(name, str) or not name.strip():
        raise ValueError("name must be a non-empty string")
    if comment is not None and (
            not isinstance(comment, str) or not comment.strip()):
        raise ValueError("comment must be a non-empty string")
