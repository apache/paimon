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

from pypaimon.common.identifier import Identifier


@dataclass
class ModelField:
    """Named input or output field of a model definition."""

    name: str
    data_type: str
    modality: Optional[str] = None

    def __post_init__(self):
        _nonblank(self.name, "name")
        _nonblank(self.data_type, "data_type")
        if self.modality is not None:
            _nonblank(self.modality, "modality")

    def to_dict(self) -> Dict[str, Any]:
        value = {"name": self.name, "data_type": self.data_type}
        if self.modality is not None:
            value["modality"] = self.modality
        return value

    def to_wire(self) -> Dict[str, Any]:
        value = {"name": self.name, "dataType": self.data_type}
        if self.modality is not None:
            value["modality"] = self.modality
        return value


class ModelDefinition:
    """Base class for extensible model definitions."""

    @property
    def type(self) -> str:
        raise NotImplementedError

    def to_dict(self) -> Dict[str, Any]:
        raise NotImplementedError

    def to_wire(self) -> Dict[str, Any]:
        raise NotImplementedError


@dataclass
class ProviderModelDefinition(ModelDefinition):
    """Definition of a model served by an external provider."""

    provider: str
    provider_model: str
    inputs: List[ModelField]
    outputs: Optional[List[ModelField]] = None
    options: Dict[str, str] = field(default_factory=dict)

    TYPE = "provider"

    def __post_init__(self):
        _nonblank(self.provider, "provider")
        _nonblank(self.provider_model, "provider_model")
        self.inputs = _copy_fields(self.inputs, "inputs", True)
        self.outputs = _copy_fields(self.outputs, "outputs", False)
        if not isinstance(self.options, dict):
            raise ValueError("options must be a mapping")
        copied_options = {}
        for name, value in self.options.items():
            _nonblank(name, "option name")
            if not isinstance(value, str):
                raise ValueError("option values must be strings")
            copied_options[name] = value
        self.options = copied_options

    @property
    def type(self) -> str:
        return self.TYPE

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "type": self.type,
            "provider": self.provider,
            "provider_model": self.provider_model,
            "inputs": [value.to_dict() for value in self.inputs],
            "options": dict(self.options),
        }
        if self.outputs is not None:
            result["outputs"] = [value.to_dict() for value in self.outputs]
        return result

    def to_wire(self) -> Dict[str, Any]:
        result = {
            "type": self.type,
            "provider": self.provider,
            "providerModel": self.provider_model,
            "inputs": [value.to_wire() for value in self.inputs],
            "options": dict(self.options),
        }
        if self.outputs is not None:
            result["outputs"] = [value.to_wire() for value in self.outputs]
        return result


@dataclass
class Model:
    """Metadata for a model registered in a catalog database."""

    identifier: Union[str, Identifier]
    definition: Union[ModelDefinition, Dict[str, Any]]
    comment: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.identifier, str):
            self.identifier = Identifier.from_string(self.identifier)
        if not isinstance(self.identifier, Identifier):
            raise ValueError(
                "identifier must be an Identifier or database-qualified name")
        if self.comment is not None:
            _nonblank(self.comment, "comment")
        self.definition = model_definition_from_dict(self.definition)

    @property
    def name(self) -> str:
        return self.identifier.get_object_name()

    @property
    def full_name(self) -> str:
        return self.identifier.get_full_name()

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "identifier": self.full_name,
            "definition": self.definition.to_dict(),
        }
        if self.comment is not None:
            result["comment"] = self.comment
        return result


def model_definition_from_dict(
        value: Union[ModelDefinition, Dict[str, Any]],
        wire: bool = False,
) -> ModelDefinition:
    if isinstance(value, ModelDefinition):
        return value
    if not isinstance(value, dict):
        raise ValueError("definition must be a ModelDefinition or mapping")
    definition_type = value.get("type")
    if definition_type != ProviderModelDefinition.TYPE:
        raise ValueError("unsupported model definition type: {}".format(
            definition_type))
    provider_model_key = "providerModel" if wire else "provider_model"
    return ProviderModelDefinition(
        provider=value.get("provider"),
        provider_model=value.get(provider_model_key),
        inputs=[
            _field_from_dict(field_value, wire)
            for field_value in (value.get("inputs") or [])
        ],
        outputs=(
            None if value.get("outputs") is None else [
                _field_from_dict(field_value, wire)
                for field_value in value.get("outputs")
            ]
        ),
        options=value.get("options") or {},
    )


def _field_from_dict(value: Any, wire: bool) -> ModelField:
    if isinstance(value, ModelField):
        return value
    if not isinstance(value, dict):
        raise ValueError(
            "model fields must be ModelField instances or mappings")
    data_type_key = "dataType" if wire else "data_type"
    return ModelField(
        name=value.get("name"),
        data_type=value.get(data_type_key),
        modality=value.get("modality"),
    )


def _copy_fields(values, name, required):
    if values is None:
        if required:
            raise ValueError("{} must be a non-empty list".format(name))
        return None
    if not isinstance(values, list) or (required and not values):
        raise ValueError("{} must be a non-empty list".format(name))
    result = []
    names = set()
    for value in values:
        field_value = _field_from_dict(value, False)
        if field_value.name in names:
            raise ValueError(
                "{} contains duplicate field '{}'".format(
                    name, field_value.name))
        names.add(field_value.name)
        result.append(ModelField(**field_value.to_dict()))
    return result


def _nonblank(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("{} must be a non-empty string".format(name))
