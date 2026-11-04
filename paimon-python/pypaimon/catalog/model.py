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
from typing import Any, Dict, Optional, Union

from pypaimon.common.identifier import Identifier


ModelFields = Dict[str, Dict[str, Any]]


@dataclass
class Model:
    """Metadata for a model registered in a catalog database."""

    identifier: Union[str, Identifier]
    provider: str
    model: str
    version: int
    inputs: ModelFields
    outputs: Optional[ModelFields] = None
    properties: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.identifier, str):
            self.identifier = Identifier.from_string(self.identifier)
        if not isinstance(self.identifier, Identifier):
            raise ValueError("identifier must be an Identifier or database-qualified name")
        _nonblank(self.provider, "provider")
        _nonblank(self.model, "model")
        if type(self.version) is not int or self.version <= 0:
            raise ValueError("version must be a positive integer")
        self.inputs = _validate_model_fields(self.inputs, "inputs", True)
        self.outputs = _validate_model_fields(self.outputs, "outputs", False)
        if not isinstance(self.properties, dict):
            raise ValueError("properties must be a mapping")
        for key in self.properties:
            _nonblank(key, "property name")
        self.properties = dict(self.properties)

    @property
    def name(self) -> str:
        return self.identifier.get_object_name()

    @property
    def full_name(self) -> str:
        return self.identifier.get_full_name()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "identifier": self.full_name,
            "provider": self.provider,
            "model": self.model,
            "version": self.version,
            "inputs": _copy_model_fields(self.inputs),
            "outputs": _copy_model_fields(self.outputs),
            "properties": dict(self.properties),
        }


def _validate_model_fields(
        fields: Optional[ModelFields], name: str, required: bool
) -> Optional[ModelFields]:
    if fields is None:
        if required:
            raise ValueError("{} must be a non-empty mapping".format(name))
        return None
    if not isinstance(fields, dict) or (required and not fields):
        raise ValueError("{} must be a non-empty mapping".format(name))

    result = {}
    for field_name, spec in fields.items():
        _nonblank(field_name, "{} field name".format(name))
        if not isinstance(spec, dict):
            raise ValueError("{} field '{}' must be a mapping".format(name, field_name))
        data_type = spec.get("data_type")
        _nonblank(data_type, "{}.{}.data_type".format(name, field_name))
        if "modality" in spec:
            _nonblank(spec["modality"], "{}.{}.modality".format(name, field_name))
        for key in spec:
            _nonblank(key, "{} field property".format(name))
        result[field_name] = dict(spec)
    return result


def _copy_model_fields(fields: Optional[ModelFields]) -> Optional[ModelFields]:
    if fields is None:
        return None
    return {name: dict(spec) for name, spec in fields.items()}


def model_fields_to_wire(fields: Optional[ModelFields]) -> Optional[ModelFields]:
    if fields is None:
        return None
    result = {}
    for name, spec in fields.items():
        wire_spec = dict(spec)
        wire_spec["dataType"] = wire_spec.pop("data_type")
        result[name] = wire_spec
    return result


def model_fields_from_wire(fields: Optional[ModelFields]) -> Optional[ModelFields]:
    if fields is None:
        return None
    result = {}
    for name, spec in fields.items():
        python_spec = dict(spec)
        if "dataType" in python_spec:
            python_spec["data_type"] = python_spec.pop("dataType")
        result[name] = python_spec
    return result


def _nonblank(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("{} must be a non-empty string".format(name))
