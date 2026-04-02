#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from typing import Dict, List, Optional

from pypaimon.common.identifier import Identifier
from pypaimon.function.function_definition import FunctionDefinition
from pypaimon.schema.data_types import DataField


class Function:
    """Interface for a function in Paimon."""

    def name(self) -> str:
        raise NotImplementedError

    def full_name(self) -> str:
        raise NotImplementedError

    def identifier(self) -> Identifier:
        raise NotImplementedError

    def input_params(self) -> Optional[List[DataField]]:
        raise NotImplementedError

    def return_params(self) -> Optional[List[DataField]]:
        raise NotImplementedError

    def is_deterministic(self) -> bool:
        raise NotImplementedError

    def definitions(self) -> Dict[str, FunctionDefinition]:
        raise NotImplementedError

    def definition(self, dialect: str) -> Optional[FunctionDefinition]:
        raise NotImplementedError

    def comment(self) -> Optional[str]:
        raise NotImplementedError

    def options(self) -> Dict[str, str]:
        raise NotImplementedError


class FunctionImpl(Function):
    """Implementation of Function."""

    def __init__(
        self,
        identifier: Identifier,
        input_params: Optional[List[DataField]] = None,
        return_params: Optional[List[DataField]] = None,
        deterministic: bool = False,
        definitions: Optional[Dict[str, FunctionDefinition]] = None,
        comment: Optional[str] = None,
        options: Optional[Dict[str, str]] = None,
    ):
        self._identifier = identifier
        self._input_params = input_params
        self._return_params = return_params
        self._deterministic = deterministic
        self._definitions = definitions or {}
        self._comment = comment
        self._options = options or {}

    def name(self) -> str:
        return self._identifier.get_object_name()

    def full_name(self) -> str:
        return self._identifier.get_full_name()

    def identifier(self) -> Identifier:
        return self._identifier

    def input_params(self) -> Optional[List[DataField]]:
        return self._input_params

    def return_params(self) -> Optional[List[DataField]]:
        return self._return_params

    def is_deterministic(self) -> bool:
        return self._deterministic

    def definitions(self) -> Dict[str, FunctionDefinition]:
        return self._definitions

    def definition(self, dialect: str) -> Optional[FunctionDefinition]:
        return self._definitions.get(dialect)

    def comment(self) -> Optional[str]:
        return self._comment

    def options(self) -> Dict[str, str]:
        return self._options

    def __eq__(self, other):
        if not isinstance(other, FunctionImpl):
            return False
        return (self._identifier == other._identifier
                and self._input_params == other._input_params
                and self._return_params == other._return_params
                and self._deterministic == other._deterministic
                and self._definitions == other._definitions
                and self._comment == other._comment
                and self._options == other._options)

    def __hash__(self):
        return hash(self._identifier)
