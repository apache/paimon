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

from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class Types:
    FILE = "file"
    SQL = "sql"
    LAMBDA = "lambda"


class FunctionFileResource:
    """Represents a file resource for a function."""

    def __init__(self, resource_type: str, uri: str):
        self.resource_type = resource_type
        self.uri = uri

    def to_dict(self) -> Dict:
        return {
            "resourceType": self.resource_type,
            "uri": self.uri,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "FunctionFileResource":
        return cls(
            resource_type=data.get("resourceType"),
            uri=data.get("uri"),
        )

    def __eq__(self, other):
        if not isinstance(other, FunctionFileResource):
            return False
        return self.resource_type == other.resource_type and self.uri == other.uri

    def __hash__(self):
        return hash((self.resource_type, self.uri))


class FunctionDefinition(ABC):
    """Base class for function definitions."""

    @staticmethod
    def file(
        file_resources: List[FunctionFileResource],
        language: str,
        class_name: str,
        function_name: str,
    ) -> "FunctionDefinition":
        return FileFunctionDefinition(file_resources, language, class_name, function_name)

    @staticmethod
    def sql(body: str) -> "FunctionDefinition":
        return SQLFunctionDefinition(body)

    @staticmethod
    def lambda_def(definition: str, language: str) -> "FunctionDefinition":
        return LambdaFunctionDefinition(definition, language)

    @abstractmethod
    def to_dict(self) -> Dict:
        """Convert to dict for JSON serialization."""

    @staticmethod
    def from_dict(data: Dict) -> Optional["FunctionDefinition"]:
        if data is None:
            return None
        defn_type = data.get("type")
        if defn_type == Types.FILE:
            file_resources = data.get("fileResources")
            if file_resources is not None:
                file_resources = [
                    FunctionFileResource.from_dict(r) if isinstance(r, dict) else r
                    for r in file_resources
                ]
            return FileFunctionDefinition(
                file_resources=file_resources,
                language=data.get("language"),
                class_name=data.get("className"),
                function_name=data.get("functionName"),
            )
        elif defn_type == Types.SQL:
            return SQLFunctionDefinition(definition=data.get("definition"))
        elif defn_type == Types.LAMBDA:
            return LambdaFunctionDefinition(
                definition=data.get("definition"),
                language=data.get("language"),
            )
        else:
            raise ValueError(f"Unknown function definition type: {defn_type}")


class FileFunctionDefinition(FunctionDefinition):

    def __init__(
        self,
        file_resources: List[FunctionFileResource],
        language: str,
        class_name: str,
        function_name: str,
    ):
        self.file_resources = file_resources
        self.language = language
        self.class_name = class_name
        self.function_name = function_name

    def to_dict(self) -> Dict:
        return {
            "type": Types.FILE,
            "fileResources": [
                r.to_dict() if isinstance(r, FunctionFileResource) else r
                for r in self.file_resources
            ] if self.file_resources else None,
            "language": self.language,
            "className": self.class_name,
            "functionName": self.function_name,
        }

    def __eq__(self, other):
        if not isinstance(other, FileFunctionDefinition):
            return False
        return (self.file_resources == other.file_resources
                and self.language == other.language
                and self.class_name == other.class_name
                and self.function_name == other.function_name)

    def __hash__(self):
        return hash((
            tuple(self.file_resources) if self.file_resources else (),
            self.language,
            self.class_name,
            self.function_name,
        ))


class SQLFunctionDefinition(FunctionDefinition):

    def __init__(self, definition: str):
        self.definition = definition

    def to_dict(self) -> Dict:
        return {
            "type": Types.SQL,
            "definition": self.definition,
        }

    def __eq__(self, other):
        if not isinstance(other, SQLFunctionDefinition):
            return False
        return self.definition == other.definition

    def __hash__(self):
        return hash(self.definition)


class LambdaFunctionDefinition(FunctionDefinition):

    def __init__(self, definition: str, language: str):
        self.definition = definition
        self.language = language

    def to_dict(self) -> Dict:
        return {
            "type": Types.LAMBDA,
            "definition": self.definition,
            "language": self.language,
        }

    def __eq__(self, other):
        if not isinstance(other, LambdaFunctionDefinition):
            return False
        return self.definition == other.definition and self.language == other.language

    def __hash__(self):
        return hash((self.definition, self.language))
