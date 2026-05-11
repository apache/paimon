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

from typing import Dict, Optional

from pypaimon.function.function_definition import FunctionDefinition


class Actions:
    SET_OPTION = "setOption"
    REMOVE_OPTION = "removeOption"
    UPDATE_COMMENT = "updateComment"
    ADD_DEFINITION = "addDefinition"
    UPDATE_DEFINITION = "updateDefinition"
    DROP_DEFINITION = "dropDefinition"


class FunctionChange:
    """Represents a change to a function."""

    def __init__(self, action: str):
        self._action = action

    @staticmethod
    def set_option(key: str, value: str) -> "SetFunctionOption":
        return SetFunctionOption(key, value)

    @staticmethod
    def remove_option(key: str) -> "RemoveFunctionOption":
        return RemoveFunctionOption(key)

    @staticmethod
    def update_comment(comment: Optional[str]) -> "UpdateFunctionComment":
        return UpdateFunctionComment(comment)

    @staticmethod
    def add_definition(name: str, definition: FunctionDefinition) -> "AddDefinition":
        return AddDefinition(name, definition)

    @staticmethod
    def update_definition(name: str, definition: FunctionDefinition) -> "UpdateDefinition":
        return UpdateDefinition(name, definition)

    @staticmethod
    def drop_definition(name: str) -> "DropDefinition":
        return DropDefinition(name)

    def to_dict(self) -> Dict:
        raise NotImplementedError

    @classmethod
    def from_dict(cls, data: Dict) -> "FunctionChange":
        action = data.get("action")
        if action == Actions.SET_OPTION:
            return SetFunctionOption(data["key"], data["value"])
        elif action == Actions.REMOVE_OPTION:
            return RemoveFunctionOption(data["key"])
        elif action == Actions.UPDATE_COMMENT:
            return UpdateFunctionComment(data.get("comment"))
        elif action == Actions.ADD_DEFINITION:
            return AddDefinition(data["name"], FunctionDefinition.from_dict(data["definition"]))
        elif action == Actions.UPDATE_DEFINITION:
            return UpdateDefinition(data["name"], FunctionDefinition.from_dict(data["definition"]))
        elif action == Actions.DROP_DEFINITION:
            return DropDefinition(data["name"])
        else:
            raise ValueError(f"Unknown function change action: {action}")


class SetFunctionOption(FunctionChange):
    def __init__(self, key: str, value: str):
        super().__init__(Actions.SET_OPTION)
        self.key = key
        self.value = value

    def to_dict(self) -> Dict:
        return {"action": self._action, "key": self.key, "value": self.value}


class RemoveFunctionOption(FunctionChange):
    def __init__(self, key: str):
        super().__init__(Actions.REMOVE_OPTION)
        self.key = key

    def to_dict(self) -> Dict:
        return {"action": self._action, "key": self.key}


class UpdateFunctionComment(FunctionChange):
    def __init__(self, comment: Optional[str]):
        super().__init__(Actions.UPDATE_COMMENT)
        self.comment = comment

    def to_dict(self) -> Dict:
        return {"action": self._action, "comment": self.comment}


class AddDefinition(FunctionChange):
    def __init__(self, name: str, definition: FunctionDefinition):
        super().__init__(Actions.ADD_DEFINITION)
        self.name = name
        self.definition = definition

    def to_dict(self) -> Dict:
        return {
            "action": self._action,
            "name": self.name,
            "definition": self.definition.to_dict(),
        }


class UpdateDefinition(FunctionChange):
    def __init__(self, name: str, definition: FunctionDefinition):
        super().__init__(Actions.UPDATE_DEFINITION)
        self.name = name
        self.definition = definition

    def to_dict(self) -> Dict:
        return {
            "action": self._action,
            "name": self.name,
            "definition": self.definition.to_dict(),
        }


class DropDefinition(FunctionChange):
    def __init__(self, name: str):
        super().__init__(Actions.DROP_DEFINITION)
        self.name = name

    def to_dict(self) -> Dict:
        return {"action": self._action, "name": self.name}
