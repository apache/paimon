"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from abc import ABC
from typing import Dict, List, Set, Tuple


class PropertyChange(ABC):
    @staticmethod
    def set_property(property: str, value: str) -> "PropertyChange":
        return SetProperty(property, value)

    @staticmethod
    def remove_property(property: str) -> "PropertyChange":
        return RemoveProperty(property)

    @staticmethod
    def get_set_properties_to_remove_keys(changes: List["PropertyChange"]) -> Tuple[Dict[str, str], Set[str]]:
        set_properties: Dict[str, str] = {}
        remove_keys: Set[str] = set()

        for change in changes:
            if isinstance(change, SetProperty):
                set_properties[change.property] = change.value
            elif isinstance(change, RemoveProperty):
                remove_keys.add(change.property)

        return set_properties, remove_keys


class SetProperty(PropertyChange):
    def __init__(self, property: str, value: str):
        self.property = property
        self.value = value


class RemoveProperty(PropertyChange):
    def __init__(self, property: str):
        self.property = property
