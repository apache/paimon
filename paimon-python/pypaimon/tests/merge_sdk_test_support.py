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

"""Typed companion SDK doubles for transport tests that do not load Rust."""

from dataclasses import dataclass
from types import ModuleType


@dataclass(frozen=True)
class MergeAssignment:
    kind: str
    value: object

    @classmethod
    def source_column(cls, name):
        return cls('source', name)

    @classmethod
    def target_column(cls, name):
        return cls('target', name)

    @classmethod
    def literal(cls, value):
        return cls('literal', value)

    @classmethod
    def function(cls, function):
        return cls('function', function)


@dataclass(frozen=True)
class WhenMatched:
    assignments: dict
    condition: object
    action: str

    @classmethod
    def update(cls, assignments, condition=None):
        assert all(isinstance(value, MergeAssignment) for value in assignments.values())
        return cls(dict(assignments), condition, 'update')

    @classmethod
    def delete(cls, condition=None):
        return cls({}, condition, 'delete')


@dataclass(frozen=True)
class WhenNotMatched:
    assignments: dict
    condition: object = None

    def __post_init__(self):
        assert all(isinstance(value, MergeAssignment) for value in self.assignments.values())


def merge_sdk_module():
    module = ModuleType('pypaimon_rust.datafusion')
    module.MergeAssignment = MergeAssignment
    module.WhenMatched = WhenMatched
    module.WhenNotMatched = WhenNotMatched
    return module
