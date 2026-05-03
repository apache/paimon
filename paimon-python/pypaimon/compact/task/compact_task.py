################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import json
from abc import ABC, abstractmethod
from typing import Any, Dict

from pypaimon.write.commit_message import CommitMessage


class CompactTask(ABC):
    """A self-contained compaction unit dispatched to a worker.

    The constructor argument list is the contract: anything captured here
    is what the worker has to rebuild its execution context.

    JSON serialization (to_dict / from_dict / serialize / deserialize) is
    declared on the base class so distributed executors have a single hook
    to call, but concrete subclasses are free to defer the implementation
    until distributed execution actually arrives — Phase 4 will fill in the
    AppendCompactTask serialization once Ray is the executor. Until then
    those methods may raise NotImplementedError; LocalExecutor never
    serializes a task and is unaffected.
    """

    TYPE: str = ""

    @abstractmethod
    def run(self) -> CommitMessage:
        """Execute the compaction unit on the local process and return a CommitMessage.

        The CommitMessage carries compact_before / compact_after files for the
        driver to assemble into a single atomic commit.
        """

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-friendly payload identifying everything the worker needs."""

    def serialize(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":")).encode("utf-8")

    @classmethod
    def deserialize(cls, payload: bytes) -> "CompactTask":
        data = json.loads(payload.decode("utf-8"))
        task_type = data.get("type")
        impl = _TASK_REGISTRY.get(task_type)
        if impl is None:
            raise ValueError(f"Unknown CompactTask type: {task_type}")
        return impl.from_dict(data)

    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompactTask":
        """Rebuild a task from its to_dict() payload."""


_TASK_REGISTRY: Dict[str, type] = {}


def register_compact_task(impl: type) -> type:
    """Decorator to register a CompactTask subclass under its TYPE string.

    The registry powers CompactTask.deserialize() so the executor can route
    payloads back to the correct subclass without a hard import.
    """
    if not issubclass(impl, CompactTask):
        raise TypeError(f"{impl} is not a CompactTask subclass")
    if not impl.TYPE:
        raise ValueError(f"{impl} must define a non-empty TYPE")
    if impl.TYPE in _TASK_REGISTRY and _TASK_REGISTRY[impl.TYPE] is not impl:
        raise ValueError(f"CompactTask TYPE {impl.TYPE!r} already registered")
    _TASK_REGISTRY[impl.TYPE] = impl
    return impl
