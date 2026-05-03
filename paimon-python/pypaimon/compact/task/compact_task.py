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
from typing import Any, Dict, Optional

from pypaimon.write.commit_message import CommitMessage


class CompactTask(ABC):
    """A self-contained compaction unit dispatched to a worker.

    Two operating modes:
    - In-process (LocalExecutor): the driver attaches the FileStoreTable
      via with_table(); the worker reuses it directly.
    - Distributed (RayExecutor): the driver attaches a table loader spec
      via with_table_loader(catalog_options, table_identifier); to_dict
      ships those + the data payload across, and from_dict on the worker
      rebuilds the table from the catalog.

    Subclasses implement _to_payload / _from_payload to add their own
    fields on top of this base envelope (catalog_loader + table_identifier
    are handled here once).
    """

    TYPE: str = ""

    # Distributed-execution loader spec, populated by CompactJob when an
    # executor that can't share the in-process table is in use.
    _catalog_loader_options: Optional[Dict[str, str]] = None
    _table_identifier: Optional[str] = None

    @abstractmethod
    def run(self) -> CommitMessage:
        """Execute the compaction unit and return a CommitMessage.

        Subclasses should obtain their FileStoreTable via self._resolve_table()
        rather than poking at the cached _table directly, so distributed and
        local paths share the same retrieval logic.
        """

    def with_table_loader(
        self,
        catalog_options: Dict[str, str],
        table_identifier: str,
    ) -> "CompactTask":
        """Attach the spec a distributed worker uses to rebuild this task's table."""
        self._catalog_loader_options = dict(catalog_options)
        self._table_identifier = table_identifier
        return self

    def _resolve_table_via_loader(self):
        if not self._catalog_loader_options or not self._table_identifier:
            raise RuntimeError(
                f"{type(self).__name__} has no in-process table and no catalog loader; "
                "the driver must call with_table() or with_table_loader() before "
                "handing this task to an executor."
            )
        # Lazy import keeps base task module decoupled from catalog code.
        from pypaimon.catalog.catalog_factory import CatalogFactory
        catalog = CatalogFactory.create(dict(self._catalog_loader_options))
        return catalog.get_table(self._table_identifier)

    def to_dict(self) -> Dict[str, Any]:
        """Standard envelope; subclasses override _to_payload to add fields."""
        return {
            "type": self.TYPE,
            "catalog_options": self._catalog_loader_options,
            "table_identifier": self._table_identifier,
            "payload": self._to_payload(),
        }

    @abstractmethod
    def _to_payload(self) -> Dict[str, Any]:
        """Subclass-specific data (partition / bucket / files / ...)."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompactTask":
        task_type = data.get("type")
        impl = _TASK_REGISTRY.get(task_type)
        if impl is None:
            raise ValueError(f"Unknown CompactTask type: {task_type}")
        task = impl._from_payload(data.get("payload") or {})
        loader_opts = data.get("catalog_options")
        identifier = data.get("table_identifier")
        if loader_opts and identifier:
            task.with_table_loader(loader_opts, identifier)
        return task

    @classmethod
    @abstractmethod
    def _from_payload(cls, payload: Dict[str, Any]) -> "CompactTask":
        """Construct a task from the subclass-specific payload only."""

    def serialize(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":")).encode("utf-8")

    @classmethod
    def deserialize(cls, payload: bytes) -> "CompactTask":
        data = json.loads(payload.decode("utf-8"))
        return cls.from_dict(data)


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
