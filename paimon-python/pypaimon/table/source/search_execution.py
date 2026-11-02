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

"""Transport and scheduling contracts for backend-owned search tasks."""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Tuple


@dataclass(frozen=True, eq=False)
class SearchTask:
    """A serializable operation with shared context and optional prerequisites.

    Dependencies use object identity within a map call. Executors resolve them
    before scheduling their dependents, without occupying a dependent worker.
    Context must contain transportable configuration, not an open reader.
    """

    operation: Callable
    context: Any
    item: Any
    dependencies: Tuple['SearchTask', ...] = ()

    def execute(self, *dependency_results):
        return self.operation(self.context, self.item, *dependency_results)


class SearchTaskExecutor(ABC):
    """Schedule search tasks while the selected backend coordinates the query."""

    @abstractmethod
    def map(self, tasks, ordered=False):
        """Return a closable iterator of ``(input_ordinal, result)`` pairs.

        ``ordered`` requires input order, including duplicate-row precedence.
        Closing the iterator or failing an operation cancels its pending work.
        Exceptions propagate without replaying through another backend.
        """
        pass

    @property
    def concurrency(self):
        """Number of concurrent tasks available to a query."""
        return 1

    def with_concurrency(self, concurrency):
        """Create a query view sharing resources and the active parent cancellation.

        Implementations with cancellation state isolate future scopes on the
        returned view, so reusing an executor does not cancel unrelated queries.
        """
        return self

    @contextmanager
    def cancellation_scope(self, event):
        """Observe a query's cooperative cancellation event when supported."""
        yield
