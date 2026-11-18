# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import timedelta
from typing import Callable, Generic, TypeVar


T = TypeVar("T")


class Wait(Generic[T]):
    """Handle for a long-running operation."""

    def __init__(self, waiter: Callable, response=None, **kwargs):
        self.response = response
        self._waiter = waiter
        self._bind = kwargs

    def __getattr__(self, name):
        try:
            return self._bind[name]
        except KeyError:
            raise AttributeError(name) from None

    def bind(self):
        return dict(self._bind)

    def result(
            self,
            timeout: timedelta = timedelta(minutes=20),
            callback: Callable[[T], None] = None,
    ) -> T:
        """Delegate waiting and return the final value.

        A timeout only stops this client-side wait; it does not cancel the
        server-side operation.
        """
        kwargs = self._bind.copy()
        return self._waiter(callback=callback, timeout=timeout, **kwargs)
