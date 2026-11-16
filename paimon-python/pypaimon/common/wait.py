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

import time
from datetime import timedelta
from typing import Callable, Generic, Optional, TypeVar


T = TypeVar("T")


class Wait(Generic[T]):
    """Client-side waiter for a long-running operation."""

    def __init__(
            self,
            initial: T,
            poll: Callable[[], T],
            is_terminal: Callable[[T], bool],
            validate_result: Callable[[T], T],
            description: str = "Operation",
            default_timeout: timedelta = timedelta(minutes=20),
            initial_poll_interval: float = 1.0,
            max_poll_interval: float = 10.0,
    ):
        if default_timeout.total_seconds() <= 0:
            raise ValueError("default_timeout must be positive")
        if initial_poll_interval <= 0:
            raise ValueError("initial_poll_interval must be positive")
        if max_poll_interval < initial_poll_interval:
            raise ValueError(
                "max_poll_interval must not be less than "
                "initial_poll_interval")
        self._current = initial
        self._poll = poll
        self._is_terminal = is_terminal
        self._validate_result = validate_result
        self._description = description
        self._default_timeout = default_timeout
        self._initial_poll_interval = initial_poll_interval
        self._max_poll_interval = max_poll_interval

    @property
    def current(self) -> T:
        """Return the latest value observed by this waiter."""
        return self._current

    def result(
            self,
            timeout: Optional[timedelta] = None,
            callback: Optional[Callable[[T], None]] = None,
    ) -> T:
        """Wait for completion and return the final value.

        The waiter polls through the supplied operation-specific callback. A
        timeout only stops this client-side wait; it does not cancel the
        server-side operation. Durable workflows should observe long-running
        operations asynchronously instead of keeping a client blocked here.
        """
        if timeout is None:
            timeout = self._default_timeout
        if not isinstance(timeout, timedelta):
            raise ValueError("timeout must be a datetime.timedelta")
        if timeout.total_seconds() <= 0:
            raise ValueError("timeout must be positive")

        deadline = time.monotonic() + timeout.total_seconds()
        poll_interval = self._initial_poll_interval
        while not self._is_terminal(self._current):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "{} did not finish within {}".format(
                        self._description, timeout))
            time.sleep(min(poll_interval, remaining))
            self._current = self._poll()
            if callback is not None:
                callback(self._current)
            poll_interval = min(
                poll_interval * 2, self._max_poll_interval)

        return self._validate_result(self._current)
