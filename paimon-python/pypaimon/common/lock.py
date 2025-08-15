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

from abc import ABC, abstractmethod
from typing import Callable, TypeVar

T = TypeVar('T')


class Lock(ABC):
    """Interface for distributed locks."""

    @abstractmethod
    def run_with_lock(self, callable_func: Callable[[], T]) -> T:
        """
        Run a callable function with lock protection.

        Args:
            callable_func: The function to run with lock

        Returns:
            The result of the callable function

        Raises:
            Exception: If lock acquisition fails or callable raises exception
        """
        pass

    @abstractmethod
    def close(self):
        """Close the lock and release any resources."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class EmptyLock(Lock):
    """A no-op lock implementation that doesn't provide any synchronization."""

    def run_with_lock(self, callable_func: Callable[[], T]) -> T:
        """Run the callable without any locking."""
        return callable_func()

    def close(self):
        """No-op close."""
        pass

    @staticmethod
    def empty() -> 'EmptyLock':
        """Factory method to create an empty lock."""
        return EmptyLock()
