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

"""Lazy discovery and process safety for the optional Native runtime."""

from importlib import import_module


def _raise_if_native_fork_safety_error(error):
    """Propagate native fork errors without probing or initializing the SDK."""
    from pypaimon.execution.dispatch import propagate_fork_safety_error
    propagate_fork_safety_error(error)


def native_runtime_available() -> bool:
    """Whether the optional companion Rust extension is installed.

    Native paths require this release's APIs; they do not probe older builds.
    """
    try:
        import_module('pypaimon_rust.datafusion')
    except ImportError:
        return False
    return True
