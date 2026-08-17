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

import importlib
import sys
import threading

if sys.version_info[:2] < (3, 7):
    # Module-level __getattr__ is unavailable before Python 3.7.
    from pypaimon.tag.tag import Tag
    from pypaimon.tag.tag_manager import TagManager

__all__ = ["Tag", "TagManager"]

_MODULE_BY_EXPORT = {
    "Tag": "pypaimon.tag.tag",
    "TagManager": "pypaimon.tag.tag_manager",
}

# Eager sibling imports let threads lock tag and tag_manager in opposite
# orders and fail with _DeadlockError. The lock serializes racing imports.
_LAZY_IMPORT_LOCK = threading.RLock()


def __getattr__(name):
    module_name = _MODULE_BY_EXPORT.get(name)
    if module_name is None:
        raise AttributeError(
            "module 'pypaimon.tag' has no attribute {}".format(name))
    with _LAZY_IMPORT_LOCK:
        if name not in globals():
            module = importlib.import_module(module_name)
            globals()[name] = getattr(module, name)
        return globals()[name]
