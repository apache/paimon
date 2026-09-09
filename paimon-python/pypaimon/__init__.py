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

if sys.version_info[:2] < (3, 8):
    try:
        from pypaimon.manifest import fastavro_py36_compat  # noqa: F401
    except ImportError:
        pass

if sys.version_info[:2] < (3, 7):
    # Module-level __getattr__ is unavailable before Python 3.7.
    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.filesystem.pvfs import PaimonVirtualFileSystem
    from pypaimon.schema.schema import Schema
    from pypaimon.tag.tag import Tag
    from pypaimon.tag.tag_manager import TagManager

__all__ = [
    "PaimonVirtualFileSystem",
    "CatalogFactory",
    "Schema",
    "Tag",
    "TagManager",
    "SQLContext",
]

_LAZY_EXPORTS = {
    "CatalogFactory": ("pypaimon.catalog.catalog_factory", "CatalogFactory"),
    "PaimonVirtualFileSystem": (
        "pypaimon.filesystem.pvfs", "PaimonVirtualFileSystem",
    ),
    "Schema": ("pypaimon.schema.schema", "Schema"),
    "Tag": ("pypaimon.tag.tag", "Tag"),
    "TagManager": ("pypaimon.tag.tag_manager", "TagManager"),
    "SQLContext": ("pypaimon_rust.datafusion", "SQLContext"),
}


# Resolution stays unlocked: submodules import these names at their top
# level, so a lock here would deadlock against Python's module locks.
def __getattr__(name):
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(
            "module 'pypaimon' has no attribute {}".format(name))
    module = importlib.import_module(target[0])
    value = getattr(module, target[1])
    globals()[name] = value
    return value
