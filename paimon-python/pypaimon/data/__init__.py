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

if sys.version_info[:2] < (3, 7):
    # Module-level __getattr__ is unavailable before Python 3.7.
    from pypaimon.data.timestamp import Timestamp
    from pypaimon.data.decimal import Decimal
    from pypaimon.data.variant_path import variant_get, variant_replace

__all__ = [
    'Timestamp',
    'Decimal',
    'variant_get',
    'variant_replace',
]

_MODULE_BY_EXPORT = {
    'Timestamp': 'pypaimon.data.timestamp',
    'Decimal': 'pypaimon.data.decimal',
    'variant_get': 'pypaimon.data.variant_path',
    'variant_replace': 'pypaimon.data.variant_path',
}


# Eager sibling imports here let one thread hold this package's module lock
# while waiting on a sibling that another thread is already importing.
def __getattr__(name):
    module_name = _MODULE_BY_EXPORT.get(name)
    if module_name is None:
        raise AttributeError(
            "module 'pypaimon.data' has no attribute {}".format(name))
    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value
    return value
