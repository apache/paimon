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
    from .config_option import ConfigOption, Description
    from .config_options import ConfigOptions
    from .options import Options
    from .core_options import CoreOptions

__all__ = [
    'ConfigOption',
    'Description',
    'ConfigOptions',
    'Options',
    'CoreOptions'
]

_MODULE_BY_EXPORT = {
    'ConfigOption': 'pypaimon.common.options.config_option',
    'Description': 'pypaimon.common.options.config_option',
    'ConfigOptions': 'pypaimon.common.options.config_options',
    'Options': 'pypaimon.common.options.options',
    'CoreOptions': 'pypaimon.common.options.core_options',
}


# This package is imported by name from many modules that its own eager
# exports pull in, so eager imports here deadlock concurrent importers.
def __getattr__(name):
    module_name = _MODULE_BY_EXPORT.get(name)
    if module_name is None:
        raise AttributeError(
            "module 'pypaimon.common.options' has no attribute {}".format(
                name))
    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value
    return value
