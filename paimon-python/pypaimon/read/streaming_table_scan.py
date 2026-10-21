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

"""Public streaming scan classes backed by Python or Native execution."""

import importlib
import sys

if sys.version_info[:2] < (3, 7):
    # Python 3.6 does not resolve module attributes through __getattr__.
    from pypaimon.execution.python.read.streaming_table_scan import AsyncStreamingTableScan
    from pypaimon.execution.native.read.streaming_table_scan import StreamTableScan

__all__ = ['AsyncStreamingTableScan', 'StreamTableScan']

_MODULE_BY_EXPORT = {
    'AsyncStreamingTableScan': 'pypaimon.execution.python.read.streaming_table_scan',
    'StreamTableScan': 'pypaimon.execution.native.read.streaming_table_scan',
}


def __getattr__(name):
    module_name = _MODULE_BY_EXPORT.get(name)
    if module_name is None:
        raise AttributeError(
            "module 'pypaimon.read.streaming_table_scan' has no attribute {!r}".format(name))
    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
