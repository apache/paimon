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

"""Public vector index API with lazy Python writer loading."""

import importlib
import sys

from pypaimon.globalindex.vindex_options import VINDEX_IDENTIFIERS
from pypaimon.globalindex.vindex_reader import VindexVectorGlobalIndexReader

if sys.version_info[:2] < (3, 7):
    from pypaimon.execution.python.globalindex.vindex.vindex_vector_index_writer import (
        VindexVectorIndexWriter,
    )

__all__ = [
    'VINDEX_IDENTIFIERS',
    'VindexVectorGlobalIndexReader',
    'VindexVectorIndexWriter',
]


def __getattr__(name):
    if name != 'VindexVectorIndexWriter':
        raise AttributeError(
            "module 'pypaimon.globalindex.vindex' has no attribute {}".format(name))
    module = importlib.import_module(
        'pypaimon.execution.python.globalindex.vindex.vindex_vector_index_writer')
    value = module.VindexVectorIndexWriter
    globals()[name] = value
    return value
