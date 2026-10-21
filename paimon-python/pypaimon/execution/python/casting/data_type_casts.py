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

"""Probe executable schema-evolution casts using the Python read executor."""

import pyarrow as pa

from pypaimon.schema.data_types import PyarrowFieldParser


# Caches the read-time cast probe per (source, target) pyarrow type so the
# alter-time check stays cheap. Keyed by the pyarrow type strings.
_EXECUTABLE_CAST_CACHE = {}


def schema_evolution_cast_supported(source_type, target_type) -> bool:
    from pypaimon.execution.python.casting.schema_evolution import cast_array_for_schema_evolution

    source_pa = PyarrowFieldParser.from_paimon_type(source_type)
    target_pa = PyarrowFieldParser.from_paimon_type(target_type)
    if source_pa == target_pa:
        return True
    cache_key = (str(source_pa), str(target_pa))
    cached = _EXECUTABLE_CAST_CACHE.get(cache_key)
    if cached is not None:
        return cached
    # Probe a one-row (null-valued) array rather than an empty one. Some Arrow
    # kernels validate target parameters only for a non-empty input. Using the
    # read executor also includes Java's per-value overflow-to-NULL rules, such
    # as integer-to-DECIMAL targets narrower than the source type's full range.
    try:
        cast_array_for_schema_evolution(
            pa.nulls(1, type=source_pa), target_pa, source_type, target_type)
        ok = True
    except (pa.lib.ArrowNotImplementedError, pa.lib.ArrowInvalid,
            pa.lib.ArrowTypeError, ValueError, NotImplementedError):
        ok = False
    _EXECUTABLE_CAST_CACHE[cache_key] = ok
    return ok
