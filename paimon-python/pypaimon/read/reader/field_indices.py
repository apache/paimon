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

"""Helpers for carrying typed field metadata through projection wrappers."""

from typing import Any, Iterable, List, Optional, Sequence, Set

from pypaimon.schema.data_types import DataField, VectorType


def blob_field_indices(fields: List[DataField]) -> Set[int]:
    return {
        i for i, f in enumerate(fields)
        if hasattr(f.type, 'type') and f.type.type == 'BLOB'
    }


def descriptor_field_indices(
        fields: List[DataField], descriptor_field_names: Iterable[str]) -> Set[int]:
    names = set(descriptor_field_names)
    if not names:
        return set()
    return {i for i, f in enumerate(fields) if f.name in names}


def descriptor_field_names_for_table(table) -> Set[str]:
    from pypaimon.common.options.core_options import CoreOptions

    names = set(CoreOptions.blob_descriptor_fields(table.options))
    if CoreOptions.blob_as_descriptor(table.options):
        names |= CoreOptions.blob_view_fields(table.options)
    return names


def descriptor_field_indices_for_table(table, fields: List[DataField]) -> Set[int]:
    return descriptor_field_indices(
        fields, descriptor_field_names_for_table(table))


def vector_field_indices(fields: List[DataField]) -> Set[int]:
    return {i for i, f in enumerate(fields) if isinstance(f.type, VectorType)}


def project_top_level_field_indices(
    source_indices: Optional[Iterable[int]],
    path_specs: Sequence[Any],
) -> Optional[Set[int]]:
    """Map top-level field indices through path specs with top_idx/sub_names."""
    if source_indices is None:
        return None
    source = set(source_indices)
    return {
        proj_pos
        for proj_pos, spec in enumerate(path_specs)
        if not spec.sub_names and spec.top_idx in source
    }
