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

"""Blob payload selection policy shared by read preparation and execution."""

from typing import List, Optional

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.predicate import Predicate
from pypaimon.read.push_down_utils import predicate_field_names
from pypaimon.read.reader.field_indices import blob_field_indices
from pypaimon.schema.data_types import DataField


def deferred_blob_field_names(table, read_fields: List[DataField],
                              predicate: Optional[Predicate],
                              limit: Optional[int],
                              has_post_filter: bool = False) -> set:
    # Only append tables have dedicated Blob files to defer. Primary-key
    # managed packs are resolved inside Rust's read pipeline.
    if table.is_primary_key_table:
        return set()
    # An auth filter also selects rows; defer past it too, like a predicate/limit.
    if ((predicate is None and limit is None and not has_post_filter)
            or CoreOptions.blob_as_descriptor(table.options)):
        return set()

    non_payload_fields = (
        CoreOptions.blob_descriptor_fields(table.options)
        | CoreOptions.blob_view_fields(table.options)
        | CoreOptions.video_frame_fields(table.options)
    )
    predicate_fields = (
        predicate_field_names(predicate) if predicate is not None else set()
    )
    return {
        read_fields[index].name
        for index in blob_field_indices(read_fields)
        if read_fields[index].name not in non_payload_fields
        and read_fields[index].name not in predicate_fields
    }
