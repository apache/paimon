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

"""Utilities for data-evolution tables."""

from typing import Callable, Iterable, TypeVar

from pypaimon.manifest.schema.data_file_meta import DataFileMeta

T = TypeVar("T")


def retrieve_anchor_file(
    entries: Iterable[T],
    file_meta_func: Callable[[T], DataFileMeta] = lambda entry: entry,
) -> T:
    """Return the oldest normal file in a data-evolution row-range group."""
    anchor = None
    anchor_key = None

    for entry in entries:
        meta = file_meta_func(entry)
        if DataFileMeta.is_blob_file(meta.file_name) or DataFileMeta.is_vector_file(meta.file_name):
            continue

        key = (meta.max_sequence_number, meta.file_name)
        if anchor_key is None or key < anchor_key:
            anchor = entry
            anchor_key = key

    if anchor is None:
        raise ValueError(
            "Data-evolution deletion vectors should have a normal anchor file "
            "in each row range group."
        )

    return anchor
