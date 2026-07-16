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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dataclasses import dataclass

from pypaimon.index.pk.primary_key_index_source_meta import PrimaryKeyIndexSourceMeta


@dataclass(frozen=True)
class PkSortedIndexGroup:
    data_level: int
    source_files: tuple
    payloads: tuple

    @staticmethod
    def create(field_id, index_type, source_files, payloads):
        if not source_files or len(payloads) != 1:
            return None
        if len({source.file_name for source in source_files}) != len(source_files):
            return None
        source_row_count = sum(source.row_count for source in source_files)
        payload = payloads[0]
        meta = payload.global_index_meta
        try:
            source_meta = PrimaryKeyIndexSourceMeta.from_index_file(payload)
        except ValueError:
            return None
        if (tuple(source_files) != tuple(source_meta.source_files)
                or payload.index_type != index_type
                or meta is None
                or meta.index_field_id != field_id
                or meta.row_range_start != 0
                or meta.row_range_end != source_row_count - 1
                or payload.row_count != source_row_count):
            return None
        return PkSortedIndexGroup(
            source_meta.data_level, tuple(source_files), tuple(payloads))
