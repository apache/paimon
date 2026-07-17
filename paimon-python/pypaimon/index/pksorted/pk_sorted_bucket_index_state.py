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

from pypaimon.index.pk.primary_key_index_source_file import PrimaryKeyIndexSourceFile
from pypaimon.index.pk.primary_key_index_source_meta import PrimaryKeyIndexSourceMeta
from pypaimon.index.pksorted.pk_sorted_index_group import PkSortedIndexGroup


@dataclass(frozen=True)
class PkSortedBucketIndexState:
    groups: tuple
    covered_source_files: tuple
    uncovered_source_files: tuple
    rejected_payloads: tuple

    @staticmethod
    def from_active_data_files(field_id, index_type, active_data_files, active_payloads):
        sources_by_level = {}
        for data_file in active_data_files:
            if data_file.file_source != 1 or data_file.level <= 0:
                continue
            sources_by_level.setdefault(data_file.level, []).append(
                PrimaryKeyIndexSourceFile(data_file.file_name, data_file.row_count))
        for sources in sources_by_level.values():
            sources.sort(key=lambda source: source.file_name)

        payloads_by_level = {}
        rejected = []
        for payload in active_payloads:
            try:
                source_meta = PrimaryKeyIndexSourceMeta.from_index_file(payload)
                desired = sources_by_level.get(source_meta.data_level)
                if desired is None or tuple(desired) != tuple(source_meta.source_files):
                    rejected.append(payload)
                else:
                    payloads_by_level.setdefault(source_meta.data_level, []).append(payload)
            except (TypeError, ValueError):
                rejected.append(payload)

        groups = []
        covered_levels = set()
        for level, payloads in sorted(payloads_by_level.items()):
            group = PkSortedIndexGroup.create(
                field_id, index_type, sources_by_level[level], payloads)
            if group is None:
                rejected.extend(payloads)
            else:
                groups.append(group)
                covered_levels.add(level)

        covered = []
        uncovered = []
        for level, sources in sorted(sources_by_level.items()):
            (covered if level in covered_levels else uncovered).extend(sources)
        return PkSortedBucketIndexState(
            tuple(groups), tuple(covered), tuple(uncovered), tuple(rejected))
