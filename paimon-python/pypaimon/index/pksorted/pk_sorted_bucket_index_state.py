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
from pypaimon.index.pk.primary_key_index_source_policy import should_read
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
            if not should_read(data_file):
                continue
            sources_by_level.setdefault(data_file.level, []).append(
                PrimaryKeyIndexSourceFile(data_file.file_name, data_file.row_count))
        for sources in sources_by_level.values():
            sources.sort(key=lambda source: source.file_name)

        candidates_by_level = {}
        rejected = []
        for payload in active_payloads:
            try:
                source_meta = PrimaryKeyIndexSourceMeta.from_index_file(payload)
                active_level_sources = sources_by_level.get(source_meta.data_level)
                if active_level_sources is None:
                    rejected.append(payload)
                    continue
                active_intersection = _active_intersection(
                    active_level_sources, source_meta.source_files)
                if not active_intersection:
                    rejected.append(payload)
                    continue
                group = PkSortedIndexGroup.create(
                    field_id, index_type, source_meta.source_files, [payload])
                if group is None:
                    rejected.append(payload)
                    continue
                candidates_by_level.setdefault(source_meta.data_level, []).append(
                    (payload, group, active_intersection))
            except (TypeError, ValueError):
                rejected.append(payload)

        groups = []
        covered_sources_by_level = {}
        for level, candidates in sorted(candidates_by_level.items()):
            if len(candidates) != 1:
                rejected.extend(candidate[0] for candidate in candidates)
                continue
            _, group, active_intersection = candidates[0]
            groups.append(group)
            covered_sources_by_level[level] = set(active_intersection)

        covered = []
        uncovered = []
        for level, sources in sorted(sources_by_level.items()):
            covered_sources = covered_sources_by_level.get(level, set())
            for source in sources:
                (covered if source in covered_sources else uncovered).append(source)
        return PkSortedBucketIndexState(
            tuple(groups), tuple(covered), tuple(uncovered), tuple(rejected))


def _active_intersection(active_sources, payload_sources):
    intersection = []
    active_source_index = 0
    for index, source in enumerate(payload_sources):
        if index > 0 and payload_sources[index - 1].file_name >= source.file_name:
            return None
        while (active_source_index < len(active_sources)
               and active_sources[active_source_index].file_name < source.file_name):
            active_source_index += 1
        if (active_source_index == len(active_sources)
                or active_sources[active_source_index].file_name != source.file_name):
            continue
        if active_sources[active_source_index].row_count != source.row_count:
            return None
        intersection.append(source)
    return intersection
