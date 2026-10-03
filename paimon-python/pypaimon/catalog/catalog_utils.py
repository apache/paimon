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

"""Helpers shared across catalog implementations."""

import re
from typing import Optional

from pypaimon.api.api_response import PagedList, Partition
from pypaimon.table.table import Table


def list_partitions_from_file_system(
        table: Table,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
        partition_name_pattern: Optional[str] = None,
) -> PagedList[Partition]:
    """Compute partitions from a table's own metadata (manifest entries).

    Mirrors Java ``CatalogUtils.listPartitionsFromFileSystem``: the aggregation
    is shared by the filesystem catalog and by the REST catalog, which falls
    back to it when the server does not implement the partition-listing
    endpoint.
    """
    from pypaimon.manifest.manifest_file_manager import ManifestFileManager
    from pypaimon.manifest.manifest_list_manager import ManifestListManager

    snapshot = table.snapshot_manager().get_latest_snapshot()
    if snapshot is None:
        return PagedList(elements=[])

    # Read all manifest entries (ADD - DELETE merged) and group by partition.
    manifest_list_manager = ManifestListManager(table)
    manifest_file_manager = ManifestFileManager(table)
    manifest_files = manifest_list_manager.read_all(snapshot)
    entries = manifest_file_manager.read_entries_parallel(manifest_files, drop_stats=True)

    partition_map = {}  # spec_key -> aggregated stats
    for entry in entries:
        spec = {field.name: str(v) for field, v in
                zip(entry.partition.fields, entry.partition.values)}
        spec_key = tuple(sorted(spec.items()))

        if spec_key not in partition_map:
            partition_map[spec_key] = {
                'spec': spec,
                'record_count': 0,
                'file_size_in_bytes': 0,
                'file_count': 0,
                'last_file_creation_time': 0,
                'buckets': set(),
            }
        stats = partition_map[spec_key]
        stats['record_count'] += entry.file.row_count
        stats['file_size_in_bytes'] += entry.file.file_size
        stats['file_count'] += 1
        if entry.file.creation_time is not None:
            ct = entry.file.creation_time.get_millisecond()
            if ct > stats['last_file_creation_time']:
                stats['last_file_creation_time'] = ct
        stats['buckets'].add(entry.bucket)

    partitions = [
        Partition(
            spec=stats['spec'],
            record_count=stats['record_count'],
            file_size_in_bytes=stats['file_size_in_bytes'],
            file_count=stats['file_count'],
            last_file_creation_time=stats['last_file_creation_time'],
            total_buckets=len(stats['buckets']),
        )
        for stats in partition_map.values()
    ]

    # Apply pattern filter with proper regex escaping.
    if partition_name_pattern:
        # Escape special regex chars except '*', then replace '*' with '.*'.
        escaped_pattern = re.escape(partition_name_pattern).replace(r'\*', '.*')
        regex = re.compile(escaped_pattern)
        partitions = [
            p for p in partitions
            if regex.fullmatch(','.join(f'{k}={v}' for k, v in p.spec.items()))
        ]

    # Sort partitions by name (partition spec string).
    partitions.sort(key=lambda p: ','.join(f'{k}={v}' for k, v in sorted(p.spec.items())))

    # Apply pagination.
    start_index = 0
    if page_token is not None:
        try:
            start_index = int(page_token)
        except ValueError:
            # Invalid token, start from beginning.
            start_index = 0

    end_index = len(partitions)
    if max_results is not None and max_results > 0:
        end_index = min(start_index + max_results, len(partitions))

    result_partitions = partitions[start_index:end_index]
    next_page_token = None
    if max_results is not None and end_index < len(partitions):
        next_page_token = str(end_index)

    return PagedList(elements=result_partitions, next_page_token=next_page_token)
