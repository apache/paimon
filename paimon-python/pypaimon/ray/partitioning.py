################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Best-effort partition sizing for PyPaimon Ray operations."""

from typing import Optional


def _resolve_num_partitions(
    num_partitions: Optional[int],
    estimated_size_bytes: Optional[int] = None,
    min_partitions: int = 1,
) -> int:
    """Resolve default shuffle partitions from input size and CPU count."""
    if num_partitions is not None:
        return num_partitions

    try:
        import ray

        cpus = int(ray.cluster_resources().get("CPU", 4))
        max_partitions = max(1, cpus * 2)
    except Exception:
        max_partitions = 4

    if estimated_size_bytes is None:
        return max_partitions

    try:
        from ray.data.context import DataContext

        target_size_bytes = int(
            DataContext.get_current().target_max_block_size
        )
    except Exception:
        return max_partitions

    if target_size_bytes <= 0:
        return max_partitions
    size_partitions = max(
        1,
        (max(0, int(estimated_size_bytes)) + target_size_bytes - 1)
        // target_size_bytes,
    )
    return min(max_partitions, max(min_partitions, size_partitions))


def _estimate_dataset_size_bytes(dataset) -> Optional[int]:
    """Read logical-plan size metadata without executing the Dataset."""
    return _estimate_dataset_metadata(dataset, "size_bytes")


def _estimate_dataset_num_rows(dataset) -> Optional[int]:
    """Read logical-plan row metadata without executing the Dataset."""
    return _estimate_dataset_metadata(dataset, "num_rows")


def _estimate_dataset_metadata(dataset, field: str) -> Optional[int]:
    try:
        operator = getattr(getattr(dataset, "_logical_plan", None), "dag", None)
        while operator is not None:
            infer_metadata = getattr(operator, "infer_metadata", None)
            if callable(infer_metadata):
                value = getattr(infer_metadata(), field, None)
                if value is not None and int(value) >= 0:
                    return int(value)
            dependencies = getattr(operator, "input_dependencies", ())
            operator = dependencies[0] if len(dependencies) == 1 else None
    except Exception:
        pass
    return None


def _resolve_row_id_num_partitions(
    num_partitions: Optional[int],
    estimated_size_bytes: Optional[int],
    estimated_num_rows: Optional[int],
    target_file_count: int,
) -> int:
    """Resolve row-ID partitions from input size and target fan-out."""
    if num_partitions is not None:
        return num_partitions

    try:
        from ray.data.context import DataContext

        default_shuffle = int(
            DataContext.get_current().default_hash_shuffle_parallelism
        )
    except Exception:
        default_shuffle = 200

    possible_groups = max(1, target_file_count)
    if estimated_num_rows is not None:
        possible_groups = min(possible_groups, max(1, estimated_num_rows))
    min_partitions = min(max(1, default_shuffle), possible_groups)
    return _resolve_num_partitions(
        None,
        estimated_size_bytes,
        min_partitions=min_partitions,
    )


def _estimate_table_scan_size_bytes(
    table,
    snapshot_id: Optional[int],
) -> Optional[int]:
    """Estimate a pinned Paimon scan from manifest file-size metadata."""
    try:
        from pypaimon.common.options.core_options import CoreOptions

        scan_table = (
            table.copy({
                CoreOptions.SCAN_SNAPSHOT_ID.key(): str(snapshot_id),
            })
            if snapshot_id is not None else table
        )
        read_builder = scan_table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        if not splits:
            return 0
        sizes = [int(getattr(split, "file_size", 0) or 0)
                 for split in splits]
        if any(size <= 0 for size in sizes):
            return None
        return sum(sizes)
    except Exception:
        return None
