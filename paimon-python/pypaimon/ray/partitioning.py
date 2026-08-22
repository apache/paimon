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

from typing import Optional, Sequence


def _resolve_num_partitions(
    num_partitions: Optional[int],
    estimated_size_bytes: Optional[int] = None,
) -> int:
    """Resolve Ray shuffle parallelism without consuming the input Dataset.

    An explicit value is preserved. For the default, the previous ``2 * CPU``
    value remains the upper bound and the fallback when input metadata is not
    available. A known input size can reduce small jobs to roughly one
    partition per Ray target-sized block.
    """
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
    return min(max_partitions, size_partitions)


def _estimate_dataset_size_bytes(dataset) -> Optional[int]:
    """Return logical-plan size metadata, never executing ``dataset``.

    Ray's public ``Dataset.size_bytes()`` executes a Dataset when its logical
    metadata is unknown. Partition selection must not add such an action, so
    use the guarded logical metadata hook and fall back when Ray cannot infer
    the size (for example after a row-count-changing ``map_batches``).
    """
    try:
        logical_plan = getattr(dataset, "_logical_plan", None)
        dag = getattr(logical_plan, "dag", None)
        infer_metadata = getattr(dag, "infer_metadata", None)
        if not callable(infer_metadata):
            return None
        size_bytes = getattr(infer_metadata(), "size_bytes", None)
        if size_bytes is None or int(size_bytes) < 0:
            return None
        return int(size_bytes)
    except Exception:
        return None


def _estimate_table_scan_size_bytes(
    table,
    snapshot_id: Optional[int],
    projection: Sequence[str],
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
        read_builder = scan_table.new_read_builder().with_projection(
            list(projection)
        )
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
