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
    unknown_num_partitions: Optional[int] = None,
    data_context=None,
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
        if unknown_num_partitions is not None:
            return min(
                max_partitions,
                max(min_partitions, int(unknown_num_partitions)),
            )
        return max_partitions

    try:
        from ray.data.context import DataContext

        context = (
            data_context
            if data_context is not None
            else DataContext.get_current()
        )
        target_size_bytes = int(context.target_max_block_size)
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
            can_modify_num_rows = _can_modify_num_rows(operator)
            if callable(infer_metadata):
                value = getattr(infer_metadata(), field, None)
                if (
                    value is not None
                    and int(value) >= 0
                    and not (
                        field == "size_bytes"
                        and can_modify_num_rows is not None
                    )
                ):
                    return int(value)
            if field == "size_bytes":
                return None
            # Only inherit row count through transforms Ray marks preserving.
            if can_modify_num_rows is not False:
                return None
            dependencies = getattr(operator, "input_dependencies", ())
            operator = dependencies[0] if len(dependencies) == 1 else None
    except Exception:
        pass
    return None


def _can_modify_num_rows(operator) -> Optional[bool]:
    value = getattr(operator, "can_modify_num_rows", None)
    if not callable(value):
        return value if isinstance(value, bool) else None

    # Ray 2.50/2.51 MapBatches reports False without a cardinality flag.
    if (
        type(operator).__name__ == "MapBatches"
        and not hasattr(operator, "_udf_modifying_row_count")
    ):
        return None
    try:
        value = value()
    except Exception:
        return None
    return value if isinstance(value, bool) else None


def _default_hash_shuffle_parallelism(data_context=None) -> int:
    try:
        from ray.data.context import DataContext

        context = (
            data_context
            if data_context is not None
            else DataContext.get_current()
        )
        return max(
            1,
            int(context.default_hash_shuffle_parallelism),
        )
    except Exception:
        return 200


def _resolve_row_id_num_partitions(
    num_partitions: Optional[int],
    estimated_size_bytes: Optional[int],
    estimated_num_rows: Optional[int],
    target_file_count: int,
    data_context=None,
) -> int:
    """Resolve row-ID partitions from input size and target fan-out."""
    if num_partitions is not None:
        return num_partitions

    default_shuffle = _default_hash_shuffle_parallelism(data_context)

    possible_groups = max(1, target_file_count)
    if estimated_num_rows is not None:
        possible_groups = min(possible_groups, max(1, estimated_num_rows))
    min_partitions = min(max(1, default_shuffle), possible_groups)
    if estimated_size_bytes is None:
        return min(
            _resolve_num_partitions(None, data_context=data_context),
            min_partitions,
        )
    return _resolve_num_partitions(
        None,
        estimated_size_bytes,
        min_partitions=min_partitions,
        data_context=data_context,
    )
