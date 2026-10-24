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

"""Shared dynamic-bucket limits, validation and read-view configuration."""

HASH_INDEX = "HASH"
SHORT_MAX_VALUE = 32767
MAX_DYNAMIC_BUCKETS = SHORT_MAX_VALUE + 1


def resolve_bucket_snapshot(table, base_snapshot_id=None):
    """Capture the same restore point for input assignment and worker writers."""
    if base_snapshot_id == 0:
        return None
    manager = table.snapshot_manager()
    if base_snapshot_id is None:
        return manager.get_latest_snapshot()
    snapshot = manager.get_snapshot_by_id(base_snapshot_id)
    if snapshot is None:
        raise ValueError("Snapshot id '{}' doesn't exist".format(base_snapshot_id))
    return snapshot


def bucket_read_builder(table, partition, snapshot):
    """Pin metadata lookup and partition selection without creating a scanner."""
    from pypaimon.common.options.core_options import CoreOptions

    scan_table = table.copy({CoreOptions.SCAN_SNAPSHOT_ID.key(): str(snapshot.id)})
    read_builder = scan_table.new_read_builder()
    if scan_table.partition_keys:
        predicate_builder = read_builder.new_predicate_builder()
        predicates = [
            predicate_builder.is_null(key) if value is None
            else predicate_builder.equal(key, value)
            for key, value in zip(scan_table.partition_keys, partition)
        ]
        read_builder.with_filter(predicate_builder.and_predicates(predicates))
    return read_builder


def to_signed_int32(value: int) -> int:
    """Return the Java ``int`` representation of ``value``."""
    value &= 0xFFFFFFFF
    return value - 0x100000000 if value >= 0x80000000 else value


def validate_max_buckets(max_buckets_num: int) -> None:
    if max_buckets_num != -1 and not 1 <= max_buckets_num <= MAX_DYNAMIC_BUCKETS:
        raise ValueError(
            "'dynamic-bucket.max-buckets' must be -1 or between 1 and "
            f"{MAX_DYNAMIC_BUCKETS}, but was {max_buckets_num}."
        )


def validate_bucket_id(bucket: int) -> None:
    if not 0 <= bucket < MAX_DYNAMIC_BUCKETS:
        raise ValueError(
            "Dynamic bucket id must be between 0 and "
            f"{MAX_DYNAMIC_BUCKETS - 1}, but was {bucket}."
        )
