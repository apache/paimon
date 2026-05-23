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

"""
Main orchestration class for partition expiration.

This mirrors the Java implementation at:
  org.apache.paimon.operation.PartitionExpire
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pypaimon.partition.partition_expire_strategy import (
    PartitionEntry,
    PartitionExpireStrategy,
    PartitionUpdateTimeExpireStrategy,
    PartitionValuesTimeExpireStrategy,
)

logger = logging.getLogger(__name__)


class PartitionExpire:
    """
    Orchestrates partition expiration: reads partition entries, applies the
    configured strategy, and drops expired partitions.

    Usage:
        expire = PartitionExpire.from_table(table)
        expired = expire.expire()

    Or construct directly:
        expire = PartitionExpire(
            expiration_time=timedelta(days=7),
            strategy=PartitionValuesTimeExpireStrategy(...),
            partition_reader=...,
            partition_dropper=...,
        )
        expired = expire.expire()
    """

    def __init__(
        self,
        expiration_time: timedelta,
        strategy: PartitionExpireStrategy,
        partition_reader,  # Callable[[], List[PartitionEntry]]
        partition_dropper,  # Callable[[List[Dict[str, str]], int], None]
        check_interval: Optional[timedelta] = None,
        max_expire_num: int = 100,
    ):
        """
        Args:
            expiration_time: How old a partition must be before it expires.
            strategy: The strategy to determine partition expiration time.
            partition_reader: A callable that returns all current PartitionEntry objects.
            partition_dropper: A callable that drops partitions (accepts List[Dict[str,str]], commit_id).
            check_interval: Minimum interval between expiration checks. If None, always check.
            max_expire_num: Maximum number of partitions to expire in one call (default 100, matching Java).
        """
        self._expiration_time = expiration_time
        self._strategy = strategy
        self._partition_reader = partition_reader
        self._partition_dropper = partition_dropper
        self._check_interval = check_interval
        self._max_expire_num = max_expire_num
        self._last_check: Optional[datetime] = None

    def expire(self, now: Optional[datetime] = None) -> List[Dict[str, str]]:
        """
        Run partition expiration.

        Args:
            now: The current time (defaults to datetime.now() if not specified).

        Returns:
            List of expired partition specs that were dropped.
            Empty list if no partitions expired or check interval not yet elapsed.
        """
        if now is None:
            now = datetime.now()

        # Check interval gating
        if self._check_interval is not None and self._last_check is not None:
            if now < self._last_check + self._check_interval:
                return []

        expired_specs = self._do_expire(now)
        self._last_check = now
        return expired_specs

    def _do_expire(self, now: datetime) -> List[Dict[str, str]]:
        """Perform the actual expiration logic."""
        expire_datetime = now - self._expiration_time

        # Read all partition entries
        partition_entries = self._partition_reader()
        if not partition_entries:
            return []

        # Apply strategy to find expired partitions
        expired_entries = self._strategy.select_expired_partitions(partition_entries, expire_datetime)
        if not expired_entries:
            return []

        # Sort by partition spec string for deterministic behavior
        expired_entries.sort(
            key=lambda e: ",".join(f"{k}={v}" for k, v in sorted(e.spec.items()))
        )

        # Limit the number of partitions to expire
        if self._max_expire_num > 0 and len(expired_entries) > self._max_expire_num:
            expired_entries = expired_entries[: self._max_expire_num]

        # Convert to partition specs
        expired_specs = [self._strategy.to_partition_string(entry) for entry in expired_entries]

        if expired_specs:
            logger.info("Expiring partitions: %s", expired_specs)
            # Use a fixed commit identifier for batch operations
            commit_identifier = int(now.timestamp() * 1000)
            self._partition_dropper(expired_specs, commit_identifier)

        return expired_specs

    @classmethod
    def from_table(cls, table, options: Optional[Dict[str, str]] = None) -> Optional['PartitionExpire']:
        """
        Create a PartitionExpire instance from a FileStoreTable.

        Reads configuration from table options:
            - partition.expiration-time
            - partition.expiration-check-interval
            - partition.expiration-strategy (values-time | update-time)
            - partition.timestamp-pattern
            - partition.timestamp-formatter

        Args:
            table: A FileStoreTable instance.
            options: Optional override options dict.

        Returns:
            A PartitionExpire instance, or None if expiration is not configured.
        """
        # Get table options
        table_options = dict(table.table_schema.options) if table.table_schema.options else {}
        if options:
            table_options.update(options)

        # Check if partition expiration is configured
        expiration_time_str = table_options.get("partition.expiration-time")
        if not expiration_time_str:
            return None

        # Table must have partition keys
        if not table.partition_keys:
            logger.warning("Cannot configure partition expiration on a non-partitioned table.")
            return None

        # Parse expiration time
        expiration_time = _parse_duration(expiration_time_str)
        if expiration_time is None:
            logger.warning("Invalid partition.expiration-time: %s", expiration_time_str)
            return None

        # Parse check interval
        check_interval_str = table_options.get("partition.expiration-check-interval")
        check_interval = _parse_duration(check_interval_str) if check_interval_str else None

        # Parse strategy
        strategy_name = table_options.get("partition.expiration-strategy", "values-time")
        partition_keys = list(table.partition_keys)
        partition_default_name = table_options.get("partition.default-name", "__DEFAULT_PARTITION__")
        timestamp_pattern = table_options.get("partition.timestamp-pattern")
        timestamp_formatter = table_options.get("partition.timestamp-formatter")

        if strategy_name == "values-time":
            strategy = PartitionValuesTimeExpireStrategy(
                partition_keys=partition_keys,
                partition_default_name=partition_default_name,
                timestamp_pattern=timestamp_pattern,
                timestamp_formatter=timestamp_formatter,
            )
        elif strategy_name == "update-time":
            strategy = PartitionUpdateTimeExpireStrategy(
                partition_keys=partition_keys,
                partition_default_name=partition_default_name,
            )
        else:
            raise ValueError(f"Unknown partition expiration strategy: '{strategy_name}'. "
                             f"Supported: 'values-time', 'update-time'.")

        # Parse max expire num
        max_expire_num_str = table_options.get("partition.expiration-max-num")
        max_expire_num = int(max_expire_num_str) if max_expire_num_str else 100

        # Build partition reader: reads manifest entries and aggregates by partition
        def partition_reader() -> List[PartitionEntry]:
            return _read_partition_entries(table)

        # Build partition dropper: uses FileStoreCommit.drop_partitions
        def partition_dropper(partitions: List[Dict[str, str]], commit_identifier: int):
            commit = table.new_batch_write_builder().new_commit()
            try:
                commit.truncate_partitions(partitions)
            finally:
                commit.close()

        return cls(
            expiration_time=expiration_time,
            strategy=strategy,
            partition_reader=partition_reader,
            partition_dropper=partition_dropper,
            check_interval=check_interval,
            max_expire_num=max_expire_num,
        )


def _read_partition_entries(table) -> List[PartitionEntry]:
    """
    Read all active partition entries from the table's latest snapshot.

    This aggregates manifest entries by partition to produce per-partition statistics,
    mirroring the logic in filesystem_catalog.list_partitions_paged.
    """
    from pypaimon.manifest.manifest_list_manager import ManifestListManager
    from pypaimon.manifest.manifest_file_manager import ManifestFileManager

    snapshot = table.snapshot_manager().get_latest_snapshot()
    if snapshot is None:
        return []

    manifest_list_manager = ManifestListManager(table)
    manifest_file_manager = ManifestFileManager(table)
    manifest_files = manifest_list_manager.read_all(snapshot)
    entries = manifest_file_manager.read_entries_parallel(manifest_files, drop_stats=True)

    # Group entries by partition spec
    partition_map = {}  # spec_key -> aggregated stats
    partition_default_name = (table.table_schema.options or {}).get(
        "partition.default-name", "__DEFAULT_PARTITION__")
    for entry in entries:
        spec = {}
        for field, v in zip(entry.partition.fields, entry.partition.values):
            if v is None:
                spec[field.name] = partition_default_name
            else:
                spec[field.name] = str(v)
        spec_key = tuple(sorted(spec.items()))

        if spec_key not in partition_map:
            partition_map[spec_key] = {
                'spec': spec,
                'record_count': 0,
                'file_size_in_bytes': 0,
                'file_count': 0,
                'last_file_creation_time': 0,
            }
        stats = partition_map[spec_key]
        stats['record_count'] += entry.file.row_count
        stats['file_size_in_bytes'] += entry.file.file_size
        stats['file_count'] += 1
        if entry.file.creation_time is not None:
            ct = entry.file.creation_time.get_millisecond()
            if ct > stats['last_file_creation_time']:
                stats['last_file_creation_time'] = ct

    return [
        PartitionEntry(
            spec=stats['spec'],
            record_count=stats['record_count'],
            file_count=stats['file_count'],
            file_size_in_bytes=stats['file_size_in_bytes'],
            last_file_creation_time=stats['last_file_creation_time'],
        )
        for stats in partition_map.values()
    ]


def _parse_duration(duration_str: Optional[str]) -> Optional[timedelta]:
    """
    Parse a duration string like '7d', '1h', '30m', '10s', '1d 2h'.
    Also supports ISO-like formats: '7 days', '1 hour'.

    Returns None if parsing fails.
    """
    if not duration_str:
        return None

    duration_str = duration_str.strip()

    # Try simple numeric formats with suffix
    import re
    total_seconds = 0
    found = False

    # Match patterns like: 7d, 12h, 30m, 60s, 500ms
    # Order matters: longer suffixes first to avoid partial matches (e.g., 'ms' before 'm')
    pattern = re.compile(
        r'(\d+)\s*(milliseconds|millisecond|ms|days|day|d|hours|hour|h|minutes|minute|min|m|seconds|second|sec|s)'
    )
    for match in pattern.finditer(duration_str.lower()):
        found = True
        value = int(match.group(1))
        unit = match.group(2)
        if unit in ('d', 'day', 'days'):
            total_seconds += value * 86400
        elif unit in ('h', 'hour', 'hours'):
            total_seconds += value * 3600
        elif unit in ('m', 'min', 'minute', 'minutes'):
            total_seconds += value * 60
        elif unit in ('s', 'sec', 'second', 'seconds'):
            total_seconds += value
        elif unit in ('ms', 'millisecond', 'milliseconds'):
            total_seconds += value / 1000.0

    if found:
        return timedelta(seconds=total_seconds)

    # Try plain integer as milliseconds (Java Duration convention)
    try:
        ms = int(duration_str)
        return timedelta(milliseconds=ms)
    except ValueError:
        pass

    return None
