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

"""The util class of resolve snapshot from scan params for time travel."""

from datetime import datetime
from typing import Optional

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.tag.tag_manager import TagManager

SCAN_KEYS = [
    CoreOptions.SCAN_SNAPSHOT_ID.key(),
    CoreOptions.SCAN_TAG_NAME.key(),
    CoreOptions.SCAN_WATERMARK.key(),
    CoreOptions.SCAN_TIMESTAMP.key(),
    CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
]


def _parse_timestamp_to_millis(timestamp_str: str) -> int:
    """Parse a timestamp string to milliseconds since epoch using local timezone.

    Consistent with Java's TimeZone.getDefault() behavior in TimeTravelUtil.

    Supports formats:
    - '2023-12-01 12:00:00'
    - '2023-12-01T12:00:00'
    - '2023-12-01 12:00:00.123'
    """
    formats = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(timestamp_str, fmt)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    raise ValueError(
        f"Cannot parse timestamp '{timestamp_str}'. "
        f"Expected format: 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd HH:mm:ss.SSS'"
    )


class TimeTravelUtil:
    """The util class of resolve snapshot from scan params for time travel."""

    @staticmethod
    def try_travel_to_snapshot(
            options: Options,
            tag_manager: TagManager,
            snapshot_manager=None,
    ) -> Optional[Snapshot]:
        """
        Try to travel to a snapshot based on the options.

        Supports the following time travel options:
        - scan.tag-name: Travel to a specific tag
        - scan.snapshot-id: Travel to a specific snapshot id
        - scan.timestamp-millis: Travel to the latest snapshot <= the given timestamp (ms)
        - scan.timestamp: Travel by timestamp string (e.g. '2023-12-01 12:00:00')
        - scan.watermark: Travel to the first snapshot with watermark >= the given value

        Args:
            options: The options containing time travel parameters
            tag_manager: The tag manager
            snapshot_manager: The snapshot manager, required when
                using snapshot-id, timestamp, or watermark based time travel

        Returns:
            The Snapshot to travel to, or None if no time travel option is set.

        Raises:
            ValueError: If more than one time travel option is set, or if the
                required manager is not provided
        """

        scan_handle_keys = [key for key in SCAN_KEYS if options.contains_key(key)]

        if not scan_handle_keys:
            return None

        if len(scan_handle_keys) > 1:
            raise ValueError(f"Only one of the following parameters may be set: {SCAN_KEYS}")

        key = scan_handle_keys[0]
        core_options = CoreOptions(options)

        if key == CoreOptions.SCAN_TAG_NAME.key():
            tag_name = core_options.scan_tag_name()
            tag = tag_manager.get(tag_name)
            if tag is None:
                raise ValueError(f"Tag '{tag_name}' doesn't exist.")
            return tag.trim_to_snapshot()
        elif key == CoreOptions.SCAN_SNAPSHOT_ID.key():
            if snapshot_manager is None:
                raise ValueError(
                    "snapshot_manager is required to resolve scan.snapshot-id"
                )
            snapshot_id = int(core_options.scan_snapshot_id())
            snapshot = snapshot_manager.get_snapshot_by_id(snapshot_id)
            if snapshot is None:
                raise ValueError(f"Snapshot id '{snapshot_id}' doesn't exist.")
            return snapshot
        elif key == CoreOptions.SCAN_TIMESTAMP_MILLIS.key():
            if snapshot_manager is None:
                raise ValueError(
                    "snapshot_manager is required to resolve scan.timestamp-millis"
                )
            timestamp_millis = int(core_options.scan_timestamp_millis())
            snapshot = snapshot_manager.earlier_or_equal_time_mills(timestamp_millis)
            if snapshot is None:
                raise ValueError(
                    f"No snapshot found with timestamp earlier than or equal to {timestamp_millis}ms."
                )
            return snapshot
        elif key == CoreOptions.SCAN_TIMESTAMP.key():
            if snapshot_manager is None:
                raise ValueError(
                    "snapshot_manager is required to resolve scan.timestamp"
                )
            timestamp_str = core_options.scan_timestamp()
            timestamp_millis = _parse_timestamp_to_millis(timestamp_str)
            snapshot = snapshot_manager.earlier_or_equal_time_mills(timestamp_millis)
            if snapshot is None:
                raise ValueError(
                    f"No snapshot found with timestamp earlier than or equal to '{timestamp_str}'."
                )
            return snapshot
        elif key == CoreOptions.SCAN_WATERMARK.key():
            if snapshot_manager is None:
                raise ValueError(
                    "snapshot_manager is required to resolve scan.watermark"
                )
            watermark = int(core_options.scan_watermark())
            snapshot = snapshot_manager.later_or_equal_watermark(watermark)
            if snapshot is None:
                raise ValueError(
                    f"No snapshot found with watermark greater than or equal to {watermark}."
                )
            return snapshot
        else:
            raise ValueError(f"Unsupported time travel mode: {key}")
