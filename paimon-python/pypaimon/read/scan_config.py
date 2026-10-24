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

"""Backend independent validation for scan options."""

import re

from pypaimon.common.options.core_options import CoreOptions


def _validate_scan_mode(table):
    """Validate scan.mode against companion options using a whitelist approach.

    Each StartupMode declares exactly which scan keys are allowed. Any
    scan key present but not in the whitelist for the resolved mode is
    rejected. This matches Java's SchemaValidation mutual-exclusion matrix.
    """
    from pypaimon.common.options.core_options import StartupMode

    core_options = table.options
    mode = core_options.startup_mode()
    options = core_options.options

    has_snapshot_id = options.contains(CoreOptions.SCAN_SNAPSHOT_ID)
    has_tag_name = options.contains(CoreOptions.SCAN_TAG_NAME)
    has_version = options.contains(CoreOptions.SCAN_VERSION)
    has_watermark = options.contains(CoreOptions.SCAN_WATERMARK)
    has_timestamp_millis = options.contains(CoreOptions.SCAN_TIMESTAMP_MILLIS)
    has_timestamp = options.contains(CoreOptions.SCAN_TIMESTAMP)
    has_incremental = options.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP)
    has_file_creation_time = options.contains(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS)
    has_creation_time = options.contains(CoreOptions.SCAN_CREATION_TIME_MILLIS)

    present_keys = []
    if has_version:
        present_keys.append(CoreOptions.SCAN_VERSION.key())
    if has_snapshot_id:
        present_keys.append(CoreOptions.SCAN_SNAPSHOT_ID.key())
    if has_tag_name:
        present_keys.append(CoreOptions.SCAN_TAG_NAME.key())
    if has_watermark:
        present_keys.append(CoreOptions.SCAN_WATERMARK.key())
    if has_timestamp_millis:
        present_keys.append(CoreOptions.SCAN_TIMESTAMP_MILLIS.key())
    if has_timestamp:
        present_keys.append(CoreOptions.SCAN_TIMESTAMP.key())
    if has_incremental:
        present_keys.append(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key())
    if has_file_creation_time:
        present_keys.append(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key())
    if has_creation_time:
        present_keys.append(CoreOptions.SCAN_CREATION_TIME_MILLIS.key())

    # scan.timestamp-millis and scan.timestamp are mutually exclusive
    if has_timestamp_millis and has_timestamp:
        raise ValueError(
            "scan.timestamp-millis and scan.timestamp cannot both be set."
        )

    # Define allowed companion keys per mode
    if mode == StartupMode.FROM_TIMESTAMP:
        allowed = {
            CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
            CoreOptions.SCAN_TIMESTAMP.key(),
        }
        if not (has_timestamp_millis or has_timestamp):
            raise ValueError(
                "scan.mode is 'from-timestamp' but neither "
                "scan.timestamp-millis nor scan.timestamp is set."
            )
    elif mode == StartupMode.FROM_SNAPSHOT_FULL:
        allowed = {CoreOptions.SCAN_SNAPSHOT_ID.key()}
        if not has_snapshot_id:
            raise ValueError(
                "scan.mode is 'from-snapshot-full' but scan.snapshot-id is not set."
            )
    elif mode == StartupMode.FROM_SNAPSHOT:
        allowed = {
            CoreOptions.SCAN_SNAPSHOT_ID.key(),
            CoreOptions.SCAN_TAG_NAME.key(),
            CoreOptions.SCAN_WATERMARK.key(),
            CoreOptions.SCAN_VERSION.key(),
        }
        if not (has_snapshot_id or has_tag_name or has_watermark or has_version):
            raise ValueError(
                "scan.mode is 'from-snapshot' but none of "
                "scan.version, scan.snapshot-id, scan.tag-name, or scan.watermark is set."
            )
    elif mode == StartupMode.INCREMENTAL:
        allowed = {CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key()}
        if not has_incremental:
            raise ValueError(
                "scan.mode is 'incremental' but "
                "incremental-between-timestamp is not set."
            )
    elif mode in (StartupMode.LATEST_FULL, StartupMode.LATEST):
        allowed = set()
    elif mode in (StartupMode.COMPACTED_FULL,
                  StartupMode.FROM_CREATION_TIMESTAMP,
                  StartupMode.FROM_FILE_CREATION_TIME):
        raise ValueError(
            f"scan.mode '{mode.value}' is not yet supported in pypaimon."
        )
    else:
        allowed = set()

    # Reject any scan key that's not in the whitelist for this mode
    disallowed = [k for k in present_keys if k not in allowed]
    if disallowed:
        raise ValueError(
            f"scan.mode '{mode.value}' conflicts with: {disallowed}. "
            f"Only {sorted(allowed) if allowed else 'no scan keys'} "
            f"are allowed for this mode."
        )


def _parse_incremental_timestamp_window(value):
    """Numeric milliseconds use Java Long parsing, including its range."""
    parts = value.split(',')
    if len(parts) != 2 or any(re.fullmatch(r'[+-]?[0-9]+', part) is None for part in parts):
        raise ValueError('incremental-between-timestamp requires start(exclusive),end(inclusive) '
                         'in milliseconds, got: ' + value)
    start, end = map(int, parts)
    if not all(-(1 << 63) <= timestamp < (1 << 63) for timestamp in (start, end)):
        raise ValueError('incremental-between-timestamp milliseconds must be signed 64-bit integers')
    return start, end


def validate_scan_options(table):
    """Validate public scan inputs without opening snapshot metadata."""
    from pypaimon.snapshot.time_travel_util import SCAN_KEYS

    _validate_scan_mode(table)
    options = table.options.options
    has_time_travel = any(options.contains_key(key) for key in SCAN_KEYS)
    if not options.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP):
        return None
    if has_time_travel:
        raise ValueError(
            "incremental-between-timestamp cannot be used together with "
            "point-in-time scan options: %s" % SCAN_KEYS)
    return _parse_incremental_timestamp_window(
        options.get(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP))
