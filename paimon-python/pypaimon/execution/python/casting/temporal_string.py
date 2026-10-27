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

"""Java temporal-to-string schema evolution for Arrow arrays."""

import pyarrow as pa

from pypaimon.utils.temporal_string import format_time, format_timestamp_parts
from pypaimon.utils.temporal_utils import _epoch_day_to_date, _local_offset
from pypaimon.table.row.generic_row import _parse_type_precision_scale


def temporal_to_string(array, source_type=None, target_type=None):
    """Return None for non-temporal input; preserve NULLs, slices and nanos.

    LTZ uses the process local zone, as Java TimeZone.getDefault() does.
    Character length is applied after formatting, using the target Paimon type.
    """
    timestamp = pa.types.is_timestamp(array.type)
    if not timestamp and not pa.types.is_time(array.type):
        return None
    unit = array.type.unit
    precision = (_parse_type_precision_scale(source_type)[0] if source_type is not None
                 else {'s': 0, 'ms': 3, 'us': 6, 'ns': 9}[unit])
    if timestamp:
        raw = array.cast(pa.int64()).to_pylist()
        ticks = {'s': 1, 'ms': 1000, 'us': 1000000, 'ns': 1000000000}[unit]
        source_name = str(source_type).upper() if source_type is not None else ''
        local_zone = (array.type.tz is not None if source_type is None else
                      source_name.startswith('TIMESTAMP_LTZ') or 'WITH LOCAL TIME ZONE' in source_name)
    else:
        raw = array.cast(pa.time32('ms'), safe=False).cast(pa.int32()).to_pylist()
    name = str(target_type).upper() if target_type is not None else 'STRING'
    length = (_parse_type_precision_scale(target_type)[0] or 1) if name.startswith(('CHAR', 'VARCHAR')) else None
    values = []
    for value in raw:
        if value is None:
            values.append(None)
            continue
        if timestamp:
            millis = value * 1000 if unit == 's' else value // (ticks // 1000)
            # Arrow seconds may exceed Java's signed millisecond storage.
            millis = (millis + (1 << 63)) % (1 << 64) - (1 << 63)
            seconds = millis // 1000
            if local_zone:
                millis += _local_offset(seconds) * 1000
                millis = (millis + (1 << 63)) % (1 << 64) - (1 << 63)
                seconds = millis // 1000
            sub_milli = value % (ticks // 1000) * (1000000000 // ticks) if ticks > 1000 else 0
            nanos = millis % 1000 * 1000000 + sub_milli
            day, second_of_day = divmod(seconds, 86400)
            day = (day + (1 << 31)) % (1 << 32) - (1 << 31)
            year, month, day = _epoch_day_to_date(day)
            hour, second_of_day = divmod(second_of_day, 3600)
            minute, second = divmod(second_of_day, 60)
            text = format_timestamp_parts(year, month, day, hour, minute, second,
                                          nanos, precision)
        else:
            text = format_time(value, precision)
        if length is not None:
            text = text[:length]
            if name.startswith('CHAR'):
                text = text.ljust(length)
        values.append(text)
    return pa.array(values, type=pa.string())
