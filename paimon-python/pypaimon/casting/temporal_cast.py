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

"""Java local-zone conversions used by existing schema-evolution casts."""

import pyarrow as pa

from pypaimon.casting.temporal_utils import (
    _epoch_day_to_java_date, _java_date_to_epoch_day,
    _local_offset, _local_to_utc_seconds, _signed,
)
from pypaimon.table.row.generic_row import _parse_type_precision_scale


def cast_temporal(array, target, source_type=None, target_type=None):
    timestamp = pa.types.is_timestamp(array.type)
    source_name = str(source_type).upper() if source_type is not None else ''
    ltz = timestamp and (array.type.tz is not None if source_type is None else
                         source_name.startswith('TIMESTAMP_LTZ') or 'WITH LOCAL TIME ZONE' in source_name)
    target_ts = pa.types.is_timestamp(target)
    cross = timestamp and target_ts and ltz != (target.tz is not None)
    date = pa.types.is_date32(array.type) and target_ts and target.tz is not None
    numeric = array.type == pa.int64() and target_ts and target.tz is not None
    if not (cross or date or numeric or timestamp and (pa.types.is_date32(target) or pa.types.is_time(target))):
        return None
    raw = array.cast(pa.int32() if date else pa.int64()).to_pylist()
    unit = array.type.unit if timestamp else 's'
    source_precision = (_parse_type_precision_scale(source_type)[0] if source_type is not None else
                        {'s': 0, 'ms': 3, 'us': 6, 'ns': 9}[unit] if timestamp else 0)
    target_precision = (_parse_type_precision_scale(target_type)[0] if target_type is not None else
                        {'s': 0, 'ms': 3, 'us': 6, 'ns': 9}[target.unit] if target_ts else 0)
    values = []
    for value in raw:
        if value is None:
            values.append(None)
            continue
        if date:
            millis, sub_milli = _java_date_to_epoch_day(value) * 86400000, 0
        elif unit == 's':
            millis, sub_milli = _signed(value * 1000, 64), 0
        elif unit == 'ms':
            millis, sub_milli = value, 0
        elif unit == 'us':
            millis, sub_milli = value // 1000, value % 1000 * 1000
        else:
            millis, sub_milli = value // 1000000, value % 1000000
        original = millis
        if cross and not ltz:
            millis = _signed(millis // 86400000, 32) * 86400000 + millis % 86400000
        if date or cross and not ltz:
            millis = _local_to_utc_seconds(millis // 1000) * 1000 + millis % 1000
        elif ltz or numeric:
            millis = _signed(millis + _local_offset(millis // 1000) * 1000, 64)
        if pa.types.is_date32(target):
            value = (_epoch_day_to_java_date((original + _local_offset(original // 1000) * 1000) // 86400000)
                     if ltz else _signed(millis // 86400000, 32))
        elif pa.types.is_time(target):
            value = ((original + _local_offset(original // 1000) * 1000) % 86400000
                     if ltz else millis % 86400000)
        else:
            if source_precision > target_precision:
                if target_precision <= 3:
                    quantum = 10 ** (3 - target_precision)
                    millis, sub_milli = _signed(millis // quantum * quantum, 64), 0
                else:
                    quantum = 10 ** (9 - target_precision)
                    sub_milli = sub_milli // quantum * quantum
            if target.unit == 's':
                value = millis // 1000
            elif target.unit == 'ms':
                value = millis
            elif target.unit == 'us':
                value = millis * 1000 + sub_milli // 1000
            else:
                value = millis * 1000000 + sub_milli
            if not -(1 << 63) <= value < (1 << 63):
                value = None
        values.append(value)
    if pa.types.is_time(target):
        return pa.array(values, type=pa.int32()).cast(pa.time32('ms')).cast(target, safe=False)
    return pa.array(values, type=pa.int32() if pa.types.is_date32(target) else pa.int64()).cast(target)
