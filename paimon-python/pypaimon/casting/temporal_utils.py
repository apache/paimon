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

"""Integer calendar and process-local zone helpers for Java temporal casts."""

import time


def _epoch_day_to_date(days):
    # March-based Gregorian eras, with floor division also for pre-epoch days.
    days += 719468
    era, day_of_era = divmod(days, 146097)
    year_of_era = (day_of_era - day_of_era // 1460 + day_of_era // 36524 - day_of_era // 146096) // 365
    year = year_of_era + era * 400
    day_of_year = day_of_era - (365 * year_of_era + year_of_era // 4 - year_of_era // 100)
    march_month = (5 * day_of_year + 2) // 153
    day = day_of_year - (153 * march_month + 2) // 5 + 1
    month = march_month + (3 if march_month < 10 else -9)
    return year + (month <= 2), month, day


def _local_offset(seconds):
    # Avoid converting the date itself through datetime's 1..9999 year range.
    local = time.localtime(seconds)
    offset = getattr(local, 'tm_gmtoff', None)
    if offset is not None:
        return offset
    return -(time.altzone if local.tm_isdst > 0 else time.timezone)


def _signed(value, bits):
    return (value + (1 << (bits - 1))) % (1 << bits) - (1 << (bits - 1))


def _div(value, divisor):
    # Java integer division truncates towards zero.
    return -(abs(value) // divisor) if value < 0 else value // divisor


def _rem(value, divisor):
    return value - _div(value, divisor) * divisor


def _java_date_to_epoch_day(date):
    j = _signed(date + 2440588 + 32044, 32)
    g, dg = _div(j, 146097), _rem(j, 146097)
    c = _div((_div(dg, 36524) + 1) * 3, 4)
    dc = dg - c * 36524
    b, db = _div(dc, 1461), _rem(dc, 1461)
    a = _div((_div(db, 365) + 1) * 3, 4)
    da = db - a * 365
    y = g * 400 + c * 100 + b * 4 + a
    m = _div(da * 5 + 308, 153) - 2
    d = da - _div((m + 4) * 153, 5) + 122
    year, month, day = y - 4800 + _div(m + 2, 12), _rem(m + 2, 12) + 1, d + 1
    leap = year % 400 == 0 or year % 4 == 0 and year % 100 != 0
    days = 29 if month == 2 and leap else 28 if month == 2 else 30 if month in (4, 6, 9, 11) else 31
    if not 1 <= month <= 12 or not 1 <= day <= days:
        raise ValueError('Invalid Java DATE conversion: {}-{}-{}'.format(year, month, day))
    year -= month <= 2
    era, yoe = divmod(year, 400)
    march = month + (-3 if month > 2 else 9)
    return era * 146097 + yoe * 365 + yoe // 4 - yoe // 100 + (153 * march + 2) // 5 + day - 1 - 719468


def _epoch_day_to_java_date(day):
    year, month, day = _epoch_day_to_date(day)
    a = (14 - month) // 12
    y, m = year + 4800 - a, month + 12 * a - 3
    return _signed(day + (153 * m + 2) // 5 + 365 * y + _div(y, 4) - _div(y, 100)
                   + _div(y, 400) - 32045 - 2440588, 32)


def _local_to_utc_seconds(seconds):
    # Inspect both sides of a possible transition. In an overlap both offsets
    # are valid: Java chooses the earlier instant. In a gap neither is valid:
    # applying the earlier offset shifts the wall clock forward through the gap.
    offsets = {_local_offset(seconds + delta) for delta in (-172800, 0, 172800)}
    candidates = [seconds - offset for offset in offsets]
    valid = [utc for utc in candidates if utc + _local_offset(utc) == seconds]
    return min(valid) if valid else max(candidates)
