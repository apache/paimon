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

from datetime import datetime, timedelta


class Timestamp:
    """
    An internal data structure representing data of TimestampType.

    This data structure is immutable and consists of a milliseconds and nanos-of-millisecond since
    1970-01-01 00:00:00. It might be stored in a compact representation (as a long value) if
    values are small enough.

    This class represents timezone-free timestamps
    """

    # the number of milliseconds in a day
    MILLIS_PER_DAY = 86400000  # = 24 * 60 * 60 * 1000

    MICROS_PER_MILLIS = 1000
    NANOS_PER_MICROS = 1000

    NANOS_PER_HOUR = 3_600_000_000_000
    NANOS_PER_MINUTE = 60_000_000_000
    NANOS_PER_SECOND = 1_000_000_000
    NANOS_PER_MICROSECOND = 1_000

    def __init__(self, millisecond: int, nano_of_millisecond: int = 0):
        if not (0 <= nano_of_millisecond <= 999_999):
            raise ValueError(
                f"nano_of_millisecond must be between 0 and 999,999, got {nano_of_millisecond}"
            )
        self._millisecond = millisecond
        self._nano_of_millisecond = nano_of_millisecond

    def get_millisecond(self) -> int:
        """Returns the number of milliseconds since 1970-01-01 00:00:00."""
        return self._millisecond

    def get_nano_of_millisecond(self) -> int:
        """
        Returns the number of nanoseconds (the nanoseconds within the milliseconds).
        The value range is from 0 to 999,999.
        """
        return self._nano_of_millisecond

    def to_local_date_time(self) -> datetime:
        """Converts this Timestamp object to a datetime (timezone-free)."""
        epoch = datetime(1970, 1, 1)
        days = self._millisecond // self.MILLIS_PER_DAY
        time_millis = self._millisecond % self.MILLIS_PER_DAY
        if time_millis < 0:
            days -= 1
            time_millis += self.MILLIS_PER_DAY

        microseconds = time_millis * 1000 + self._nano_of_millisecond // 1000
        return epoch + timedelta(days=days, microseconds=microseconds)

    def to_millis_timestamp(self) -> 'Timestamp':
        return Timestamp.from_epoch_millis(self._millisecond)

    def to_micros(self) -> int:
        """Converts this Timestamp object to micros."""
        micros = self._millisecond * self.MICROS_PER_MILLIS
        return micros + self._nano_of_millisecond // self.NANOS_PER_MICROS

    def __eq__(self, other):
        if not isinstance(other, Timestamp):
            return False
        return (self._millisecond == other._millisecond and
                self._nano_of_millisecond == other._nano_of_millisecond)

    def __lt__(self, other):
        if not isinstance(other, Timestamp):
            return NotImplemented
        if self._millisecond != other._millisecond:
            return self._millisecond < other._millisecond
        return self._nano_of_millisecond < other._nano_of_millisecond

    def __le__(self, other):
        return self == other or self < other

    def __gt__(self, other):
        if not isinstance(other, Timestamp):
            return NotImplemented
        if self._millisecond != other._millisecond:
            return self._millisecond > other._millisecond
        return self._nano_of_millisecond > other._nano_of_millisecond

    def __ge__(self, other):
        return self == other or self > other

    def __hash__(self):
        return hash((self._millisecond, self._nano_of_millisecond))

    def __repr__(self):
        return f"Timestamp(millisecond={self._millisecond}, nano_of_millisecond={self._nano_of_millisecond})"

    def __str__(self):
        return self.to_local_date_time().strftime("%Y-%m-%d %H:%M:%S.%f")

    @staticmethod
    def now() -> 'Timestamp':
        """Creates an instance of Timestamp for now."""
        return Timestamp.from_local_date_time(datetime.now())

    @staticmethod
    def from_epoch_millis(milliseconds: int, nanos_of_millisecond: int = 0) -> 'Timestamp':
        """
        Creates an instance of Timestamp from milliseconds.
        Args:
            milliseconds: the number of milliseconds since 1970-01-01 00:00:00
            nanos_of_millisecond: the nanoseconds within the millisecond, from 0 to 999,999
        """
        return Timestamp(milliseconds, nanos_of_millisecond)

    @staticmethod
    def from_local_date_time(date_time: datetime) -> 'Timestamp':
        """
        Creates an instance of Timestamp from a datetime (timezone-free).

        Args:
            date_time: a datetime object (should be naive, without timezone)
        """
        if date_time.tzinfo is not None:
            raise ValueError("datetime must be naive (no timezone)")

        epoch_date = datetime(1970, 1, 1).date()
        date_time_date = date_time.date()

        epoch_day = (date_time_date - epoch_date).days
        time_part = date_time.time()

        nano_of_day = (
            time_part.hour * Timestamp.NANOS_PER_HOUR
            + time_part.minute * Timestamp.NANOS_PER_MINUTE
            + time_part.second * Timestamp.NANOS_PER_SECOND
            + time_part.microsecond * Timestamp.NANOS_PER_MICROSECOND
        )

        millisecond = epoch_day * Timestamp.MILLIS_PER_DAY + nano_of_day // 1_000_000
        nano_of_millisecond = int(nano_of_day % 1_000_000)

        return Timestamp(millisecond, nano_of_millisecond)

    @staticmethod
    def from_micros(micros: int) -> 'Timestamp':
        """Creates an instance of Timestamp from micros."""
        mills = micros // Timestamp.MICROS_PER_MILLIS
        nanos = (micros - mills * Timestamp.MICROS_PER_MILLIS) * Timestamp.NANOS_PER_MICROS
        return Timestamp.from_epoch_millis(mills, int(nanos))
