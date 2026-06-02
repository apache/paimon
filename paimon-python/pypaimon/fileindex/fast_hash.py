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

"""Fast hash functions for bloom filter file index."""

from typing import Callable
import calendar
import struct
from datetime import date, datetime, time

try:
    import xxhash
    HAS_XXHASH = True
except ImportError:
    HAS_XXHASH = False

from pypaimon.schema.data_types import DataType

_EPOCH_DATE = date(1970, 1, 1)


def _to_signed_64(value: int) -> int:
    """Reinterpret the low 64 bits of ``value`` as a signed 64-bit integer.

    This mirrors Java's silent overflow on ``long`` arithmetic: every ``+`` and
    ``<<`` wraps modulo 2**64. Applying this after each step keeps intermediate
    values in the same domain as the JVM, and because Python's ``>>`` on a
    negative int is already an arithmetic (sign-extending) shift, the right
    shifts then match Java's ``>>`` on a signed long.
    """
    value &= 0xFFFFFFFFFFFFFFFF
    if value >= 0x8000000000000000:
        value -= 0x10000000000000000
    return value


def get_long_hash(key: int) -> int:
    """Thomas Wang's 64-bit integer hash function (matches Java FastHash.getLongHash).

    Java performs this entirely in signed 64-bit ``long`` arithmetic, so each
    operation must be truncated to 64 bits to reproduce its overflow behavior;
    otherwise the subsequent arithmetic right shifts see high bits the JVM never
    has and the hash diverges (e.g. for key=2**40).
    """
    key = _to_signed_64(key)

    key = _to_signed_64((~key) + _to_signed_64(key << 21))
    key = _to_signed_64(key ^ (key >> 24))
    key = _to_signed_64((key + _to_signed_64(key << 3)) + _to_signed_64(key << 8))
    key = _to_signed_64(key ^ (key >> 14))
    key = _to_signed_64((key + _to_signed_64(key << 2)) + _to_signed_64(key << 4))
    key = _to_signed_64(key ^ (key >> 28))
    key = _to_signed_64(key + _to_signed_64(key << 31))

    # Return as unsigned 64-bit
    return key & 0xFFFFFFFFFFFFFFFF


def hash64_bytes(data: bytes) -> int:
    """Hash bytes using xxhash."""
    if not HAS_XXHASH:
        raise RuntimeError("xxhash library is required for file index. Install with: pip install xxhash")
    return xxhash.xxh64(data).intdigest()


def _parse_base_type(type_str: str):
    """Split a type string into (base name, precision).

    Examples: ``"INT"`` -> ``("INT", None)``, ``"VARCHAR(10)"`` -> ``("VARCHAR", 10)``,
    ``"TIMESTAMP(6) NOT NULL"`` -> ``("TIMESTAMP", 6)``. Matching the parsed base
    name (rather than substring containment) avoids collisions such as ``"TIME"``
    being found inside ``"TIMESTAMP"`` or ``"INT"`` inside ``"BIGINT"``.
    """
    s = type_str.upper().strip()
    if s.endswith(" NOT NULL"):
        s = s[: -len(" NOT NULL")].strip()
    precision = None
    if "(" in s:
        base, _, rest = s.partition("(")
        base = base.strip()
        digits = rest.split(")")[0].split(",")[0].strip()
        if digits.isdigit():
            precision = int(digits)
    else:
        base = s
    return base, precision


def _truncate_signed(value: int, bits: int) -> int:
    """Reinterpret the low ``bits`` of ``value`` as a signed integer.

    Mirrors Java's narrowing cast ``(byte)``/``(short)`` applied before hashing
    TINYINT/SMALLINT values.
    """
    mask = (1 << bits) - 1
    value &= mask
    sign_bit = 1 << (bits - 1)
    if value >= sign_bit:
        value -= (1 << bits)
    return value


def _date_to_epoch_day(value) -> int:
    """Convert a Python ``date``/``datetime`` to Java's DATE internal repr (epoch day)."""
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return (value - _EPOCH_DATE).days
    # Already an int (epoch day) -> pass through.
    return int(value)


def _time_to_millis_of_day(value) -> int:
    """Convert a Python ``time`` to Java's TIME internal repr (millis of day)."""
    if isinstance(value, time):
        return (value.hour * 3600 + value.minute * 60 + value.second) * 1000 + value.microsecond // 1000
    # Already an int (millis of day) -> pass through.
    return int(value)


def _timestamp_hash(value, precision) -> int:
    """Hash a timestamp value, mirroring Java: millis for precision<=3, else micros.

    Accepts pypaimon ``Timestamp`` objects (``get_millisecond``/``to_micros``) as
    well as native ``datetime``. Raw ints are rejected, because the correct unit
    (millis vs micros) cannot be inferred from an int alone and guessing would
    silently hash the wrong value (and could drop rows).
    """
    if value is None:
        return 0

    use_millis = precision is not None and precision <= 3

    if hasattr(value, 'get_millisecond') and hasattr(value, 'to_micros'):
        return get_long_hash(value.get_millisecond() if use_millis else value.to_micros())

    if isinstance(value, datetime):
        # A timezone-aware datetime carries an instant, but ``timetuple()``
        # exposes only the wall-clock fields and silently drops the offset, so
        # hashing it would match the wrong instant (e.g. 1970-01-01T00:00+08:00
        # would hash as epoch 0 instead of -28800s) and could false-SKIP a file.
        # Java's TIMESTAMP is offset-free; proper TIMESTAMP_LTZ handling (UTC
        # normalization) is out of scope here, so refuse aware values and let
        # the caller fail open.
        if value.tzinfo is not None:
            raise TypeError(
                "Cannot hash a timezone-aware datetime for file index; "
                "TIMESTAMP_LTZ pushdown is not supported"
            )
        epoch_seconds = calendar.timegm(value.timetuple())
        millis = epoch_seconds * 1000 + value.microsecond // 1000
        if use_millis:
            return get_long_hash(millis)
        micros = epoch_seconds * 1_000_000 + value.microsecond
        return get_long_hash(micros)

    raise TypeError(
        f"Cannot hash timestamp literal of type {type(value).__name__}; "
        "expected a Timestamp or datetime"
    )


def get_hash_function(data_type: DataType) -> Callable:
    """Get appropriate hash function for the given data type.

    Dispatch mirrors Java's ``FastHash.FastHashVisitor``: string/char via xxhash,
    binary via xxhash, integral/date/time via Thomas Wang long hash, float/double
    via their IEEE-754 bit patterns, and timestamps via millis or micros depending
    on precision.
    """
    base, precision = _parse_base_type(str(data_type))

    # String types
    if base in ('VARCHAR', 'CHAR', 'STRING'):
        def hash_fn(value):
            if value is None:
                return 0
            if isinstance(value, str):
                return hash64_bytes(value.encode('utf-8'))
            return hash64_bytes(value)
        return hash_fn

    # Binary types
    if base in ('BINARY', 'VARBINARY', 'BYTES'):
        def hash_fn(value):
            if value is None:
                return 0
            return hash64_bytes(value)
        return hash_fn

    # Float type (hash the 32-bit IEEE-754 representation, like Float.floatToIntBits)
    if base == 'FLOAT':
        def hash_fn(value):
            if value is None:
                return 0
            float_bits = struct.unpack('>i', struct.pack('>f', float(value)))[0]
            return get_long_hash(float_bits)
        return hash_fn

    # Double type (hash the 64-bit IEEE-754 representation, like Double.doubleToLongBits)
    if base == 'DOUBLE':
        def hash_fn(value):
            if value is None:
                return 0
            double_bits = struct.unpack('>q', struct.pack('>d', float(value)))[0]
            return get_long_hash(double_bits)
        return hash_fn

    # Timestamp types (precision decides millis vs micros, matching Java)
    if base in ('TIMESTAMP', 'TIMESTAMP_LTZ'):
        def hash_fn(value):
            return _timestamp_hash(value, precision)
        return hash_fn

    # Date type (Java internal repr: epoch day as int)
    if base == 'DATE':
        def hash_fn(value):
            if value is None:
                return 0
            return get_long_hash(_date_to_epoch_day(value))
        return hash_fn

    # Time type (Java internal repr: millis of day as int)
    if base == 'TIME':
        def hash_fn(value):
            if value is None:
                return 0
            return get_long_hash(_time_to_millis_of_day(value))
        return hash_fn

    # Byte / short integral types: Java casts to (byte)/(short) before hashing,
    # i.e. sign-truncates to 8/16 bits. In-range pyarrow literals are unaffected;
    # this only matters for out-of-range literals, where it keeps us bit-for-bit
    # consistent with the Java writer.
    if base == 'TINYINT':
        def hash_fn(value):
            if value is None:
                return 0
            return get_long_hash(_truncate_signed(int(value), 8))
        return hash_fn

    if base == 'SMALLINT':
        def hash_fn(value):
            if value is None:
                return 0
            return get_long_hash(_truncate_signed(int(value), 16))
        return hash_fn

    # Remaining integral types (Java hashes the int/long value directly)
    if base in ('INT', 'INTEGER', 'BIGINT'):
        def hash_fn(value):
            if value is None:
                return 0
            return get_long_hash(int(value))
        return hash_fn

    # Default fallback
    raise ValueError(f"Unsupported data type for file index: {data_type}")
