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

from typing import Optional


class MemorySize:
    """MemorySize is a representation of a number of bytes, viewable in different units."""

    ZERO = None
    MAX_VALUE = None

    def __init__(self, bytes: int):
        """Constructs a new MemorySize."""
        if bytes < 0:
            raise ValueError("bytes must be >= 0")
        self.bytes = bytes

    @staticmethod
    def of_mebi_bytes(mebi_bytes: int) -> 'MemorySize':
        return MemorySize(mebi_bytes << 20)

    @staticmethod
    def of_kibi_bytes(kibi_bytes: int) -> 'MemorySize':
        return MemorySize(kibi_bytes << 10)

    @staticmethod
    def of_bytes(bytes: int) -> 'MemorySize':
        return MemorySize(bytes)

    def get_bytes(self) -> int:
        return self.bytes

    def get_kibi_bytes(self) -> int:
        return self.bytes >> 10

    def get_mebi_bytes(self) -> int:
        return self.bytes >> 20

    def get_gibi_bytes(self) -> int:
        return self.bytes >> 30

    def get_tebi_bytes(self) -> int:
        return self.bytes >> 40

    def __eq__(self, other) -> bool:
        return isinstance(other, MemorySize) and self.bytes == other.bytes

    def __hash__(self) -> int:
        return hash(self.bytes)

    def __str__(self) -> str:
        return self.format_to_string()

    def format_to_string(self) -> str:
        ORDERED_UNITS = [MemoryUnit.BYTES, MemoryUnit.KILO_BYTES, MemoryUnit.MEGA_BYTES,
                         MemoryUnit.GIGA_BYTES, MemoryUnit.TERA_BYTES]

        highest_integer_unit = MemoryUnit.BYTES
        for idx, unit in enumerate(ORDERED_UNITS):
            if self.bytes % unit.multiplier != 0:
                if idx == 0:
                    highest_integer_unit = ORDERED_UNITS[0]
                else:
                    highest_integer_unit = ORDERED_UNITS[idx - 1]
                break
        else:
            highest_integer_unit = MemoryUnit.BYTES

        return f"{self.bytes // highest_integer_unit.multiplier} {highest_integer_unit.units[1]}"

    def __repr__(self) -> str:
        return f"MemorySize({self.bytes})"

    def __lt__(self, other: 'MemorySize') -> bool:
        return self.bytes < other.bytes

    def __le__(self, other: 'MemorySize') -> bool:
        return self.bytes <= other.bytes

    def __gt__(self, other: 'MemorySize') -> bool:
        return self.bytes > other.bytes

    def __ge__(self, other: 'MemorySize') -> bool:
        return self.bytes >= other.bytes

    @staticmethod
    def parse(text: str) -> 'MemorySize':
        return MemorySize(MemorySize.parse_bytes(text))

    @staticmethod
    def parse_bytes(text: str) -> int:
        if text is None:
            raise ValueError("text cannot be None")

        trimmed = text.strip()
        if not trimmed:
            raise ValueError("argument is an empty- or whitespace-only string")

        pos = 0
        while pos < len(trimmed) and trimmed[pos].isdigit():
            pos += 1

        number_str = trimmed[:pos]
        unit_str = trimmed[pos:].strip().lower()

        if not number_str:
            raise ValueError("text does not start with a number")

        try:
            value = int(number_str)
        except ValueError:
            raise ValueError(
                f"The value '{number_str}' cannot be represented as 64bit number (numeric overflow).")

        unit = MemorySize._parse_unit(unit_str)
        multiplier = unit.multiplier if unit else 1
        result = value * multiplier

        if result // multiplier != value:
            raise ValueError(
                f"The value '{text}' cannot be represented as 64bit number of bytes (numeric overflow).")

        return result

    @staticmethod
    def _parse_unit(unit_str: str) -> Optional['MemoryUnit']:
        if not unit_str:
            return None

        for unit in [MemoryUnit.BYTES, MemoryUnit.KILO_BYTES, MemoryUnit.MEGA_BYTES,
                     MemoryUnit.GIGA_BYTES, MemoryUnit.TERA_BYTES]:
            if unit_str in unit.units:
                return unit

        raise ValueError(
            f"Memory size unit '{unit_str}' does not match any of the recognized units: "
            f"{MemoryUnit.get_all_units()}")


class MemoryUnit:
    """Enum which defines memory unit, mostly used to parse value from configuration file."""

    def __init__(self, units: list, multiplier: int):
        self.units = units
        self.multiplier = multiplier

    BYTES = None
    KILO_BYTES = None
    MEGA_BYTES = None
    GIGA_BYTES = None
    TERA_BYTES = None

    @staticmethod
    def get_all_units() -> str:
        all_units = []
        for unit in [MemoryUnit.BYTES, MemoryUnit.KILO_BYTES, MemoryUnit.MEGA_BYTES,
                     MemoryUnit.GIGA_BYTES, MemoryUnit.TERA_BYTES]:
            all_units.append("(" + " | ".join(unit.units) + ")")
        return " / ".join(all_units)

    @staticmethod
    def has_unit(text: str) -> bool:
        if text is None:
            raise ValueError("text cannot be None")

        trimmed = text.strip()
        if not trimmed:
            raise ValueError("argument is an empty- or whitespace-only string")

        pos = 0
        while pos < len(trimmed) and trimmed[pos].isdigit():
            pos += 1

        unit = trimmed[pos:].strip().lower()
        return len(unit) > 0


MemoryUnit.BYTES = MemoryUnit(["b", "bytes"], 1)
MemoryUnit.KILO_BYTES = MemoryUnit(["k", "kb", "kibibytes"], 1024)
MemoryUnit.MEGA_BYTES = MemoryUnit(["m", "mb", "mebibytes"], 1024 * 1024)
MemoryUnit.GIGA_BYTES = MemoryUnit(["g", "gb", "gibibytes"], 1024 * 1024 * 1024)
MemoryUnit.TERA_BYTES = MemoryUnit(["t", "tb", "tebibytes"], 1024 * 1024 * 1024 * 1024)

MemorySize.ZERO = MemorySize(0)
MemorySize.MAX_VALUE = MemorySize(2**63 - 1)
