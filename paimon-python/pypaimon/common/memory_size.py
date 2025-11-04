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

"""
MemorySize is a representation of a number of bytes, viewable in different units.

Parsing:
The size can be parsed from a text expression. If the expression is a pure number,
the value will be interpreted as bytes.

Supported formats:
- 1b or 1bytes (bytes)
- 1k or 1kb or 1kibibytes (interpreted as kibibytes = 1024 bytes)
- 1m or 1mb or 1mebibytes (interpreted as mebibytes = 1024 kibibytes)
- 1g or 1gb or 1gibibytes (interpreted as gibibytes = 1024 mebibytes)
- 1t or 1tb or 1tebibytes (interpreted as tebibytes = 1024 gibibytes)
"""

import re


class MemorySize:
    """MemorySize is a representation of a number of bytes, viewable in different units."""

    ZERO = None  # Will be set after class definition
    MAX_VALUE = None  # Will be set after class definition

    def __init__(self, bytes: int):
        """
        Constructs a new MemorySize.

        Args:
            bytes: The size, in bytes. Must be zero or larger.
        """
        if bytes < 0:
            raise ValueError("bytes must be >= 0")
        self.bytes = bytes

    @staticmethod
    def of_mebi_bytes(mebi_bytes: int) -> 'MemorySize':
        """Create a MemorySize from mebibytes."""
        return MemorySize(mebi_bytes << 20)

    @staticmethod
    def of_kibi_bytes(kibi_bytes: int) -> 'MemorySize':
        """Create a MemorySize from kibibytes."""
        return MemorySize(kibi_bytes << 10)

    @staticmethod
    def of_bytes(bytes: int) -> 'MemorySize':
        """Create a MemorySize from bytes."""
        return MemorySize(bytes)

    def get_bytes(self) -> int:
        """Gets the memory size in bytes."""
        return self.bytes

    def get_kibi_bytes(self) -> int:
        """Gets the memory size in Kibibytes (= 1024 bytes)."""
        return self.bytes >> 10

    def get_mebi_bytes(self) -> int:
        """Gets the memory size in Mebibytes (= 1024 Kibibytes)."""
        return self.bytes >> 20

    def get_gibi_bytes(self) -> int:
        """Gets the memory size in Gibibytes (= 1024 Mebibytes)."""
        return self.bytes >> 30

    def get_tebi_bytes(self) -> int:
        """Gets the memory size in Tebibytes (= 1024 Gibibytes)."""
        return self.bytes >> 40

    def __eq__(self, other) -> bool:
        """Check equality with another MemorySize."""
        if not isinstance(other, MemorySize):
            return False
        return self.bytes == other.bytes

    def __hash__(self) -> int:
        """Compute hash code."""
        return hash(self.bytes)

    def __str__(self) -> str:
        """String representation."""
        return self.format_to_string()

    def format_to_string(self) -> str:
        """Format to string representation."""
        # Find the highest unit that divides evenly
        if self.bytes == 0:
            return "0 bytes"

        for unit in [MemoryUnit.TERA_BYTES, MemoryUnit.GIGA_BYTES, MemoryUnit.MEGA_BYTES,
                     MemoryUnit.KILO_BYTES, MemoryUnit.BYTES]:
            if self.bytes % unit.multiplier == 0:
                value = self.bytes // unit.multiplier
                return f"{value} {unit.units[1]}"

        return f"{self.bytes} bytes"

    def __repr__(self) -> str:
        """Representation."""
        return f"MemorySize({self.bytes})"

    def __lt__(self, other: 'MemorySize') -> bool:
        """Less than comparison."""
        return self.bytes < other.bytes

    def __le__(self, other: 'MemorySize') -> bool:
        """Less than or equal comparison."""
        return self.bytes <= other.bytes

    def __gt__(self, other: 'MemorySize') -> bool:
        """Greater than comparison."""
        return self.bytes > other.bytes

    def __ge__(self, other: 'MemorySize') -> bool:
        """Greater than or equal comparison."""
        return self.bytes >= other.bytes

    @staticmethod
    def parse(text: str) -> 'MemorySize':
        """
        Parses the given string as a MemorySize.

        Args:
            text: The string to parse

        Returns:
            The parsed MemorySize

        Raises:
            ValueError: If the expression cannot be parsed.
        """
        return MemorySize(MemorySize.parse_bytes(text))

    @staticmethod
    def parse_bytes(text: str) -> int:
        """
        Parses the given string as bytes. The supported expressions are listed
        under MemorySize documentation.

        Args:
            text: The string to parse

        Returns:
            The parsed size, in bytes.

        Raises:
            ValueError: If the expression cannot be parsed.
        """
        if text is None:
            raise ValueError("text cannot be None")

        trimmed = text.strip()
        if not trimmed:
            raise ValueError("argument is an empty- or whitespace-only string")

        # Extract number and unit
        match = re.match(r'^(\d+)\s*([a-zA-Z]*)$', trimmed)
        if not match:
            raise ValueError(f"cannot parse memory size: '{text}'")

        number_str = match.group(1)
        unit_str = match.group(2).lower() if match.group(2) else ""

        try:
            value = int(number_str)
        except ValueError:
            raise ValueError(
                f"The value '{number_str}' cannot be represented as 64bit number (numeric overflow).")

        multiplier = MemoryUnit.parse_unit(unit_str).multiplier
        result = value * multiplier

        # Check for overflow
        if multiplier != 0 and result // multiplier != value:
            raise ValueError(
                f"The value '{text}' cannot be represented as 64bit number of bytes (numeric overflow).")

        return result


class MemoryUnit:
    """Enum which defines memory unit, mostly used to parse value from configuration file."""

    def __init__(self, units: list, multiplier: int):
        self.units = units
        self.multiplier = multiplier

    BYTES = None  # Will be set after class definition
    KILO_BYTES = None
    MEGA_BYTES = None
    GIGA_BYTES = None
    TERA_BYTES = None

    @staticmethod
    def parse_unit(unit_str: str) -> 'MemoryUnit':
        """
        Parse a unit string and return the corresponding MemoryUnit.

        Args:
            unit_str: The unit string (e.g., "kb", "mb", "bytes")

        Returns:
            The corresponding MemoryUnit, or BYTES if no unit specified

        Raises:
            ValueError: If the unit is not recognized
        """
        unit_str = unit_str.strip().lower()

        if not unit_str:
            return MemoryUnit.BYTES

        for unit in [MemoryUnit.BYTES, MemoryUnit.KILO_BYTES, MemoryUnit.MEGA_BYTES,
                     MemoryUnit.GIGA_BYTES, MemoryUnit.TERA_BYTES]:
            if unit_str in unit.units:
                return unit

        raise ValueError(
            f"Memory size unit '{unit_str}' does not match any of the recognized units: "
            f"{MemoryUnit.get_all_units()}")

    @staticmethod
    def get_all_units() -> str:
        """Get all recognized units as a string."""
        all_units = []
        for unit in [MemoryUnit.BYTES, MemoryUnit.KILO_BYTES, MemoryUnit.MEGA_BYTES,
                     MemoryUnit.GIGA_BYTES, MemoryUnit.TERA_BYTES]:
            all_units.append("(" + " | ".join(unit.units) + ")")
        return " / ".join(all_units)

    @staticmethod
    def has_unit(text: str) -> bool:
        """
        Check if the text contains a unit specification.

        Args:
            text: The text to check

        Returns:
            True if the text contains a unit, False otherwise
        """
        if text is None:
            raise ValueError("text cannot be None")

        trimmed = text.strip()
        if not trimmed:
            raise ValueError("argument is an empty- or whitespace-only string")

        # Check if there's a unit after the number
        match = re.match(r'^\d+\s*([a-zA-Z]+)$', trimmed)
        return match is not None and bool(match.group(1))


# Initialize MemoryUnit constants
MemoryUnit.BYTES = MemoryUnit(["b", "bytes"], 1)
MemoryUnit.KILO_BYTES = MemoryUnit(["k", "kb", "kibibytes"], 1024)
MemoryUnit.MEGA_BYTES = MemoryUnit(["m", "mb", "mebibytes"], 1024 * 1024)
MemoryUnit.GIGA_BYTES = MemoryUnit(["g", "gb", "gibibytes"], 1024 * 1024 * 1024)
MemoryUnit.TERA_BYTES = MemoryUnit(["t", "tb", "tebibytes"], 1024 * 1024 * 1024 * 1024)

# Initialize MemorySize constants
MemorySize.ZERO = MemorySize(0)
MemorySize.MAX_VALUE = MemorySize(2**63 - 1)  # Long.MAX_VALUE equivalent

