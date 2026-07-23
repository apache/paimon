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

import re
from decimal import Decimal as BigDecimal
from decimal import ROUND_HALF_UP
from decimal import localcontext
from typing import Optional, Tuple

_DECIMAL_PATTERN = re.compile(
    r"^(DECIMAL|NUMERIC|DEC)(?:\((\d+)(?:,\s*(\d+))?\))?$",
    re.IGNORECASE,
)


class Decimal:
    """
    An internal data structure representing data of DecimalType.

    This data structure is immutable and might store decimal values in a
    compact representation (as an integer value) if values are small enough.

    Python implementation note:
        ``decimal_val`` is an instance of Python's standard library
        ``decimal.Decimal`` (imported as ``BigDecimal``), corresponding to
        Java's ``java.math.BigDecimal``.
    """

    # Maximum number of decimal digits a long integer can represent.
    # (1e18 < Long.MAX_VALUE < 1e19)
    MAX_LONG_DIGITS = 18

    MAX_COMPACT_PRECISION = 18

    # Powers of 10 used for compact decimal conversion.
    POW10 = tuple(10 ** i for i in range(MAX_COMPACT_PRECISION + 1))

    # The semantics of the fields are as follows:
    #
    # - precision and scale represent the SQL decimal type.
    # - If decimal_val is set, it stores the complete decimal value.
    # - Otherwise, the decimal value is represented by
    #     long_val / (10 ** scale).
    #
    # Note that (precision, scale) must always be correct.
    #
    # If precision > MAX_COMPACT_PRECISION:
    #     decimal_val stores the value and long_val is undefined.
    # Otherwise:
    #     (long_val, scale) represents the value and decimal_val may be
    #     lazily initialized and cached.
    def __init__(
            self,
            precision: int,
            scale: int,
            long_val: int,
            decimal_val: Optional[BigDecimal],
    ):
        self.precision = precision
        self.scale = scale
        self.long_val = long_val
        self.decimal_val = decimal_val

    # ----------------------------------------------------------------------
    # Public Interfaces
    # ----------------------------------------------------------------------

    def to_big_decimal(self) -> BigDecimal:
        """
        Converts this Decimal into a decimal.Decimal instance.
        """
        if self.decimal_val is None:
            self.decimal_val = BigDecimal(self.long_val).scaleb(-self.scale)
        return self.decimal_val

    def to_unscaled_long(self) -> int:
        """
        Returns the unscaled integer value of this Decimal.

        Raises:
            ArithmeticError:
                If this Decimal does not exactly fit into a long integer.
        """
        if self.is_compact():
            return self.long_val

        value = self.to_big_decimal().scaleb(self.scale)

        if value != value.to_integral_exact():
            raise ArithmeticError("Decimal does not exactly fit in long.")

        int_val = int(value)
        if not (-(1 << 63) <= int_val <= (1 << 63) - 1):
            raise ArithmeticError("BigInteger out of long range")

        return int_val

    def to_unscaled_bytes(self) -> bytes:
        """
        Returns the unscaled value encoded as a signed byte array.
        """
        value = self.to_big_decimal().scaleb(self.scale)

        if value != value.to_integral_exact():
            raise ArithmeticError("Decimal does not exactly fit in long.")

        unscaled = int(value)
        length = max(1, (unscaled.bit_length() + 8) // 8)
        return unscaled.to_bytes(length, byteorder="big", signed=True)

    def is_compact(self) -> bool:
        """
        Returns whether the decimal value is small enough to be stored in a long integer.
        """
        return self.precision <= self.MAX_COMPACT_PRECISION

    def copy(self) -> "Decimal":
        """
        Returns a copy of this Decimal.
        """
        return Decimal(
            self.precision,
            self.scale,
            self.long_val,
            self.decimal_val,
        )

    @staticmethod
    def extract_decimal_precision_scale(type_str: str) -> Tuple[int, int]:
        """
        Extracts the precision and scale from a DECIMAL/NUMERIC/DEC type string.

        Examples:
            DECIMAL -> (10, 0)
            DECIMAL(10) -> (10, 0)
            DECIMAL(10,2) -> (10, 2)
            NUMERIC(20,5) -> (20, 5)
            DEC(18,6) -> (18, 6)
        """
        match = _DECIMAL_PATTERN.fullmatch(type_str.strip())
        if match is None:
            raise ValueError(f"Invalid decimal type: {type_str}")

        precision = match.group(2)
        scale = match.group(3)

        if precision is None:
            return 10, 0

        if scale is None:
            return int(precision), 0

        return int(precision), int(scale)

    # ----------------------------------------------------------------------
    # Comparison
    # ----------------------------------------------------------------------

    def __eq__(self, other):
        if not isinstance(other, Decimal):
            return False
        return self.compare_to(other) == 0

    def __lt__(self, other):
        return self.compare_to(other) < 0

    def compare_to(self, other: "Decimal") -> int:
        """
        Compares this Decimal with another Decimal.
        """
        if (
                self.is_compact()
                and other.is_compact()
                and self.scale == other.scale
        ):
            if self.long_val < other.long_val:
                return -1
            if self.long_val > other.long_val:
                return 1
            return 0

        a = self.to_big_decimal()
        b = other.to_big_decimal()

        if a < b:
            return -1
        if a > b:
            return 1
        return 0

    def __hash__(self):
        return hash(self.to_big_decimal())

    def __str__(self):
        return format(self.to_big_decimal(), "f")

    # ----------------------------------------------------------------------
    # Constructor Utilities
    # ----------------------------------------------------------------------

    @classmethod
    def from_big_decimal(
            cls,
            value: BigDecimal,
            precision: int,
            scale: int,
    ) -> Optional["Decimal"]:
        """
        Creates a ``Decimal`` from a Python's built-in ``decimal.Decimal`` with the given precision and scale.

        The value is rounded to the requested scale using ROUND_HALF_UP.
        If the resulting precision exceeds the specified precision, None is returned.

        Note:
            Java ``BigDecimal`` provides arbitrary precision. Paimon supports
            DECIMAL precision up to 38 digits, so a temporary context with
            precision >= 38 is used here to match Java semantics without
            modifying the global decimal context.
        """

        quant = BigDecimal(1).scaleb(-scale)

        with localcontext() as ctx:
            # Java BigDecimal is arbitrary precision.
            # Paimon DECIMAL supports precision up to 38.
            ctx.prec = max(
                precision + scale,
                len(value.as_tuple().digits) + abs(value.as_tuple().exponent) + scale,
                38,
            )

            value = value.quantize(
                quant,
                rounding=ROUND_HALF_UP,
            )

        digits = len(value.as_tuple().digits)

        if digits > precision:
            return None

        long_val = -1

        if precision <= cls.MAX_COMPACT_PRECISION:
            unscaled = value.scaleb(scale)

            if unscaled != unscaled.to_integral_exact():
                raise ArithmeticError(
                    "Decimal does not exactly fit in long."
                )

            long_val = int(unscaled)

        return cls(
            precision,
            scale,
            long_val,
            value,
        )

    @classmethod
    def from_unscaled_long(
            cls,
            unscaled_long: int,
            precision: int,
            scale: int,
    ) -> "Decimal":
        """
        Creates a Decimal from an unscaled integer value with the given precision and scale.
        """
        if precision <= 0 or precision > cls.MAX_LONG_DIGITS:
            raise ValueError(
                "precision must be between 1 and {}".format(
                    cls.MAX_LONG_DIGITS
                )
            )

        return cls(
            precision,
            scale,
            unscaled_long,
            None,
        )

    @classmethod
    def from_unscaled_bytes(
            cls,
            unscaled_bytes: bytes,
            precision: int,
            scale: int,
    ) -> Optional["Decimal"]:
        """
        Creates a Decimal from an unscaled signed byte array.
        """
        value = int.from_bytes(
            unscaled_bytes,
            byteorder="big",
            signed=True,
        )

        bd = BigDecimal(value).scaleb(-scale)

        return cls.from_big_decimal(
            bd,
            precision,
            scale,
        )

    @classmethod
    def zero(cls, precision: int, scale: int) -> Optional["Decimal"]:
        """
        Creates a Decimal representing zero with the given precision and scale.

        If the precision exceeds the supported range, None is returned.
        """
        if precision <= cls.MAX_COMPACT_PRECISION:
            return cls(
                precision,
                scale,
                0,
                None,
            )

        return cls.from_big_decimal(
            BigDecimal(0),
            precision,
            scale,
        )

    @staticmethod
    def is_compact_precision(precision: int) -> bool:
        """
        Returns whether the specified precision can be stored in compact form.
        """
        return precision <= Decimal.MAX_COMPACT_PRECISION
