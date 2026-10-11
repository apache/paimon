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
import re
from typing import Any, Dict, Optional

from pypaimon.management.java_string import is_blank, java_length
from pypaimon.management.permission_access import PermissionAccess
from pypaimon.management.permission_columns import PermissionColumns
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.resource_type import ResourceType

# Java Instant.parse on JDK 8/11 (ISO_INSTANT): case-insensitive, ASCII digits, Z offset only.
_INSTANT = re.compile(
    r"([+-]?)([0-9]{4,10})-([0-9]{2})-([0-9]{2})[Tt]([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(?:\.([0-9]{0,9}))?[Zz]")
# Instant.MIN and Instant.MAX, in epoch seconds.
_MIN_SECOND = -31557014167219200
_MAX_SECOND = 31556889864403199


class PermissionAssignment:
    MAX_PRINCIPAL_LENGTH = 128

    FIELD_RESOURCE = "resource"
    FIELD_ACCESS = "access"
    FIELD_PRINCIPAL = "principal"
    FIELD_COLUMNS = "columns"
    FIELD_EXPIRE_TIME = "expireTime"

    def __init__(self,
                 resource: PermissionResource,
                 access: str,
                 principal: str,
                 columns: Optional[PermissionColumns] = None,
                 expire_time: Optional[str] = None):
        if resource is None:
            raise ValueError("resource cannot be null")
        self._resource = resource
        self._access = PermissionAccess.canonicalize_for(resource, access)
        self._principal = PermissionAssignment.validate_principal(principal)
        if resource.get_type() == ResourceType.COLUMN:
            if columns is None:
                raise ValueError("columns is required for COLUMN resource.")
        elif columns is not None:
            raise ValueError("columns is only valid for COLUMN resource.")
        _validate_expire_time(expire_time)
        self._columns = columns
        self._expire_time = expire_time

    @staticmethod
    def validate_principal(principal: str) -> str:
        if is_blank(principal):
            raise ValueError("principal cannot be empty.")
        if java_length(principal) > PermissionAssignment.MAX_PRINCIPAL_LENGTH:
            raise ValueError("principal must contain at most {} characters.".format(
                PermissionAssignment.MAX_PRINCIPAL_LENGTH))
        return principal

    def get_resource(self) -> PermissionResource:
        return self._resource

    def get_access(self) -> str:
        return self._access

    def get_principal(self) -> str:
        return self._principal

    def get_columns(self) -> Optional[PermissionColumns]:
        return self._columns

    def get_expire_time(self) -> Optional[str]:
        return self._expire_time

    def to_dict(self) -> Dict[str, Any]:
        result = {
            self.FIELD_RESOURCE: None if self._resource is None else self._resource.to_dict(),
            self.FIELD_ACCESS: self._access,
            self.FIELD_PRINCIPAL: self._principal,
        }
        if self._columns is not None:
            result[self.FIELD_COLUMNS] = self._columns.to_dict()
        if self._expire_time is not None:
            result[self.FIELD_EXPIRE_TIME] = self._expire_time
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PermissionAssignment":
        """Unvalidated, like Java's Jackson constructor; the nested resource and columns validate."""
        resource = data.get(cls.FIELD_RESOURCE)
        columns = data.get(cls.FIELD_COLUMNS)
        expire_time = data.get(cls.FIELD_EXPIRE_TIME)
        # Java's (String) cast rejects any other type.
        if expire_time is not None and not isinstance(expire_time, str):
            raise TypeError("expireTime must be a string, got {!r}.".format(expire_time))
        assignment = cls.__new__(cls)
        assignment._resource = None if resource is None else PermissionResource.from_dict(resource)
        assignment._access = data.get(cls.FIELD_ACCESS)
        assignment._principal = data.get(cls.FIELD_PRINCIPAL)
        assignment._columns = None if columns is None else PermissionColumns.from_dict(columns)
        assignment._expire_time = expire_time
        return assignment


def _validate_expire_time(expire_time: Optional[str]):
    if expire_time is None:
        return
    nanos = _parse_instant(expire_time)
    if nanos is None:
        raise ValueError("expireTime must be an ISO-8601 UTC instant.")
    if nanos % 1_000_000 != 0:
        raise ValueError("expireTime must have at most millisecond precision.")


def _parse_instant(text: str) -> Optional[int]:
    """The nano-of-second of ``text`` if Java's Instant.parse accepts it, else None."""
    match = _INSTANT.fullmatch(text)
    if match is None:
        return None
    sign, digits = match.group(1), match.group(2)
    # Strict EXCEEDS_PAD: no -0000, and '+' exactly when there are more than four digits.
    if sign == "-":
        if int(digits) == 0:
            return None
    elif (sign == "+") != (len(digits) > 4):
        return None
    year = -int(digits) if sign == "-" else int(digits)
    month, day, hour, minute, second = (int(group) for group in match.group(3, 4, 5, 6, 7))
    nanos = int((match.group(8) or "").ljust(9, "0"))
    days = 0
    # ISO_INSTANT reads 24:00:00 as the next midnight and folds a leap second into :59.
    if (hour, minute, second, nanos) == (24, 0, 0, 0):
        hour, days = 0, 1
    elif (hour, minute, second) == (23, 59, 60):
        second = 59
    if not (1 <= month <= 12 and 1 <= day <= _days_in_month(year, month)
            and hour <= 23 and minute <= 59 and second <= 59):
        return None
    epoch_second = ((_days_from_civil(year, month, day) + days) * 86400
                    + hour * 3600 + minute * 60 + second)
    return nanos if _MIN_SECOND <= epoch_second <= _MAX_SECOND else None


def _days_in_month(year: int, month: int) -> int:
    if month == 2:
        return 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28
    return 30 if month in (4, 6, 9, 11) else 31


def _days_from_civil(year: int, month: int, day: int) -> int:
    """Days since 1970-01-01 in the proleptic Gregorian calendar, for any year."""
    year -= month <= 2
    era = year // 400
    year_of_era = year - era * 400
    day_of_year = (153 * (month + (-3 if month > 2 else 9)) + 2) // 5 + day - 1
    day_of_era = year_of_era * 365 + year_of_era // 4 - year_of_era // 100 + day_of_year
    return era * 146097 + day_of_era - 719468
