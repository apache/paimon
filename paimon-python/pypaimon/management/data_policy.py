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
from typing import Any, Dict, Optional

from pypaimon.management.column_mask import ColumnMask
from pypaimon.management.permission_assignment import PermissionAssignment
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.policy_type import PolicyType
from pypaimon.management.row_filter import RowFilter


class DataPolicy:
    FIELD_RESOURCE = "resource"
    FIELD_ROW_FILTER = "rowFilter"
    FIELD_COLUMN_MASK = "columnMask"
    FIELD_PRINCIPAL = "principal"

    def __init__(self,
                 resource: PermissionResource,
                 row_filter: Optional[RowFilter],
                 column_mask: Optional[ColumnMask],
                 principal: str):
        if resource is None:
            raise ValueError("resource cannot be null")
        resource.validate_policy_attachment()
        if (row_filter is None) == (column_mask is None):
            raise ValueError("A policy must contain exactly one of rowFilter and columnMask.")
        self._resource = resource
        self._row_filter = row_filter
        self._column_mask = column_mask
        self._principal = PermissionAssignment.validate_principal(principal)

    @classmethod
    def row_filter(cls, resource: PermissionResource, row_filter: RowFilter,
                   principal: str) -> "DataPolicy":
        return cls(resource, row_filter, None, principal)

    @classmethod
    def column_mask(cls, resource: PermissionResource, column_mask: ColumnMask,
                    principal: str) -> "DataPolicy":
        return cls(resource, None, column_mask, principal)

    def get_resource(self) -> PermissionResource:
        return self._resource

    def get_row_filter(self) -> Optional[RowFilter]:
        return self._row_filter

    def get_column_mask(self) -> Optional[ColumnMask]:
        return self._column_mask

    def type(self) -> PolicyType:
        return PolicyType.COLUMN_MASKING if self._row_filter is None else PolicyType.ROW_FILTER

    def get_principal(self) -> str:
        return self._principal

    def to_dict(self) -> Dict[str, Any]:
        result = {self.FIELD_RESOURCE: self._resource.to_dict()}
        if self._row_filter is not None:
            result[self.FIELD_ROW_FILTER] = self._row_filter.to_dict()
        if self._column_mask is not None:
            result[self.FIELD_COLUMN_MASK] = self._column_mask.to_dict()
        result[self.FIELD_PRINCIPAL] = self._principal
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataPolicy":
        resource = data.get(cls.FIELD_RESOURCE)
        row_filter = data.get(cls.FIELD_ROW_FILTER)
        column_mask = data.get(cls.FIELD_COLUMN_MASK)
        return cls(None if resource is None else PermissionResource.from_dict(resource),
                   None if row_filter is None else RowFilter.from_dict(row_filter),
                   None if column_mask is None else ColumnMask.from_dict(column_mask),
                   data.get(cls.FIELD_PRINCIPAL))
