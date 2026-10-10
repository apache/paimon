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
from typing import Optional

from pypaimon.management.java_string import is_blank
from pypaimon.management.list_permissions_request import \
    ListPermissionsRequest
from pypaimon.management.permission_assignment import PermissionAssignment
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.policy_type import PolicyType


class ListPoliciesRequest:

    def __init__(self,
                 resource: PermissionResource,
                 policy_type: Optional[PolicyType] = None,
                 principal: Optional[str] = None,
                 column: Optional[str] = None,
                 page_token: Optional[str] = None,
                 max_results: Optional[int] = None):
        if resource is None:
            raise ValueError("resource cannot be null")
        resource.validate_policy_attachment()
        if not is_blank(principal):
            PermissionAssignment.validate_principal(principal)
        if max_results is not None and max_results <= 0:
            raise ValueError("maxResults must be greater than 0.")
        if max_results is not None and max_results > ListPermissionsRequest.MAX_PAGE_SIZE:
            raise ValueError(
                "maxResults must be at most {}.".format(ListPermissionsRequest.MAX_PAGE_SIZE))
        if not is_blank(column) and policy_type != PolicyType.COLUMN_MASKING:
            raise ValueError("column filter requires type COLUMN_MASKING.")
        self._resource = resource
        self._type = policy_type
        self._principal = None if is_blank(principal) else principal
        self._column = None if is_blank(column) else column
        self._page_token = page_token
        self._max_results = max_results

    def get_resource(self) -> PermissionResource:
        return self._resource

    def get_type(self) -> Optional[PolicyType]:
        return self._type

    def get_principal(self) -> Optional[str]:
        return self._principal

    def get_column(self) -> Optional[str]:
        return self._column

    def get_page_token(self) -> Optional[str]:
        return self._page_token

    def get_max_results(self) -> Optional[int]:
        return self._max_results

    def with_page_token(self, new_page_token: Optional[str]) -> "ListPoliciesRequest":
        return ListPoliciesRequest(self._resource, self._type, self._principal, self._column,
                                   new_page_token, self._max_results)
