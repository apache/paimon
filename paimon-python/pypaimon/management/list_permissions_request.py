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
from pypaimon.management.permission_access import PermissionAccess
from pypaimon.management.permission_assignment import PermissionAssignment
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.resource_type import ResourceType


class ListPermissionsRequest:

    MAX_PAGE_SIZE = 1000

    def __init__(self,
                 resource_type: ResourceType,
                 database: Optional[str] = None,
                 table: Optional[str] = None,
                 function: Optional[str] = None,
                 view: Optional[str] = None,
                 principal: Optional[str] = None,
                 access: Optional[str] = None,
                 page_token: Optional[str] = None,
                 max_results: Optional[int] = None):
        self._resource = _exact_resource(resource_type, database, table, function, view)
        if not is_blank(principal):
            PermissionAssignment.validate_principal(principal)
        if max_results is not None and max_results <= 0:
            raise ValueError("maxResults must be greater than 0.")
        if max_results is not None and max_results > self.MAX_PAGE_SIZE:
            raise ValueError("maxResults must be at most {}.".format(self.MAX_PAGE_SIZE))
        self._principal = None if is_blank(principal) else principal
        self._access = (None if is_blank(access)
                        else PermissionAccess.canonicalize_for(self._resource, access))
        self._page_token = page_token
        self._max_results = max_results

    def get_resource_type(self) -> ResourceType:
        return self._resource.get_type()

    def get_database(self) -> Optional[str]:
        return self._resource.get_database()

    def get_table(self) -> Optional[str]:
        return self._resource.get_table()

    def get_function(self) -> Optional[str]:
        return self._resource.get_function()

    def get_view(self) -> Optional[str]:
        return self._resource.get_view()

    def get_principal(self) -> Optional[str]:
        return self._principal

    def get_access(self) -> Optional[str]:
        return self._access

    def get_page_token(self) -> Optional[str]:
        return self._page_token

    def get_max_results(self) -> Optional[int]:
        return self._max_results

    def resource(self) -> PermissionResource:
        return self._resource

    def with_page_token(self, new_page_token: Optional[str]) -> "ListPermissionsRequest":
        resource = self._resource
        return ListPermissionsRequest(
            resource.get_type(), resource.get_database(), resource.get_table(),
            resource.get_function(), resource.get_view(), self._principal, self._access,
            new_page_token, self._max_results)


def _exact_resource(resource_type, database, table, function, view) -> PermissionResource:
    if resource_type is None:
        raise ValueError("resourceType cannot be null")
    try:
        return PermissionResource(resource_type, database, table, function, view)
    except ValueError as e:
        raise ValueError(
            "Permission listing requires an exact target resource: {}".format(e)) from e
