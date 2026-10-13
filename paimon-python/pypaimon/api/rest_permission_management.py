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
from pypaimon.api.api_response import PagedList
from pypaimon.management.list_permissions_request import \
    ListPermissionsRequest
from pypaimon.management.permission_assignment import PermissionAssignment
from pypaimon.management.permission_management import PermissionManagement
from pypaimon.management.permission_resource import PermissionResource


class RESTPermissionManagement(PermissionManagement):

    def __init__(self, api):
        self.api = api

    def list_permissions(self, request: ListPermissionsRequest) -> PagedList[PermissionAssignment]:
        response = self.api.list_permissions(request)
        return PagedList(response.get_permissions(), response.get_next_page_token())

    def grant_permission(self, assignment: PermissionAssignment) -> None:
        self.api.grant_permission(assignment)

    def revoke_permission(self, resource: PermissionResource, access: str, principal: str) -> None:
        self.api.revoke_permission(resource, access, principal)
