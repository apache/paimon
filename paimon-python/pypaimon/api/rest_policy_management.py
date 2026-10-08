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

from pypaimon.api.api_response import ErrorResponse, PagedList
from pypaimon.api.rest_exception import AlreadyExistsException
from pypaimon.management.data_policy import DataPolicy
from pypaimon.management.list_policies_request import ListPoliciesRequest
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.policy_management import (PolicyAlreadyExistException,
                                                   PolicyManagement)
from pypaimon.management.policy_type import PolicyType


class RESTPolicyManagement(PolicyManagement):

    def __init__(self, api):
        self.api = api

    def list_policies(self, request: ListPoliciesRequest) -> PagedList[DataPolicy]:
        response = self.api.list_policies(request)
        return PagedList(response.get_policies(), response.get_next_page_token())

    def create_policy(self, policy: DataPolicy) -> None:
        try:
            self.api.create_policy(policy)
        except AlreadyExistsException as e:
            if e.resource_type == ErrorResponse.RESOURCE_TYPE_POLICY:
                raise PolicyAlreadyExistException(policy) from e
            raise

    def drop_policy(self, resource: PermissionResource, policy_type: PolicyType, principal: str,
                    column: Optional[str], ignore_if_not_exists: bool) -> None:
        self.api.drop_policy(resource, policy_type, principal, column, ignore_if_not_exists)
