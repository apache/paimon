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
from abc import ABC, abstractmethod
from typing import Optional

from pypaimon.api.api_response import PagedList
from pypaimon.management.data_policy import DataPolicy
from pypaimon.management.list_policies_request import ListPoliciesRequest
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.policy_type import PolicyType


class PolicyManagement(ABC):

    @abstractmethod
    def list_policies(self, request: ListPoliciesRequest) -> PagedList[DataPolicy]:
        pass

    @abstractmethod
    def create_policy(self, policy: DataPolicy) -> None:
        pass

    @abstractmethod
    def drop_policy(self, resource: PermissionResource, policy_type: PolicyType, principal: str,
                    column: Optional[str], ignore_if_not_exists: bool) -> None:
        pass


class PolicyAlreadyExistException(Exception):

    def __init__(self, policy: DataPolicy):
        target = policy.type().name
        if policy.get_column_mask() is not None:
            target += "(" + policy.get_column_mask().get_on_column() + ")"
        resource = policy.get_resource()
        super().__init__("{} policy for principal '{}' already exists on table '{}.{}'.".format(
            target, policy.get_principal(), resource.get_database(), resource.get_table()))
        self._policy = policy

    def policy(self) -> DataPolicy:
        return self._policy
