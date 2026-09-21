/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.rest;

import org.apache.paimon.PagedList;
import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.management.DataPolicy;
import org.apache.paimon.management.ListPoliciesRequest;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.PolicyManagement;
import org.apache.paimon.management.PolicyManagement.PolicyAlreadyExistException;
import org.apache.paimon.management.PolicyType;
import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.ListPoliciesResponse;

import javax.annotation.Nullable;

/** REST implementation of data policy management for a configured catalog prefix. */
@Experimental
public class RESTPolicyManagement implements PolicyManagement {

    private final RESTApi api;

    public RESTPolicyManagement(RESTApi api) {
        this.api = api;
    }

    @Override
    public PagedList<DataPolicy> listPolicies(ListPoliciesRequest request) {
        ListPoliciesResponse response = api.listPolicies(request);
        return new PagedList<>(response.getPolicies(), response.getNextPageToken());
    }

    @Override
    public void createPolicy(DataPolicy policy) throws PolicyAlreadyExistException {
        try {
            api.createPolicy(policy);
        } catch (AlreadyExistsException e) {
            if (ErrorResponse.RESOURCE_TYPE_POLICY.equals(e.resourceType())) {
                throw new PolicyAlreadyExistException(policy, e);
            }
            throw e;
        }
    }

    @Override
    public void dropPolicy(
            PermissionResource resource,
            PolicyType type,
            String principal,
            @Nullable String column,
            boolean ignoreIfNotExists) {
        api.dropPolicy(resource, type, principal, column, ignoreIfNotExists);
    }
}
