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

package org.apache.paimon.management;

import org.apache.paimon.annotation.Experimental;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Filters for listing policies attached to an exact table resource. */
@Experimental
public class ListPoliciesRequest {

    private final PermissionResource resource;
    @Nullable private final String name;
    @Nullable private final PolicyType type;
    @Nullable private final PrincipalType principalType;
    @Nullable private final String principal;
    @Nullable private final String pageToken;
    @Nullable private final Integer maxResults;

    public ListPoliciesRequest(
            PermissionResource resource,
            @Nullable String name,
            @Nullable PolicyType type,
            @Nullable PrincipalType principalType,
            @Nullable String principal,
            @Nullable String pageToken,
            @Nullable Integer maxResults) {
        this.resource = checkNotNull(resource, "resource cannot be null");
        resource.validatePolicyAttachment();
        checkArgument(
                (principalType == null) == isBlank(principal),
                "principalType and principal must be specified together.");
        checkArgument(maxResults == null || maxResults > 0, "maxResults must be greater than 0.");
        checkArgument(
                maxResults == null || maxResults <= ListPermissionsRequest.MAX_PAGE_SIZE,
                "maxResults must be at most %s.",
                ListPermissionsRequest.MAX_PAGE_SIZE);
        this.name = isBlank(name) ? null : name;
        this.type = type;
        this.principalType = principalType;
        this.principal = isBlank(principal) ? null : principal;
        this.pageToken = isBlank(pageToken) ? null : pageToken;
        this.maxResults = maxResults;
    }

    public PermissionResource getResource() {
        return resource;
    }

    @Nullable
    public String getName() {
        return name;
    }

    @Nullable
    public PolicyType getType() {
        return type;
    }

    @Nullable
    public PrincipalType getPrincipalType() {
        return principalType;
    }

    @Nullable
    public String getPrincipal() {
        return principal;
    }

    @Nullable
    public String getPageToken() {
        return pageToken;
    }

    @Nullable
    public Integer getMaxResults() {
        return maxResults;
    }

    public ListPoliciesRequest withPageToken(@Nullable String newPageToken) {
        return new ListPoliciesRequest(
                resource, name, type, principalType, principal, newPageToken, maxResults);
    }

    private static boolean isBlank(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }
}
