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
    @Nullable private final PolicyType type;
    @Nullable private final String principal;
    @Nullable private final String column;
    @Nullable private final String pageToken;
    @Nullable private final Integer maxResults;

    public ListPoliciesRequest(
            PermissionResource resource,
            @Nullable PolicyType type,
            @Nullable String principal,
            @Nullable String column,
            @Nullable String pageToken,
            @Nullable Integer maxResults) {
        this.resource = checkNotNull(resource, "resource cannot be null");
        resource.validatePolicyAttachment();
        if (!isBlank(principal)) {
            PermissionAssignment.validatePrincipal(principal);
        }
        checkArgument(maxResults == null || maxResults > 0, "maxResults must be greater than 0.");
        checkArgument(
                maxResults == null || maxResults <= ListPermissionsRequest.MAX_PAGE_SIZE,
                "maxResults must be at most %s.",
                ListPermissionsRequest.MAX_PAGE_SIZE);
        this.type = type;
        this.principal = isBlank(principal) ? null : principal;
        checkArgument(
                isBlank(column) || type == PolicyType.COLUMN_MASKING,
                "column filter requires type COLUMN_MASKING.");
        this.column = isBlank(column) ? null : column;
        this.pageToken = isBlank(pageToken) ? null : pageToken;
        this.maxResults = maxResults;
    }

    public PermissionResource getResource() {
        return resource;
    }

    @Nullable
    public PolicyType getType() {
        return type;
    }

    @Nullable
    public String getPrincipal() {
        return principal;
    }

    @Nullable
    public String getColumn() {
        return column;
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
        return new ListPoliciesRequest(resource, type, principal, column, newPageToken, maxResults);
    }

    private static boolean isBlank(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }
}
