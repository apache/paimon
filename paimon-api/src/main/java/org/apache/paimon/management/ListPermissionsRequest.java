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

/** Exact resource, principal, and pagination filters for listing permission assignments. */
@Experimental
public class ListPermissionsRequest {

    public static final int MAX_PAGE_SIZE = 1000;

    private final PermissionResource resource;
    @Nullable private final String principal;
    @Nullable private final String access;
    @Nullable private final String pageToken;
    @Nullable private final Integer maxResults;

    public ListPermissionsRequest(
            ResourceType resourceType,
            @Nullable String database,
            @Nullable String table,
            @Nullable String function,
            @Nullable String view,
            @Nullable String principal,
            @Nullable String access,
            @Nullable String pageToken,
            @Nullable Integer maxResults) {
        this.resource = exactResource(resourceType, database, table, function, view);
        if (!isBlank(principal)) {
            PermissionAssignment.validatePrincipal(principal);
        }
        checkArgument(maxResults == null || maxResults > 0, "maxResults must be greater than 0.");
        checkArgument(
                maxResults == null || maxResults <= MAX_PAGE_SIZE,
                "maxResults must be at most %s.",
                MAX_PAGE_SIZE);
        this.principal = isBlank(principal) ? null : principal;
        this.access = isBlank(access) ? null : PermissionAccess.canonicalize(resource, access);
        this.pageToken = isBlank(pageToken) ? null : pageToken;
        this.maxResults = maxResults;
    }

    public ResourceType getResourceType() {
        return resource.getType();
    }

    @Nullable
    public String getDatabase() {
        return resource.getDatabase();
    }

    @Nullable
    public String getTable() {
        return resource.getTable();
    }

    @Nullable
    public String getFunction() {
        return resource.getFunction();
    }

    @Nullable
    public String getView() {
        return resource.getView();
    }

    @Nullable
    public String getPrincipal() {
        return principal;
    }

    @Nullable
    public String getAccess() {
        return access;
    }

    @Nullable
    public String getPageToken() {
        return pageToken;
    }

    @Nullable
    public Integer getMaxResults() {
        return maxResults;
    }

    public PermissionResource resource() {
        return resource;
    }

    public ListPermissionsRequest withPageToken(@Nullable String newPageToken) {
        return new ListPermissionsRequest(
                resource.getType(),
                resource.getDatabase(),
                resource.getTable(),
                resource.getFunction(),
                resource.getView(),
                principal,
                access,
                newPageToken,
                maxResults);
    }

    private static PermissionResource exactResource(
            ResourceType resourceType,
            @Nullable String database,
            @Nullable String table,
            @Nullable String function,
            @Nullable String view) {
        checkNotNull(resourceType, "resourceType cannot be null");
        try {
            return new PermissionResource(resourceType, database, table, function, view);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Permission listing requires an exact target resource: " + e.getMessage(), e);
        }
    }

    private static boolean isBlank(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }
}
