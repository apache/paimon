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

package org.apache.paimon.rest.responses;

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.management.PermissionAssignment;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;
import java.util.List;

/** Response for listing permissions. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class ListPermissionsResponse implements PagedResponse<PermissionAssignment> {

    private static final String FIELD_PERMISSIONS = "permissions";
    private static final String FIELD_NEXT_PAGE_TOKEN = "nextPageToken";

    @JsonProperty(FIELD_PERMISSIONS)
    private final List<PermissionAssignment> permissions;

    @Nullable
    @JsonProperty(FIELD_NEXT_PAGE_TOKEN)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String nextPageToken;

    @JsonCreator
    @ConstructorProperties({FIELD_PERMISSIONS, FIELD_NEXT_PAGE_TOKEN})
    public ListPermissionsResponse(
            @JsonProperty(FIELD_PERMISSIONS) List<PermissionAssignment> permissions,
            @Nullable @JsonProperty(FIELD_NEXT_PAGE_TOKEN) String nextPageToken) {
        this.permissions = permissions;
        this.nextPageToken = nextPageToken;
    }

    @JsonGetter(FIELD_PERMISSIONS)
    public List<PermissionAssignment> getPermissions() {
        return permissions;
    }

    @Override
    @Nullable
    @JsonGetter(FIELD_NEXT_PAGE_TOKEN)
    public String getNextPageToken() {
        return nextPageToken;
    }

    @Override
    public List<PermissionAssignment> data() {
        return permissions;
    }
}
