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

import javax.annotation.Nullable;

/** Resource filters and pagination for listing explicitly granted permissions. */
public class ListPermissionsRequest {

    private final ResourceType resourceType;
    @Nullable private final String database;
    @Nullable private final String table;
    @Nullable private final String function;
    @Nullable private final String view;
    @Nullable private final String principal;
    @Nullable private final String pageToken;
    @Nullable private final Integer maxResults;

    public ListPermissionsRequest(
            ResourceType resourceType,
            @Nullable String database,
            @Nullable String table,
            @Nullable String function,
            @Nullable String view,
            @Nullable String principal,
            @Nullable String pageToken,
            @Nullable Integer maxResults) {
        this.resourceType = resourceType;
        this.database = database;
        this.table = table;
        this.function = function;
        this.view = view;
        this.principal = principal;
        this.pageToken = pageToken;
        this.maxResults = maxResults;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    @Nullable
    public String getDatabase() {
        return database;
    }

    @Nullable
    public String getTable() {
        return table;
    }

    @Nullable
    public String getFunction() {
        return function;
    }

    @Nullable
    public String getView() {
        return view;
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
}
