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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Response for listing databases. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ListDatabasesResponse implements PagedResponse<String> {

    private static final String FIELD_DATABASES = "databases";
    private static final String FIELD_NEXT_PAGE_TOKEN = "nextPageToken";

    @JsonProperty(FIELD_DATABASES)
    private final List<String> databases;

    @JsonProperty(FIELD_NEXT_PAGE_TOKEN)
    private final String nextPageToken;

    public ListDatabasesResponse(@JsonProperty(FIELD_DATABASES) List<String> databases) {
        this(databases, null);
    }

    @JsonCreator
    public ListDatabasesResponse(
            @JsonProperty(FIELD_DATABASES) List<String> databases,
            @JsonProperty(FIELD_NEXT_PAGE_TOKEN) String nextPageToken) {
        this.databases = databases;
        this.nextPageToken = nextPageToken;
    }

    @JsonGetter(FIELD_DATABASES)
    public List<String> getDatabases() {
        return this.databases;
    }

    @Override
    public String getNextPageToken() {
        return this.nextPageToken;
    }

    @Override
    public List<String> data() {
        return this.getDatabases();
    }
}
