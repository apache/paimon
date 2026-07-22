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

package org.apache.paimon.rest.requests;

import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Request for listing partitions by filter. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ListPartitionsByFilterRequest implements RESTRequest {

    private static final String FIELD_FILTER = "filter";
    private static final String FIELD_PARTITION_NAME_PATTERN = "partitionNamePattern";
    private static final String FIELD_MAX_RESULTS = "maxResults";
    private static final String FIELD_PAGE_TOKEN = "pageToken";

    /** JSON serialization of a Paimon {@code Predicate} tree over the partition columns. */
    @JsonProperty(FIELD_FILTER)
    private final String filter;

    @JsonProperty(FIELD_PARTITION_NAME_PATTERN)
    @Nullable
    private final String partitionNamePattern;

    @JsonProperty(FIELD_MAX_RESULTS)
    @Nullable
    private final Integer maxResults;

    @JsonProperty(FIELD_PAGE_TOKEN)
    @Nullable
    private final String pageToken;

    @JsonCreator
    public ListPartitionsByFilterRequest(
            @JsonProperty(FIELD_FILTER) String filter,
            @JsonProperty(FIELD_PARTITION_NAME_PATTERN) @Nullable String partitionNamePattern,
            @JsonProperty(FIELD_MAX_RESULTS) @Nullable Integer maxResults,
            @JsonProperty(FIELD_PAGE_TOKEN) @Nullable String pageToken) {
        this.filter = filter;
        this.partitionNamePattern = partitionNamePattern;
        this.maxResults = maxResults;
        this.pageToken = pageToken;
    }

    @JsonGetter(FIELD_FILTER)
    public String getFilter() {
        return filter;
    }

    @JsonGetter(FIELD_PARTITION_NAME_PATTERN)
    @Nullable
    public String getPartitionNamePattern() {
        return partitionNamePattern;
    }

    @JsonGetter(FIELD_MAX_RESULTS)
    @Nullable
    public Integer getMaxResults() {
        return maxResults;
    }

    @JsonGetter(FIELD_PAGE_TOKEN)
    @Nullable
    public String getPageToken() {
        return pageToken;
    }
}
