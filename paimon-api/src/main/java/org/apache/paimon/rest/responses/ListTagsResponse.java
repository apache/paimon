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

/** Response for listing tags. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ListTagsResponse implements PagedResponse<String> {

    private static final String FIELD_TAGS = "tags";
    private static final String FIELD_NEXT_PAGE_TOKEN = "nextPageToken";

    @JsonProperty(FIELD_TAGS)
    private final List<String> tags;

    @JsonProperty(FIELD_NEXT_PAGE_TOKEN)
    private final String nextPageToken;

    @JsonCreator
    public ListTagsResponse(
            @JsonProperty(FIELD_TAGS) List<String> tags,
            @JsonProperty(FIELD_NEXT_PAGE_TOKEN) String nextPageToken) {
        this.tags = tags;
        this.nextPageToken = nextPageToken;
    }

    @JsonGetter(FIELD_TAGS)
    public List<String> tags() {
        return this.tags;
    }

    @JsonGetter(FIELD_NEXT_PAGE_TOKEN)
    public String getNextPageToken() {
        return this.nextPageToken;
    }

    @Override
    public List<String> data() {
        return tags();
    }
}
