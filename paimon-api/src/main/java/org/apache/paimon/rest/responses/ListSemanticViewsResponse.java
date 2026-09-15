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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;
import java.util.List;

import static java.util.Collections.emptyList;

/** One page of semantic view names in a database. */
@Experimental
public class ListSemanticViewsResponse implements PagedResponse<String> {

    private static final String FIELD_SEMANTIC_VIEWS = "semanticViews";
    private static final String FIELD_NEXT_PAGE_TOKEN = "nextPageToken";

    @JsonProperty(FIELD_SEMANTIC_VIEWS)
    private final List<String> semanticViews;

    @Nullable
    @JsonProperty(FIELD_NEXT_PAGE_TOKEN)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String nextPageToken;

    @JsonCreator
    @ConstructorProperties({FIELD_SEMANTIC_VIEWS, FIELD_NEXT_PAGE_TOKEN})
    public ListSemanticViewsResponse(
            @JsonProperty(FIELD_SEMANTIC_VIEWS) List<String> semanticViews,
            @Nullable @JsonProperty(FIELD_NEXT_PAGE_TOKEN) String nextPageToken) {
        this.semanticViews = semanticViews == null ? emptyList() : semanticViews;
        this.nextPageToken = nextPageToken;
    }

    @JsonGetter(FIELD_SEMANTIC_VIEWS)
    public List<String> getSemanticViews() {
        return semanticViews;
    }

    @Override
    @Nullable
    @JsonGetter(FIELD_NEXT_PAGE_TOKEN)
    public String getNextPageToken() {
        return nextPageToken;
    }

    @Override
    public List<String> data() {
        return semanticViews;
    }
}
