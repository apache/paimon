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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/** Response for listing model metadata in a database. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ListModelsResponse implements PagedResponse<GetModelResponse> {

    private static final String FIELD_MODELS = "models";
    private static final String FIELD_NEXT_PAGE_TOKEN = "nextPageToken";

    private final List<GetModelResponse> models;
    private final @Nullable String nextPageToken;

    @JsonCreator
    public ListModelsResponse(
            @Nullable @JsonProperty(FIELD_MODELS) List<GetModelResponse> models,
            @Nullable @JsonProperty(FIELD_NEXT_PAGE_TOKEN) String nextPageToken) {
        this.models = models == null ? Collections.emptyList() : models;
        this.nextPageToken = nextPageToken;
    }

    @JsonGetter(FIELD_MODELS)
    public List<GetModelResponse> getModels() {
        return models;
    }

    @Override
    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter(FIELD_NEXT_PAGE_TOKEN)
    public String getNextPageToken() {
        return nextPageToken;
    }

    @Override
    public List<GetModelResponse> data() {
        return models;
    }
}
