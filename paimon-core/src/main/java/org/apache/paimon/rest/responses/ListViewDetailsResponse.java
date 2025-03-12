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

/** Response for listing view details. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ListViewDetailsResponse implements PagedResponse<GetViewResponse> {

    private static final String FIELD_VIEW_DETAILS = "viewDetails";
    private static final String FIELD_NEXT_PAGE_TOKEN = "nextPageToken";

    @JsonProperty(FIELD_VIEW_DETAILS)
    private final List<GetViewResponse> viewDetails;

    @JsonProperty(FIELD_NEXT_PAGE_TOKEN)
    private final String nextPageToken;

    public ListViewDetailsResponse(
            @JsonProperty(FIELD_VIEW_DETAILS) List<GetViewResponse> viewDetails) {
        this(viewDetails, null);
    }

    @JsonCreator
    public ListViewDetailsResponse(
            @JsonProperty(FIELD_VIEW_DETAILS) List<GetViewResponse> viewDetails,
            @JsonProperty(FIELD_NEXT_PAGE_TOKEN) String nextPageToken) {
        this.viewDetails = viewDetails;
        this.nextPageToken = nextPageToken;
    }

    @JsonGetter(FIELD_VIEW_DETAILS)
    public List<GetViewResponse> getViewDetails() {
        return this.viewDetails;
    }

    @JsonGetter(FIELD_NEXT_PAGE_TOKEN)
    public String getNextPageToken() {
        return this.nextPageToken;
    }

    @Override
    public List<GetViewResponse> data() {
        return getViewDetails();
    }
}
