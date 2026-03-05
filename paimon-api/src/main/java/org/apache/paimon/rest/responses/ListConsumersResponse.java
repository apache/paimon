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

import org.apache.paimon.consumer.ConsumerInfo;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Response for list consumers. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ListConsumersResponse implements PagedResponse<ConsumerInfo> {

    private static final String FIELD_CONSUMERS = "consumers";
    private static final String FIELD_NEXT_PAGE_TOKEN = "nextPageToken";

    @JsonProperty(FIELD_CONSUMERS)
    private final List<ConsumerInfo> consumers;

    @JsonProperty(FIELD_NEXT_PAGE_TOKEN)
    private final String nextPageToken;

    public ListConsumersResponse(@JsonProperty(FIELD_CONSUMERS) List<ConsumerInfo> consumers) {
        this(consumers, null);
    }

    @JsonCreator
    public ListConsumersResponse(
            @JsonProperty(FIELD_CONSUMERS) List<ConsumerInfo> consumers,
            @JsonProperty(FIELD_NEXT_PAGE_TOKEN) String nextPageToken) {
        this.consumers = consumers;
        this.nextPageToken = nextPageToken;
    }

    @JsonGetter(FIELD_CONSUMERS)
    public List<ConsumerInfo> getConsumers() {
        return this.consumers;
    }

    @Override
    public List<ConsumerInfo> data() {
        return this.consumers;
    }

    @JsonGetter(FIELD_NEXT_PAGE_TOKEN)
    public String getNextPageToken() {
        return this.nextPageToken;
    }
}
