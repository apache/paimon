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

import org.apache.paimon.Snapshot;
import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Response for getting tag. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetTagResponse implements RESTResponse {

    private static final String FIELD_TAG_NAME = "tagName";
    private static final String FIELD_SNAPSHOT = "snapshot";
    private static final String FIELD_TAG_CREATE_TIME = "tagCreateTime";
    private static final String FIELD_TAG_TIME_RETAINED = "tagTimeRetained";

    @JsonProperty(FIELD_TAG_NAME)
    private final String tagName;

    @JsonProperty(FIELD_SNAPSHOT)
    private final Snapshot snapshot;

    @Nullable
    @JsonProperty(FIELD_TAG_CREATE_TIME)
    private final Long tagCreateTime;

    @Nullable
    @JsonProperty(FIELD_TAG_TIME_RETAINED)
    private final String tagTimeRetained;

    @JsonCreator
    public GetTagResponse(
            @JsonProperty(FIELD_TAG_NAME) String tagName,
            @JsonProperty(FIELD_SNAPSHOT) Snapshot snapshot,
            @Nullable @JsonProperty(FIELD_TAG_CREATE_TIME) Long tagCreateTime,
            @Nullable @JsonProperty(FIELD_TAG_TIME_RETAINED) String tagTimeRetained) {
        this.tagName = tagName;
        this.snapshot = snapshot;
        this.tagCreateTime = tagCreateTime;
        this.tagTimeRetained = tagTimeRetained;
    }

    @JsonGetter(FIELD_TAG_NAME)
    public String tagName() {
        return tagName;
    }

    @JsonGetter(FIELD_SNAPSHOT)
    public Snapshot snapshot() {
        return snapshot;
    }

    @Nullable
    @JsonGetter(FIELD_TAG_CREATE_TIME)
    public Long tagCreateTime() {
        return tagCreateTime;
    }

    @Nullable
    @JsonGetter(FIELD_TAG_TIME_RETAINED)
    public String tagTimeRetained() {
        return tagTimeRetained;
    }
}
