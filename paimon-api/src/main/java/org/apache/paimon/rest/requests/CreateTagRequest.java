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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Request for creating tag. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateTagRequest implements RESTRequest {

    private static final String FIELD_TAG_NAME = "tagName";
    private static final String FIELD_SNAPSHOT_ID = "snapshotId";
    private static final String FIELD_TIME_RETAINED = "timeRetained";

    @JsonProperty(FIELD_TAG_NAME)
    private final String tagName;

    @Nullable
    @JsonProperty(FIELD_SNAPSHOT_ID)
    private final Long snapshotId;

    @Nullable
    @JsonProperty(FIELD_TIME_RETAINED)
    private final String timeRetained;

    @JsonCreator
    public CreateTagRequest(
            @JsonProperty(FIELD_TAG_NAME) String tagName,
            @Nullable @JsonProperty(FIELD_SNAPSHOT_ID) Long snapshotId,
            @Nullable @JsonProperty(FIELD_TIME_RETAINED) String timeRetained) {
        this.tagName = tagName;
        this.snapshotId = snapshotId;
        this.timeRetained = timeRetained;
    }

    @JsonGetter(FIELD_TAG_NAME)
    public String tagName() {
        return tagName;
    }

    @Nullable
    @JsonGetter(FIELD_SNAPSHOT_ID)
    public Long snapshotId() {
        return snapshotId;
    }

    @Nullable
    @JsonGetter(FIELD_TIME_RETAINED)
    public String timeRetained() {
        return timeRetained;
    }
}
