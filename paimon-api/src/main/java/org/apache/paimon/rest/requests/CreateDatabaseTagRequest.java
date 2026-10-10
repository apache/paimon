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

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;

/** Database extension of tag creation: capture a branch instead of one table snapshot ID. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateDatabaseTagRequest implements RESTRequest {

    private static final String FIELD_TAG_NAME = "tagName";
    private static final String FIELD_FROM_BRANCH = "fromBranch";
    private static final String FIELD_TIME_RETAINED = "timeRetained";

    private final String tagName;
    @Nullable private final String fromBranch;
    @Nullable private final String timeRetained;

    @JsonCreator
    @ConstructorProperties({FIELD_TAG_NAME, FIELD_FROM_BRANCH, FIELD_TIME_RETAINED})
    public CreateDatabaseTagRequest(
            @JsonProperty(FIELD_TAG_NAME) String tagName,
            @Nullable @JsonProperty(FIELD_FROM_BRANCH) String fromBranch,
            @Nullable @JsonProperty(FIELD_TIME_RETAINED) String timeRetained) {
        this.tagName = tagName;
        this.fromBranch = fromBranch;
        this.timeRetained = timeRetained;
    }

    @JsonGetter(FIELD_TAG_NAME)
    public String tagName() {
        return tagName;
    }

    /** Null selects the database's main branch. */
    @Nullable
    @JsonGetter(FIELD_FROM_BRANCH)
    public String fromBranch() {
        return fromBranch;
    }

    @Nullable
    @JsonGetter(FIELD_TIME_RETAINED)
    public String timeRetained() {
        return timeRetained;
    }
}
