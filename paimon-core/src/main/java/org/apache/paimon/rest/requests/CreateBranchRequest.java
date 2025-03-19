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

/** Request for creating branch. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateBranchRequest implements RESTRequest {

    private static final String FIELD_BRANCH = "branch";
    private static final String FIELD_FROM_TAG = "fromTag";

    @JsonProperty(FIELD_BRANCH)
    private final String branch;

    @Nullable
    @JsonProperty(FIELD_FROM_TAG)
    private final String fromTag;

    @JsonCreator
    public CreateBranchRequest(
            @JsonProperty(FIELD_BRANCH) String branch,
            @Nullable @JsonProperty(FIELD_FROM_TAG) String fromTag) {
        this.branch = branch;
        this.fromTag = fromTag;
    }

    @JsonGetter(FIELD_BRANCH)
    public String branch() {
        return branch;
    }

    @Nullable
    @JsonGetter(FIELD_FROM_TAG)
    public String fromTag() {
        return fromTag;
    }
}
