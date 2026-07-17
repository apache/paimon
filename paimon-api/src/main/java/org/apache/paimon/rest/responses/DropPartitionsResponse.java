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

import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/** Response for dropping (unregistering) partitions. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DropPartitionsResponse implements RESTResponse {

    private static final String FIELD_DROPPED = "dropped";
    private static final String FIELD_MISSING = "missing";

    @JsonProperty(FIELD_DROPPED)
    private final List<Map<String, String>> dropped;

    @JsonProperty(FIELD_MISSING)
    private final List<Map<String, String>> missing;

    @JsonCreator
    public DropPartitionsResponse(
            @JsonProperty(FIELD_DROPPED) List<Map<String, String>> dropped,
            @JsonProperty(FIELD_MISSING) List<Map<String, String>> missing) {
        this.dropped = dropped;
        this.missing = missing;
    }

    @JsonGetter(FIELD_DROPPED)
    public List<Map<String, String>> getDropped() {
        return dropped;
    }

    @JsonGetter(FIELD_MISSING)
    public List<Map<String, String>> getMissing() {
        return missing;
    }
}
