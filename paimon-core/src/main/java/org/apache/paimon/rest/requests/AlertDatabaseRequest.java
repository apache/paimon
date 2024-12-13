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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/** Request for alerting database. */
public class AlertDatabaseRequest implements RESTRequest {

    private static final String FIELD_REMOVALS = "removals";
    private static final String FIELD_UPDATES = "updates";

    @JsonProperty(FIELD_REMOVALS)
    private List<String> removals;

    @JsonProperty(FIELD_UPDATES)
    private Map<String, String> updates;

    @JsonCreator
    public AlertDatabaseRequest(
            @JsonProperty(FIELD_REMOVALS) List<String> removals,
            @JsonProperty(FIELD_UPDATES) Map<String, String> updates) {
        this.removals = removals;
        this.updates = updates;
    }

    @JsonGetter(FIELD_REMOVALS)
    public List<String> getRemovals() {
        return removals;
    }

    @JsonGetter(FIELD_UPDATES)
    public Map<String, String> getUpdates() {
        return updates;
    }
}
