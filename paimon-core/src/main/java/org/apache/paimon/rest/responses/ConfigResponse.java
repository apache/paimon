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
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/** Response for getting config. */
public class ConfigResponse implements RESTResponse {
    private static final String FIELD_DEFAULTS = "defaults";
    private static final String FIELD_OVERRIDES = "overrides";

    @JsonProperty(FIELD_DEFAULTS)
    private Map<String, String> defaults;

    @JsonProperty(FIELD_OVERRIDES)
    private Map<String, String> overrides;

    @JsonCreator
    public ConfigResponse(
            @JsonProperty(FIELD_DEFAULTS) Map<String, String> defaults,
            @JsonProperty(FIELD_OVERRIDES) Map<String, String> overrides) {
        this.defaults = defaults;
        this.overrides = overrides;
    }

    public Map<String, String> merge(Map<String, String> clientProperties) {
        Preconditions.checkNotNull(
                clientProperties,
                "Cannot merge client properties with server-provided properties. Invalid client configuration: null");
        Map<String, String> merged =
                defaults != null ? Maps.newHashMap(defaults) : Maps.newHashMap();
        merged.putAll(clientProperties);

        if (overrides != null) {
            merged.putAll(overrides);
        }

        return ImmutableMap.copyOf(Maps.filterValues(merged, Objects::nonNull));
    }

    @JsonGetter(FIELD_DEFAULTS)
    public Map<String, String> getDefaults() {
        return defaults;
    }

    @JsonGetter(FIELD_OVERRIDES)
    public Map<String, String> getOverrides() {
        return overrides;
    }
}
