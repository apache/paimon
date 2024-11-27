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

import org.apache.paimon.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.beans.ConstructorProperties;
import java.util.Map;

/** Response for getting config. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfigResponse implements RESTResponse {
    private static final String FIELD_OPTIONS = "options";

    @JsonProperty(FIELD_OPTIONS)
    private Map<String, String> options;

    @ConstructorProperties({FIELD_OPTIONS})
    public ConfigResponse(Map<String, String> options) {
        this.options = options;
    }

    @JsonGetter(FIELD_OPTIONS)
    public Map<String, String> options() {
        return options;
    }
}
