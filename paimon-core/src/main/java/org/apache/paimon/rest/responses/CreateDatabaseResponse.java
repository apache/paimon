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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.beans.ConstructorProperties;
import java.util.Map;

/** Response for creating database. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateDatabaseResponse implements RESTResponse {
    private static final String FIELD_NAME = "name";
    private static final String FIELD_PROPERTIES = "properties";

    @JsonProperty(FIELD_NAME)
    private String name;

    @JsonProperty(FIELD_PROPERTIES)
    private Map<String, String> properties;

    @ConstructorProperties({FIELD_NAME, FIELD_PROPERTIES})
    public CreateDatabaseResponse(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
    }

    @JsonGetter(FIELD_NAME)
    public String name() {
        return name;
    }

    @JsonGetter(FIELD_PROPERTIES)
    public Map<String, String> properties() {
        return properties;
    }
}
