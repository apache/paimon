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

import org.apache.paimon.catalog.Database;
import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.rest.RESTCatalogInternalOptions.DATABASE_COMMENT;

/** Response for getting database. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetDatabaseResponse implements RESTResponse, Database {

    private static final String FIELD_NAME = "name";
    private static final String FIELD_OPTIONS = "options";

    @JsonProperty(FIELD_NAME)
    private final String name;

    @JsonProperty(FIELD_OPTIONS)
    private final Map<String, String> options;

    @JsonCreator
    public GetDatabaseResponse(
            @JsonProperty(FIELD_NAME) String name,
            @JsonProperty(FIELD_OPTIONS) Map<String, String> options) {
        this.name = name;
        this.options = options;
    }

    @JsonGetter(FIELD_NAME)
    public String getName() {
        return name;
    }

    @JsonGetter(FIELD_OPTIONS)
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public String name() {
        return this.getName();
    }

    @Override
    public Map<String, String> options() {
        return this.getOptions();
    }

    @Override
    public Optional<String> comment() {
        return Optional.ofNullable(
                this.options.getOrDefault(DATABASE_COMMENT.key(), DATABASE_COMMENT.defaultValue()));
    }
}
