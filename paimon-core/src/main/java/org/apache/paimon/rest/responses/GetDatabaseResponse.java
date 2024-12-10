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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;
import java.util.Map;
import java.util.Optional;

/** Response for getting database. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetDatabaseResponse implements RESTResponse, Database {
    private static final String FIELD_NAME = "name";
    private static final String FIELD_OPTIONS = "options";
    private static final String FIELD_COMMENT = "comment";

    @JsonProperty(FIELD_NAME)
    private final String name;

    @JsonProperty(FIELD_OPTIONS)
    private final Map<String, String> options;

    @JsonProperty(FIELD_COMMENT)
    @Nullable
    private final String comment;

    @ConstructorProperties({FIELD_NAME, FIELD_OPTIONS, FIELD_COMMENT})
    public GetDatabaseResponse(String name, Map<String, String> options, @Nullable String comment) {
        this.name = name;
        this.options = options;
        this.comment = comment;
    }

    @Override
    @JsonGetter(FIELD_NAME)
    public String name() {
        return name;
    }

    @Override
    @JsonGetter(FIELD_OPTIONS)
    public Map<String, String> options() {
        return options;
    }

    @Override
    @JsonGetter(FIELD_COMMENT)
    public Optional<String> comment() {
        return Optional.ofNullable(comment);
    }
}
