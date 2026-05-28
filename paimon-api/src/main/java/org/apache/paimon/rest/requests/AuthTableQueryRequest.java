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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;

/** Request for auth table query. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AuthTableQueryRequest implements RESTRequest {

    private static final String FIELD_SELECT = "select";
    private static final String FIELD_ACCESS_TYPE = "accessType";
    private static final String FIELD_FALLBACK_FROM = "fallbackFrom";

    @JsonProperty(FIELD_SELECT)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final List<String> select;

    @JsonProperty(FIELD_ACCESS_TYPE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final String accessType;

    @JsonProperty(FIELD_FALLBACK_FROM)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final String fallbackFrom;

    @JsonCreator
    public AuthTableQueryRequest(
            @JsonProperty(FIELD_SELECT) @Nullable List<String> select,
            @JsonProperty(FIELD_ACCESS_TYPE) @Nullable String accessType,
            @JsonProperty(FIELD_FALLBACK_FROM) @Nullable String fallbackFrom) {
        this.select = select;
        this.accessType = accessType;
        this.fallbackFrom = fallbackFrom;
    }

    public AuthTableQueryRequest(@Nullable List<String> select) {
        this(select, null, null);
    }

    @JsonGetter(FIELD_SELECT)
    @Nullable
    public List<String> select() {
        return select;
    }

    @JsonGetter(FIELD_ACCESS_TYPE)
    @Nullable
    public String accessType() {
        return accessType;
    }

    @JsonGetter(FIELD_FALLBACK_FROM)
    @Nullable
    public String fallbackFrom() {
        return fallbackFrom;
    }
}
