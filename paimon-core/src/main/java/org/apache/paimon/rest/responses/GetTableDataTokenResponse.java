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

import java.util.Map;

/** Response for table data token. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetTableDataTokenResponse implements RESTResponse {

    private static final String FIELD_TOKEN = "token";
    private static final String FIELD_EXPIRES_AT_MILLIS = "expiresAtMillis";

    @JsonProperty(FIELD_TOKEN)
    private final Map<String, String> token;

    @JsonProperty(FIELD_EXPIRES_AT_MILLIS)
    private final long expiresAtMillis;

    @JsonCreator
    public GetTableDataTokenResponse(
            @JsonProperty(FIELD_TOKEN) Map<String, String> token,
            @JsonProperty(FIELD_EXPIRES_AT_MILLIS) long expiresAtMillis) {
        this.token = token;
        this.expiresAtMillis = expiresAtMillis;
    }

    @JsonGetter(FIELD_TOKEN)
    public Map<String, String> getToken() {
        return token;
    }

    @JsonGetter(FIELD_EXPIRES_AT_MILLIS)
    public long getExpiresAtMillis() {
        return expiresAtMillis;
    }
}
