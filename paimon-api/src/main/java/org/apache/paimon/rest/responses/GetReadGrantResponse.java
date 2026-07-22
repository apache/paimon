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

import java.util.Objects;

/**
 * Response containing a grant bound to server-approved dependencies of a table or view.
 *
 * <p>A successful response approves every dependency target in its corresponding request. The
 * client keeps those targets locally instead of requiring the server to echo the complete graph.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetReadGrantResponse implements RESTResponse {

    private static final String FIELD_READ_GRANT = "readGrant";
    private static final String FIELD_EXPIRES_AT_MILLIS = "expiresAtMillis";

    private final String readGrant;
    private final long expiresAtMillis;

    @JsonCreator
    public GetReadGrantResponse(
            @JsonProperty(FIELD_READ_GRANT) String readGrant,
            @JsonProperty(FIELD_EXPIRES_AT_MILLIS) long expiresAtMillis) {
        this.readGrant = Objects.requireNonNull(readGrant, "readGrant");
        this.expiresAtMillis = expiresAtMillis;
    }

    @JsonGetter(FIELD_READ_GRANT)
    public String getReadGrant() {
        return readGrant;
    }

    @JsonGetter(FIELD_EXPIRES_AT_MILLIS)
    public long getExpiresAtMillis() {
        return expiresAtMillis;
    }
}
