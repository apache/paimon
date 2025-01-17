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

import java.util.Date;
import java.util.Map;

/** Response for table credentials. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetTableCredentialsResponse implements RESTResponse {

    private static final String FIELD_CREDENTIAL = "credential";
    private static final String FIELD_EXPIREAT = "expiresAt";

    @JsonProperty(FIELD_CREDENTIAL)
    private final Map<String, String> credential;

    @JsonProperty(FIELD_EXPIREAT)
    private Date expiresAt;

    @JsonCreator
    public GetTableCredentialsResponse(
            @JsonProperty(FIELD_EXPIREAT) Date expiresAt,
            @JsonProperty(FIELD_CREDENTIAL) Map<String, String> credential) {
        this.expiresAt = expiresAt;
        this.credential = credential;
    }

    @JsonGetter(FIELD_CREDENTIAL)
    public Map<String, String> getCredential() {
        return credential;
    }

    @JsonGetter(FIELD_EXPIREAT)
    public Date getExpiresAt() {
        return expiresAt;
    }
}
