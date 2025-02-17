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

package org.apache.paimon.rest.auth;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/** <b>Ali CLoud</b> DLF Token. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DLFToken {

    private static final String ACCESS_KEY_ID_FIELD_NAME = "AccessKeyId";
    private static final String ACCESS_KEY_SECRET_FIELD_NAME = "AccessKeySecret";
    private static final String SECURITY_TOKEN_FIELD_NAME = "SecurityToken";
    private static final String EXPIRATION_FIELD_NAME = "Expiration";

    @JsonProperty(ACCESS_KEY_ID_FIELD_NAME)
    private final String accessKeyId;

    @JsonProperty(ACCESS_KEY_SECRET_FIELD_NAME)
    private final String accessKeySecret;

    @JsonProperty(SECURITY_TOKEN_FIELD_NAME)
    private final String securityToken;

    @JsonProperty(EXPIRATION_FIELD_NAME)
    private final String expiration;

    @JsonCreator
    public DLFToken(
            @JsonProperty(ACCESS_KEY_ID_FIELD_NAME) String accessKeyId,
            @JsonProperty(ACCESS_KEY_SECRET_FIELD_NAME) String accessKeySecret,
            @JsonProperty(SECURITY_TOKEN_FIELD_NAME) String securityToken,
            @JsonProperty(EXPIRATION_FIELD_NAME) String expiration) {
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.securityToken = securityToken;
        this.expiration = expiration;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public String getSecurityToken() {
        return securityToken;
    }

    public String getExpiration() {
        return expiration;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DLFToken that = (DLFToken) o;
        return Objects.equals(accessKeyId, that.accessKeyId)
                && Objects.equals(accessKeySecret, that.accessKeySecret)
                && Objects.equals(securityToken, that.securityToken)
                && Objects.equals(expiration, that.expiration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessKeyId, accessKeySecret, securityToken, expiration);
    }
}
