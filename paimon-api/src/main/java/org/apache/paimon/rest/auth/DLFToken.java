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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/** <b>Ali CLoud</b> DLF Token. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DLFToken {

    public static final DateTimeFormatter TOKEN_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

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

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(EXPIRATION_FIELD_NAME)
    @Nullable
    private final String expiration;

    @JsonIgnore @Nullable private final Long expirationAtMills;

    @JsonCreator
    public DLFToken(
            @JsonProperty(ACCESS_KEY_ID_FIELD_NAME) String accessKeyId,
            @JsonProperty(ACCESS_KEY_SECRET_FIELD_NAME) String accessKeySecret,
            @JsonProperty(SECURITY_TOKEN_FIELD_NAME) String securityToken,
            @Nullable @JsonProperty(EXPIRATION_FIELD_NAME) String expiration) {
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.securityToken = securityToken;
        this.expiration = expiration;
        if (expiration == null) {
            this.expirationAtMills = null;
        } else {
            LocalDateTime dateTime = LocalDateTime.parse(expiration, TOKEN_DATE_FORMATTER);
            // Note: the date time is UTC time zone
            this.expirationAtMills = dateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        }
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

    @Nullable
    public Long getExpirationAtMills() {
        return expirationAtMills;
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
