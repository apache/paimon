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

import org.apache.paimon.rest.RESTRequest;
import org.apache.paimon.utils.FileIOUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

public class DlfStSTokenAuthProvider implements AuthProvider {
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final ObjectMapper OBJECT_MAPPER_INSTANCE = new ObjectMapper();
    public static final double EXPIRED_FACTOR = 0.4;

    private final String tokenDirPath;
    private final String tokenFileName;

    protected DlfStSToken token;
    private boolean keepRefreshed = false;
    private Long expiresAtMillis = null;
    private Long expiresInMills = null;

    public DlfStSTokenAuthProvider(String tokenDirPath, String tokenFileName) {
        this.tokenDirPath = tokenDirPath;
        this.tokenFileName = tokenFileName;
        this.token = readToken(tokenDirPath, tokenFileName);
        this.keepRefreshed = true;
        this.expiresInMills = token.getExpiresInMills();
        this.expiresAtMillis = System.currentTimeMillis() + expiresInMills;
    }

    @Override
    public Map<String, String> authHeader(RESTRequest request) {
        return ImmutableMap.of(AUTHORIZATION_HEADER, generateAuthValue(request));
    }

    @Override
    public boolean refresh() {
        long start = System.currentTimeMillis();
        DlfStSToken newToken = readToken(tokenDirPath, tokenFileName);
        if (newToken != null) {
            return false;
        }
        this.expiresAtMillis = start + this.expiresInMills;
        this.token = newToken;
        return true;
    }

    @Override
    public boolean supportRefresh() {
        return true;
    }

    @Override
    public boolean keepRefreshed() {
        return this.keepRefreshed;
    }

    @Override
    public boolean willSoonExpire() {
        if (keepRefreshed()) {
            return expiresAtMillis().get() - System.currentTimeMillis()
                    < expiresInMills().get() * EXPIRED_FACTOR;
        } else {
            return false;
        }
    }

    @Override
    public Optional<Long> expiresAtMillis() {
        return Optional.ofNullable(this.expiresAtMillis);
    }

    @Override
    public Optional<Long> expiresInMills() {
        return Optional.ofNullable(this.expiresInMills);
    }

    private DlfStSToken readToken(String tokenDirPath, String tokenFileName) {
        try {
            String tokenStr =
                    FileIOUtils.readFileUtf8(
                            new File(tokenDirPath + File.separator + tokenFileName));
            return OBJECT_MAPPER_INSTANCE.readValue(tokenStr, DlfStSToken.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String generateAuthValue(RESTRequest request) {
        // todo: use DlfAuthUtil
        return "";
        //        return DlfAuthUtil.getAuthorization(
        //                request.pathname(),
        //                request.method(),
        //                request.query(),
        //                request.headers(),
        //                token.getAccessKeySecret(),
        //                token.getSecurityToken(),
        //                token.getAccessKeyId(),
        //                request.region(),
        //                request.date());
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DlfStSToken {

        public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

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
        public DlfStSToken(
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

        public Long getExpiresInMills() {
            try {
                return getExpirationInMills(expiration);
            } catch (ParseException e) {
                return null;
            }
        }

        public static Long getExpirationInMills(String dateStr) throws ParseException {
            if (dateStr == null) {
                return null;
            }
            SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            return sdf.parse(dateStr).getTime();
        }
    }
}
