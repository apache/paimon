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

import org.apache.paimon.utils.FileIOUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Auth provider for DLF. */
public class DlfAuthProvider implements AuthProvider {
    public static final String DLF_DATE_HEADER_KEY = "x-dlf-date";
    public static final String DLF_HOST_HEADER_KEY = "host";
    public static final String DLF_AUTHORIZATION_HEADER_KEY = "authorization";
    public static final String DLF_DATA_MD5_HEX_HEADER_KEY = "x-dlf-data-md5-hex";
    public static final double EXPIRED_FACTOR = 0.4;

    private static final ObjectMapper OBJECT_MAPPER_INSTANCE = new ObjectMapper();
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");

    private final String tokenFilePath;

    protected DlfToken token;
    private final boolean keepRefreshed;
    private Long expiresAtMillis;
    private final Long tokenRefreshInMills;

    public static DlfAuthProvider buildRefreshToken(
            String tokenFilePath, Long tokenRefreshInMills) {
        DlfToken token = readToken(tokenFilePath);
        Long expiresAtMillis = token.getExpiresInMills();
        return new DlfAuthProvider(
                tokenFilePath, token, true, expiresAtMillis, tokenRefreshInMills);
    }

    public static DlfAuthProvider buildAKToken(String accessKeyId, String accessKeySecret) {
        DlfToken token = new DlfToken(accessKeyId, accessKeySecret, null, null);
        return new DlfAuthProvider(null, token, false, null, null);
    }

    public DlfAuthProvider(
            String tokenFilePath,
            DlfToken token,
            boolean keepRefreshed,
            Long expiresAtMillis,
            Long tokenRefreshInMills) {
        this.tokenFilePath = tokenFilePath;
        this.token = token;
        this.keepRefreshed = keepRefreshed;
        this.expiresAtMillis = expiresAtMillis;
        this.tokenRefreshInMills = tokenRefreshInMills;
    }

    @Override
    public Map<String, String> header(
            Map<String, String> baseHeader, RestAuthParameter restAuthParameter) {
        try {
            String date = getDate();
            String dataMd5Hex = DlfAuthSignature.md5Hex(restAuthParameter.data());
            String authorization =
                    DlfAuthSignature.getAuthorization(restAuthParameter, token, dataMd5Hex, date);
            Map<String, String> headersWithAuth = new HashMap<>();
            headersWithAuth.putAll(baseHeader);
            headersWithAuth.put(DLF_AUTHORIZATION_HEADER_KEY, authorization);
            headersWithAuth.put(DLF_DATE_HEADER_KEY, date);
            headersWithAuth.put(DLF_HOST_HEADER_KEY, restAuthParameter.host());
            headersWithAuth.put(DLF_DATA_MD5_HEX_HEADER_KEY, dataMd5Hex);
            return headersWithAuth;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String token() {
        try {
            return OBJECT_MAPPER_INSTANCE.writeValueAsString(this.token);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean refresh() {
        long start = System.currentTimeMillis();
        DlfToken newToken = readToken(tokenFilePath);
        if (newToken == null) {
            return false;
        }
        this.expiresAtMillis = start + this.tokenRefreshInMills;
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
                    < tokenRefreshInMills().get() * EXPIRED_FACTOR;
        } else {
            return false;
        }
    }

    @Override
    public Optional<Long> expiresAtMillis() {
        return Optional.ofNullable(this.expiresAtMillis);
    }

    @Override
    public Optional<Long> tokenRefreshInMills() {
        return Optional.ofNullable(this.tokenRefreshInMills);
    }

    static String getDate() {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        return now.format(DATE_FORMATTER);
    }

    // todo: if file not exist, throw exception?
    private static DlfToken readToken(String tokenFilePath) {
        try {
            String tokenStr = FileIOUtils.readFileUtf8(new File(tokenFilePath));
            return OBJECT_MAPPER_INSTANCE.readValue(tokenStr, DlfToken.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
