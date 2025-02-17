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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.rest.RESTObjectMapper.OBJECT_MAPPER;

/** Auth provider for <b>Ali CLoud</b> DLF. */
public class DLFAuthProvider implements AuthProvider {

    public static final String DLF_HOST_HEADER_KEY = "Host";
    public static final String DLF_AUTHORIZATION_HEADER_KEY = "Authorization";
    public static final String DLF_DATA_MD5_HEX_HEADER_KEY = "x-dlf-data-md5-hex";
    public static final String DLF_DATE_HEADER_KEY = "x-dlf-date";
    public static final String DLF_SECURITY_TOKEN_HEADER_KEY = "x-dlf-security-token";
    public static final String DLF_ACCESSKEY_ID_HEADER_KEY = "x-dlf-accesskey-id";
    public static final double EXPIRED_FACTOR = 0.4;

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private final String tokenFilePath;

    protected DLFToken token;
    private final boolean keepRefreshed;
    private Long expiresAtMillis;
    private final Long tokenRefreshInMills;

    public static DLFAuthProvider buildRefreshToken(
            String tokenFilePath, Long tokenRefreshInMills) {
        DLFToken token = readToken(tokenFilePath);
        Long expiresAtMillis = getExpirationInMills(token.getExpiration());
        return new DLFAuthProvider(
                tokenFilePath, token, true, expiresAtMillis, tokenRefreshInMills);
    }

    public static DLFAuthProvider buildAKToken(String accessKeyId, String accessKeySecret) {
        DLFToken token = new DLFToken(accessKeyId, accessKeySecret, null, null);
        return new DLFAuthProvider(null, token, false, null, null);
    }

    public DLFAuthProvider(
            String tokenFilePath,
            DLFToken token,
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
            Map<String, String> baseHeader, RESTAuthParameter restAuthParameter) {
        try {
            String date = getDate();
            String dataMd5Hex = DLFAuthSignature.md5Hex(restAuthParameter.data());
            String authorization =
                    DLFAuthSignature.getAuthorization(restAuthParameter, token, dataMd5Hex, date);
            Map<String, String> headersWithAuth = new HashMap<>(baseHeader);
            headersWithAuth.put(DLF_AUTHORIZATION_HEADER_KEY, authorization);
            headersWithAuth.put(DLF_DATE_HEADER_KEY, date);
            headersWithAuth.put(DLF_HOST_HEADER_KEY, restAuthParameter.host());
            headersWithAuth.put(DLF_DATA_MD5_HEX_HEADER_KEY, dataMd5Hex);
            if (token.getSecurityToken() != null) {
                headersWithAuth.put(DLF_SECURITY_TOKEN_HEADER_KEY, token.getSecurityToken());
                headersWithAuth.put(DLF_ACCESSKEY_ID_HEADER_KEY, token.getAccessKeyId());
            }
            return headersWithAuth;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean refresh() {
        long start = System.currentTimeMillis();
        DLFToken newToken = readToken(tokenFilePath);
        if (newToken == null) {
            return false;
        }
        this.expiresAtMillis = start + this.tokenRefreshInMills;
        this.token = newToken;
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

    private static DLFToken readToken(String tokenFilePath) {
        try {
            File tokenFile = new File(tokenFilePath);
            if (tokenFile.exists()) {
                String tokenStr = FileIOUtils.readFileUtf8(tokenFile);
                return OBJECT_MAPPER.readValue(tokenStr, DLFToken.class);
            } else {
                throw new FileNotFoundException(tokenFilePath);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Long getExpirationInMills(String dateStr) {
        try {
            if (dateStr == null) {
                return null;
            }
            LocalDateTime dateTime = LocalDateTime.parse(dateStr, DATE_FORMATTER);
            return dateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
