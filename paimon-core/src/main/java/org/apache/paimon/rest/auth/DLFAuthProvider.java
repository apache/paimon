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

import okhttp3.MediaType;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Auth provider for <b>Ali CLoud</b> DLF. */
public class DLFAuthProvider implements AuthProvider {

    public static final String DLF_AUTHORIZATION_HEADER_KEY = "Authorization";
    public static final String DLF_CONTENT_MD5_HEADER_KEY = "Content-MD5";
    public static final String DLF_CONTENT_TYPE_KEY = "Content-Type";
    public static final String DLF_DATE_HEADER_KEY = "x-dlf-date";
    public static final String DLF_SECURITY_TOKEN_HEADER_KEY = "x-dlf-security-token";
    public static final String DLF_AUTH_VERSION_HEADER_KEY = "x-dlf-version";
    public static final String DLF_CONTENT_SHA56_HEADER_KEY = "x-dlf-content-sha256";
    public static final String DLF_CONTENT_SHA56_VALUE = "UNSIGNED-PAYLOAD";
    public static final double EXPIRED_FACTOR = 0.4;
    public static final DateTimeFormatter TOKEN_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    public static final DateTimeFormatter AUTH_DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");
    public static final DateTimeFormatter AUTH_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd");
    protected static final MediaType MEDIA_TYPE = MediaType.parse("application/json");

    private final DLFTokenLoader tokenLoader;

    protected DLFToken token;
    private final boolean keepRefreshed;
    private Long expiresAtMillis;
    private final Long tokenRefreshInMills;
    private final String region;

    public static DLFAuthProvider buildRefreshToken(
            DLFTokenLoader tokenLoader, Long tokenRefreshInMills, String region) {
        DLFToken token = tokenLoader.loadToken();
        Long expiresAtMillis = getExpirationInMills(token.getExpiration());
        return new DLFAuthProvider(
                tokenLoader, token, true, expiresAtMillis, tokenRefreshInMills, region);
    }

    public static DLFAuthProvider buildAKToken(
            String accessKeyId, String accessKeySecret, String securityToken, String region) {
        DLFToken token = new DLFToken(accessKeyId, accessKeySecret, securityToken, null);
        return new DLFAuthProvider(null, token, false, null, null, region);
    }

    public DLFAuthProvider(
            DLFTokenLoader tokenLoader,
            DLFToken token,
            boolean keepRefreshed,
            Long expiresAtMillis,
            Long tokenRefreshInMills,
            String region) {
        this.tokenLoader = tokenLoader;
        this.token = token;
        this.keepRefreshed = keepRefreshed;
        this.expiresAtMillis = expiresAtMillis;
        this.tokenRefreshInMills = tokenRefreshInMills;
        this.region = region;
    }

    @Override
    public Map<String, String> header(
            Map<String, String> baseHeader, RESTAuthParameter restAuthParameter) {
        try {
            String dateTime =
                    baseHeader.getOrDefault(
                            DLF_DATE_HEADER_KEY.toLowerCase(),
                            ZonedDateTime.now(ZoneOffset.UTC).format(AUTH_DATE_TIME_FORMATTER));
            String date = dateTime.substring(0, 8);
            Map<String, String> signHeaders =
                    generateSignHeaders(
                            restAuthParameter.data(), dateTime, token.getSecurityToken());
            String authorization =
                    DLFAuthSignature.getAuthorization(
                            restAuthParameter, token, region, signHeaders, dateTime, date);
            Map<String, String> headersWithAuth = new HashMap<>(baseHeader);
            headersWithAuth.putAll(signHeaders);
            headersWithAuth.put(DLF_AUTHORIZATION_HEADER_KEY, authorization);
            return headersWithAuth;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> generateSignHeaders(
            String data, String dateTime, String securityToken) throws Exception {
        Map<String, String> signHeaders = new HashMap<>();
        signHeaders.put(DLF_DATE_HEADER_KEY, dateTime);
        signHeaders.put(DLF_CONTENT_SHA56_HEADER_KEY, DLF_CONTENT_SHA56_VALUE);
        signHeaders.put(DLF_AUTH_VERSION_HEADER_KEY, DLFAuthSignature.VERSION);
        if (data != null && !data.isEmpty()) {
            signHeaders.put(DLF_CONTENT_TYPE_KEY, MEDIA_TYPE.toString());
            signHeaders.put(DLF_CONTENT_MD5_HEADER_KEY, DLFAuthSignature.md5(data));
        }
        if (securityToken != null) {
            signHeaders.put(DLF_SECURITY_TOKEN_HEADER_KEY, securityToken);
        }
        return signHeaders;
    }

    @Override
    public boolean refresh() {
        long start = System.currentTimeMillis();
        DLFToken newToken = tokenLoader.loadToken();
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

    private static Long getExpirationInMills(String dateStr) {
        try {
            if (dateStr == null) {
                return null;
            }
            LocalDateTime dateTime = LocalDateTime.parse(dateStr, TOKEN_DATE_FORMATTER);
            return dateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
