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

import okhttp3.MediaType;

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
    private static final long[] READ_TOKEN_FILE_BACKOFF_WAIT_TIME_MILLIS = {1_000, 3_000, 5_000};

    private final String tokenFilePath;

    protected DLFToken token;
    private final boolean keepRefreshed;
    private Long expiresAtMillis;
    private final Long tokenRefreshInMills;
    private final String region;

    public static DLFAuthProvider buildRefreshToken(
            String tokenFilePath, Long tokenRefreshInMills, String region) {
        DLFToken token = readToken(tokenFilePath, 0);
        Long expiresAtMillis = getExpirationInMills(token.getExpiration());
        return new DLFAuthProvider(
                tokenFilePath, token, true, expiresAtMillis, tokenRefreshInMills, region);
    }

    public static DLFAuthProvider buildAKToken(
            String accessKeyId, String accessKeySecret, String securityToken, String region) {
        DLFToken token = new DLFToken(accessKeyId, accessKeySecret, securityToken, null);
        return new DLFAuthProvider(null, token, false, null, null, region);
    }

    public DLFAuthProvider(
            String tokenFilePath,
            DLFToken token,
            boolean keepRefreshed,
            Long expiresAtMillis,
            Long tokenRefreshInMills,
            String region) {
        this.tokenFilePath = tokenFilePath;
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
            ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
            String date = now.format(AUTH_DATE_FORMATTER);
            String dateTime = now.format(AUTH_DATE_TIME_FORMATTER);
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
        DLFToken newToken = readToken(tokenFilePath, 0);
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

    protected static DLFToken readToken(String tokenFilePath, int retryTimes) {
        try {
            File tokenFile = new File(tokenFilePath);
            if (tokenFile.exists()) {
                String tokenStr = FileIOUtils.readFileUtf8(tokenFile);
                return OBJECT_MAPPER.readValue(tokenStr, DLFToken.class);
            } else if (retryTimes < READ_TOKEN_FILE_BACKOFF_WAIT_TIME_MILLIS.length - 1) {
                Thread.sleep(READ_TOKEN_FILE_BACKOFF_WAIT_TIME_MILLIS[retryTimes]);
                return readToken(tokenFilePath, retryTimes + 1);
            } else {
                throw new FileNotFoundException(tokenFilePath);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
