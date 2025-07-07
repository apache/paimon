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

import org.apache.paimon.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.rest.RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Auth provider for <b>Ali CLoud</b> DLF. */
public class DLFAuthProvider implements AuthProvider {

    private static final Logger LOG = LoggerFactory.getLogger(DLFAuthProvider.class);

    public static final String DLF_AUTHORIZATION_HEADER_KEY = "Authorization";
    public static final String DLF_CONTENT_MD5_HEADER_KEY = "Content-MD5";
    public static final String DLF_CONTENT_TYPE_KEY = "Content-Type";
    public static final String DLF_DATE_HEADER_KEY = "x-dlf-date";
    public static final String DLF_SECURITY_TOKEN_HEADER_KEY = "x-dlf-security-token";
    public static final String DLF_AUTH_VERSION_HEADER_KEY = "x-dlf-version";
    public static final String DLF_CONTENT_SHA56_HEADER_KEY = "x-dlf-content-sha256";
    public static final String DLF_CONTENT_SHA56_VALUE = "UNSIGNED-PAYLOAD";

    public static final DateTimeFormatter AUTH_DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");
    protected static final String MEDIA_TYPE = "application/json";

    @Nullable private final DLFTokenLoader tokenLoader;
    private final String region;

    @Nullable protected volatile DLFToken token;

    public static DLFAuthProvider fromTokenLoader(DLFTokenLoader tokenLoader, String region) {
        return new DLFAuthProvider(tokenLoader, null, region);
    }

    public static DLFAuthProvider fromAccessKey(
            String accessKeyId, String accessKeySecret, String securityToken, String region) {
        DLFToken token = new DLFToken(accessKeyId, accessKeySecret, securityToken, null);
        return new DLFAuthProvider(null, token, region);
    }

    public DLFAuthProvider(
            @Nullable DLFTokenLoader tokenLoader, @Nullable DLFToken token, String region) {
        this.tokenLoader = tokenLoader;
        this.token = token;
        this.region = region;
    }

    @Override
    public Map<String, String> mergeAuthHeader(
            Map<String, String> baseHeader, RESTAuthParameter restAuthParameter) {
        DLFToken token = getFreshToken();
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

    @VisibleForTesting
    DLFToken getFreshToken() {
        if (shouldRefresh()) {
            synchronized (this) {
                if (shouldRefresh()) {
                    refreshToken();
                }
            }
        }
        return token;
    }

    private void refreshToken() {
        checkNotNull(tokenLoader);
        LOG.info("begin refresh meta token for loader [{}]", tokenLoader.description());
        this.token = tokenLoader.loadToken();
        checkNotNull(token);
        LOG.info(
                "end refresh meta token for loader [{}] expiresAtMillis [{}]",
                tokenLoader.description(),
                token.getExpirationAtMills());
    }

    private boolean shouldRefresh() {
        // no token, get new one
        if (token == null) {
            return true;
        }
        // never expire
        Long expireTime = token.getExpirationAtMills();
        if (expireTime == null) {
            return false;
        }
        long now = System.currentTimeMillis();
        return expireTime - now < TOKEN_EXPIRATION_SAFE_TIME_MILLIS;
    }

    public static Map<String, String> generateSignHeaders(
            String data, String dateTime, String securityToken) throws Exception {
        Map<String, String> signHeaders = new HashMap<>();
        signHeaders.put(DLF_DATE_HEADER_KEY, dateTime);
        signHeaders.put(DLF_CONTENT_SHA56_HEADER_KEY, DLF_CONTENT_SHA56_VALUE);
        signHeaders.put(DLF_AUTH_VERSION_HEADER_KEY, DLFAuthSignature.VERSION);
        if (data != null && !data.isEmpty()) {
            signHeaders.put(DLF_CONTENT_TYPE_KEY, MEDIA_TYPE);
            signHeaders.put(DLF_CONTENT_MD5_HEADER_KEY, DLFAuthSignature.md5(data));
        }
        if (securityToken != null) {
            signHeaders.put(DLF_SECURITY_TOKEN_HEADER_KEY, securityToken);
        }
        return signHeaders;
    }
}
