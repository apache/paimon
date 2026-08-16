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

import java.time.Instant;
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
    private final String uri;
    private final String region;
    private final String signingAlgorithm;

    @Nullable protected volatile DLFToken token;
    private final DLFRequestSigner signer;

    public static DLFAuthProvider fromTokenLoader(
            DLFTokenLoader tokenLoader, String uri, String region, String signingAlgorithm) {
        return new DLFAuthProvider(tokenLoader, null, uri, region, signingAlgorithm);
    }

    public static DLFAuthProvider fromAccessKey(
            String accessKeyId,
            String accessKeySecret,
            @Nullable String securityToken,
            String uri,
            String region,
            String signingAlgorithm) {
        DLFToken token = new DLFToken(accessKeyId, accessKeySecret, securityToken, null);
        return new DLFAuthProvider(null, token, uri, region, signingAlgorithm);
    }

    public DLFAuthProvider(
            @Nullable DLFTokenLoader tokenLoader,
            @Nullable DLFToken token,
            String uri,
            String region,
            String signingAlgorithm) {
        this.tokenLoader = tokenLoader;
        this.token = token;
        this.uri = uri;
        this.region = region;
        this.signingAlgorithm = signingAlgorithm;
        this.signer = createSigner(signingAlgorithm);
    }

    @Override
    public Map<String, String> mergeAuthHeader(
            Map<String, String> baseHeader, RESTAuthParameter restAuthParameter) {
        DLFToken token = getFreshToken();
        try {
            Instant now = Instant.now();
            String host = extractHost(uri);
            Map<String, String> signHeaders =
                    signer.signHeaders(
                            restAuthParameter.data(), now, token.getSecurityToken(), host);
            String authorization =
                    signer.authorization(restAuthParameter, token, host, signHeaders);
            Map<String, String> headersWithAuth = new HashMap<>(baseHeader);
            headersWithAuth.putAll(signHeaders);
            headersWithAuth.put(DLF_AUTHORIZATION_HEADER_KEY, authorization);
            return headersWithAuth;
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate authorization header", e);
        }
    }

    /**
     * Extracts the host (with port if present) from a URI string.
     *
     * <p>Handles URIs in the following formats:
     *
     * <ul>
     *   <li>http://hostname/prefix -> hostname
     *   <li>https://hostname:8080/prefix -> hostname:8080
     *   <li>http://hostname -> hostname
     *   <li>https://hostname:8080 -> hostname:8080
     * </ul>
     *
     * @param uri the URI string
     * @return the host part (with port if present) of the URI
     */
    @VisibleForTesting
    static String extractHost(String uri) {
        // Remove protocol (http:// or https://)
        String withoutProtocol = uri.replaceFirst("^https?://", "");

        // Remove path (everything after '/')
        int pathIndex = withoutProtocol.indexOf('/');
        return pathIndex >= 0 ? withoutProtocol.substring(0, pathIndex) : withoutProtocol;
    }

    private DLFRequestSigner createSigner(String signingAlgorithm) {
        switch (signingAlgorithm) {
            case DLFDefaultSigner.IDENTIFIER:
                return new DLFDefaultSigner(region);
            case DLFOpenApiSigner.IDENTIFIER:
                return new DLFOpenApiSigner();
            default:
                throw new IllegalArgumentException(
                        "Unknown DLF signing algorithm: "
                                + signingAlgorithm
                                + ". Supported: "
                                + DLFDefaultSigner.IDENTIFIER
                                + ", "
                                + DLFOpenApiSigner.IDENTIFIER);
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
}
