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

import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.paimon.rest.RESTUtil.decodeString;

/**
 * Signer for Aliyun OpenAPI (product code: DlfNext/2026-01-18).
 *
 * <p>Reference: https://help.aliyun.com/zh/sdk/product-overview/roa-mechanism
 */
public class DLFOpenApiSigner implements DLFRequestSigner {

    public static final String IDENTIFIER = "openapi";

    private static final String HMAC_SHA1 = "HmacSHA1";
    private static final String DATE_HEADER = "Date";
    private static final String ACCEPT_HEADER = "Accept";
    private static final String CONTENT_MD5_HEADER = "Content-MD5";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String HOST_HEADER = "Host";
    private static final String X_ACS_SIGNATURE_METHOD = "x-acs-signature-method";
    private static final String X_ACS_SIGNATURE_NONCE = "x-acs-signature-nonce";
    private static final String X_ACS_SIGNATURE_VERSION = "x-acs-signature-version";
    private static final String X_ACS_VERSION = "x-acs-version";
    private static final String X_ACS_SECURITY_TOKEN = "x-acs-security-token";

    private static final String ACCEPT_VALUE = "application/json";
    private static final String CONTENT_TYPE_VALUE = "application/json";
    private static final String SIGNATURE_METHOD_VALUE = "HMAC-SHA1";
    private static final String SIGNATURE_VERSION_VALUE = "1.0";
    private static final String API_VERSION = "2026-01-18";

    private static final DateTimeFormatter GMT_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
                    .withZone(ZoneId.of("GMT"));

    @Override
    public Map<String, String> signHeaders(
            @Nullable String body, Instant now, @Nullable String securityToken, String host) {
        // Parameter validation
        if (now == null) {
            throw new IllegalArgumentException("Parameter 'now' cannot be null");
        }
        if (host == null) {
            throw new IllegalArgumentException("Parameter 'host' cannot be null");
        }

        Map<String, String> headers = new HashMap<>();

        // Date header (GMT format)
        String dateStr = GMT_DATE_FORMATTER.format(now.atZone(ZoneId.of("GMT")));
        headers.put(DATE_HEADER, dateStr);

        // Accept header
        headers.put(ACCEPT_HEADER, ACCEPT_VALUE);

        // Content-MD5 (if body exists)
        if (body != null && !body.isEmpty()) {
            try {
                headers.put(CONTENT_MD5_HEADER, md5Base64(body));
                headers.put(CONTENT_TYPE_HEADER, CONTENT_TYPE_VALUE);
            } catch (Exception e) {
                throw new RuntimeException("Failed to calculate Content-MD5", e);
            }
        }

        // Host header
        headers.put(HOST_HEADER, host);

        // x-acs-* headers
        headers.put(X_ACS_SIGNATURE_METHOD, SIGNATURE_METHOD_VALUE);

        // Enhanced nonce: UUID + timestamp + thread ID
        String nonce = generateUniqueNonce();
        headers.put(X_ACS_SIGNATURE_NONCE, nonce);

        headers.put(X_ACS_SIGNATURE_VERSION, SIGNATURE_VERSION_VALUE);
        headers.put(X_ACS_VERSION, API_VERSION);

        // Security token (if present)
        if (securityToken != null) {
            headers.put(X_ACS_SECURITY_TOKEN, securityToken);
        }

        return headers;
    }

    /**
     * Generates a unique nonce: UUID + timestamp + thread ID.
     *
     * @return unique nonce string
     */
    private String generateUniqueNonce() {
        StringBuilder uniqueNonce = new StringBuilder();
        UUID uuid = UUID.randomUUID();
        uniqueNonce.append(uuid.toString());
        uniqueNonce.append(System.currentTimeMillis());
        uniqueNonce.append(Thread.currentThread().getId());
        return uniqueNonce.toString();
    }

    @Override
    public String authorization(
            RESTAuthParameter restAuthParameter,
            DLFToken token,
            String host,
            Map<String, String> signHeaders)
            throws Exception {
        // Parameter validation
        if (restAuthParameter == null) {
            throw new IllegalArgumentException("Parameter 'restAuthParameter' cannot be null");
        }
        if (token == null) {
            throw new IllegalArgumentException("Parameter 'token' cannot be null");
        }
        if (host == null) {
            throw new IllegalArgumentException("Parameter 'host' cannot be null");
        }
        if (signHeaders == null) {
            throw new IllegalArgumentException("Parameter 'signHeaders' cannot be null");
        }

        // Step 1: Build CanonicalizedHeaders (x-acs-* headers, sorted, lowercase)
        String canonicalizedHeaders = buildCanonicalizedHeaders(signHeaders);

        // Step 2: Build CanonicalizedResource (path + sorted query string)
        String canonicalizedResource = buildCanonicalizedResource(restAuthParameter);

        // Step 3: Build StringToSign
        String stringToSign =
                buildStringToSign(
                        restAuthParameter,
                        signHeaders,
                        canonicalizedHeaders,
                        canonicalizedResource);

        // Step 4: Calculate signature
        String signature = calculateSignature(stringToSign, token.getAccessKeySecret());

        // Step 5: Build Authorization header
        return "acs " + token.getAccessKeyId() + ":" + signature;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private static String buildCanonicalizedHeaders(Map<String, String> headers) {
        TreeMap<String, String> sortedHeaders = new TreeMap<>();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey().toLowerCase();
            if (key.startsWith("x-acs-")) {
                sortedHeaders.put(key, StringUtils.trim(entry.getValue()));
            }
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : sortedHeaders.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    private static String buildCanonicalizedResource(RESTAuthParameter restAuthParameter) {
        // Decode the path and use the original unencoded path for signature calculation
        // For paths containing special characters like $ (encoded as %24)
        String path = decodeString(restAuthParameter.resourcePath());
        Map<String, String> params = restAuthParameter.parameters();

        if (params == null || params.isEmpty()) {
            return path;
        }

        // Sort query parameters by key
        TreeMap<String, String> sortedParams = new TreeMap<>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            sortedParams.put(
                    entry.getKey(), entry.getValue() != null ? decodeString(entry.getValue()) : "");
        }

        // Build query string
        StringBuilder queryString = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : sortedParams.entrySet()) {
            if (!first) {
                queryString.append("&");
            }
            queryString.append(entry.getKey());
            String value = entry.getValue();
            if (value != null && !value.isEmpty()) {
                queryString.append("=").append(value);
            }
            first = false;
        }

        return path + "?" + queryString.toString();
    }

    private static String buildStringToSign(
            RESTAuthParameter restAuthParameter,
            Map<String, String> headers,
            String canonicalizedHeaders,
            String canonicalizedResource) {
        StringBuilder sb = new StringBuilder();

        // HTTPMethod
        sb.append(restAuthParameter.method()).append("\n");

        // Accept
        String accept = headers.getOrDefault(ACCEPT_HEADER, "");
        sb.append(accept).append("\n");

        // Content-MD5
        String contentMd5 = headers.getOrDefault(CONTENT_MD5_HEADER, "");
        sb.append(contentMd5).append("\n");

        // Content-Type
        String contentType = headers.getOrDefault(CONTENT_TYPE_HEADER, "");
        sb.append(contentType).append("\n");

        // Date
        String date = headers.get(DATE_HEADER);
        sb.append(date).append("\n");

        // CanonicalizedHeaders
        sb.append(canonicalizedHeaders);

        // CanonicalizedResource
        sb.append(canonicalizedResource);

        return sb.toString();
    }

    private static String calculateSignature(String stringToSign, String accessKeySecret)
            throws Exception {
        Mac mac = Mac.getInstance(HMAC_SHA1);
        SecretKeySpec secretKeySpec =
                new SecretKeySpec(accessKeySecret.getBytes(StandardCharsets.UTF_8), HMAC_SHA1);
        mac.init(secretKeySpec);
        byte[] signatureBytes = mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(signatureBytes);
    }

    private static String md5Base64(String data) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] md5Bytes = md.digest(data.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(md5Bytes);
    }
}
