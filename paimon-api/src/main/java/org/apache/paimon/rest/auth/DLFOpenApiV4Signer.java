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

import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.rest.RESTUtil.decodeString;

/**
 * Signer for Aliyun OpenAPI V4 requests, using the ACS4-HMAC-SHA256 algorithm.
 *
 * <p>Unlike {@link DLFOpenApiSigner}, which signs a string built from the {@code Date} and {@code
 * Content-MD5} headers with HMAC-SHA1, this signer hashes a canonical request and signs it with a
 * key derived from the date, the region and the product, the way the POP gateway does.
 *
 * <p>Reference: https://github.com/aliyun/alibabacloud-gateway/tree/master/alibabacloud-gateway-pop
 */
public class DLFOpenApiV4Signer implements DLFRequestSigner {

    public static final String IDENTIFIER = "openapi-v4";

    private static final String SIGNATURE_ALGORITHM = "ACS4-HMAC-SHA256";
    private static final String HMAC_SHA256 = "HmacSHA256";

    /** Key derivation prefix and terminator, shared with the DLF4 scheme. */
    private static final String SIGN_PREFIX = "aliyun_v4";

    private static final String REQUEST_TYPE = SIGN_PREFIX + "_request";

    /** POP product code; it is the third level of the credential scope. */
    private static final String PRODUCT = "DlfNext";

    private static final String HOST_HEADER = "host";
    private static final String CONTENT_TYPE_HEADER = "content-type";
    private static final String X_ACS_DATE = "x-acs-date";
    private static final String X_ACS_SIGNATURE_NONCE = "x-acs-signature-nonce";
    private static final String X_ACS_VERSION = "x-acs-version";
    private static final String X_ACS_CONTENT_SHA256 = "x-acs-content-sha256";
    private static final String X_ACS_SECURITY_TOKEN = "x-acs-security-token";
    private static final String X_ACS_ACTION = "x-acs-action";

    private static final String CONTENT_TYPE_VALUE = "application/json";
    private static final String API_VERSION = "2026-01-18";

    private static final DateTimeFormatter ACS_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    private static final String EMPTY_BODY_SHA256 =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private final String region;

    public DLFOpenApiV4Signer(String region) {
        if (region == null) {
            throw new IllegalArgumentException("Parameter 'region' cannot be null");
        }
        this.region = region;
    }

    @Override
    public Map<String, String> signHeaders(
            @Nullable String body, Instant now, @Nullable String securityToken, String host) {
        if (now == null) {
            throw new IllegalArgumentException("Parameter 'now' cannot be null");
        }
        if (host == null) {
            throw new IllegalArgumentException("Parameter 'host' cannot be null");
        }

        Map<String, String> headers = new HashMap<>();

        // x-acs-date (ISO 8601, always UTC)
        headers.put(
                X_ACS_DATE,
                ACS_DATE_FORMATTER.format(ZonedDateTime.ofInstant(now, ZoneOffset.UTC)));

        headers.put(HOST_HEADER, host);

        // An absent body hashes to the well-known SHA-256 of the empty string
        if (body != null && !body.isEmpty()) {
            headers.put(X_ACS_CONTENT_SHA256, sha256Hex(body));
            headers.put(CONTENT_TYPE_HEADER, CONTENT_TYPE_VALUE);
        } else {
            headers.put(X_ACS_CONTENT_SHA256, EMPTY_BODY_SHA256);
        }

        headers.put(X_ACS_SIGNATURE_NONCE, generateUniqueNonce());
        headers.put(X_ACS_VERSION, API_VERSION);

        if (securityToken != null) {
            headers.put(X_ACS_SECURITY_TOKEN, securityToken);
        }

        return headers;
    }

    /** Adds the API name as a signed {@code x-acs-action}, which POP gateways may need to route. */
    @Override
    public Map<String, String> signRequestHeaders(
            RESTAuthParameter restAuthParameter,
            Instant now,
            @Nullable String securityToken,
            String host) {
        if (restAuthParameter == null) {
            throw new IllegalArgumentException("Parameter 'restAuthParameter' cannot be null");
        }
        Map<String, String> headers =
                signHeaders(restAuthParameter.data(), now, securityToken, host);
        if (restAuthParameter.apiName() != null) {
            headers.put(X_ACS_ACTION, restAuthParameter.apiName());
        }
        return headers;
    }

    /** Generates a unique nonce: UUID + timestamp + thread ID. */
    private static String generateUniqueNonce() {
        return UUID.randomUUID().toString()
                + System.currentTimeMillis()
                + Thread.currentThread().getId();
    }

    @Override
    public String authorization(
            RESTAuthParameter restAuthParameter,
            DLFToken token,
            String host,
            Map<String, String> signHeaders)
            throws Exception {
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

        CanonicalHeadersResult canonicalHeadersResult = buildCanonicalHeaders(signHeaders);
        String canonicalQueryString = buildCanonicalQueryString(restAuthParameter.parameters());

        String canonicalUri = restAuthParameter.resourcePath();
        if (canonicalUri == null || canonicalUri.trim().isEmpty()) {
            canonicalUri = "/";
        }

        String hashedPayload = signHeaders.getOrDefault(X_ACS_CONTENT_SHA256, EMPTY_BODY_SHA256);

        String canonicalRequest =
                restAuthParameter.method()
                        + "\n"
                        + canonicalUri
                        + "\n"
                        + canonicalQueryString
                        + "\n"
                        + canonicalHeadersResult.canonicalHeaders
                        + "\n"
                        + canonicalHeadersResult.signedHeaders
                        + "\n"
                        + hashedPayload;

        String stringToSign = SIGNATURE_ALGORITHM + "\n" + sha256Hex(canonicalRequest);

        // The credential scope date comes from the signed x-acs-date, so the two cannot drift
        String date = credentialScopeDate(signHeaders.get(X_ACS_DATE));
        byte[] signingKey = signingKey(token.getAccessKeySecret(), date);
        String signature = hexEncode(hmacSha256(signingKey, stringToSign));

        return SIGNATURE_ALGORITHM
                + " Credential="
                + token.getAccessKeyId()
                + "/"
                + date
                + "/"
                + region
                + "/"
                + PRODUCT
                + "/"
                + REQUEST_TYPE
                + ",SignedHeaders="
                + canonicalHeadersResult.signedHeaders
                + ",Signature="
                + signature;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /** Turns {@code 2025-04-16T03:44:46Z} into {@code 20250416}. */
    private static String credentialScopeDate(@Nullable String acsDate) {
        if (acsDate == null || acsDate.length() < 10) {
            throw new IllegalArgumentException("Header '" + X_ACS_DATE + "' is missing or invalid");
        }
        return acsDate.substring(0, 10).replace("-", "");
    }

    /** Derives the signing key: secret, then date, region, product and request type in turn. */
    private byte[] signingKey(String accessKeySecret, String date) throws Exception {
        byte[] dateKey =
                hmacSha256((SIGN_PREFIX + accessKeySecret).getBytes(StandardCharsets.UTF_8), date);
        byte[] dateRegionKey = hmacSha256(dateKey, region);
        byte[] dateRegionProductKey = hmacSha256(dateRegionKey, PRODUCT);
        return hmacSha256(dateRegionProductKey, REQUEST_TYPE);
    }

    /** The canonical header block and the {@code SignedHeaders} list that describes it. */
    static class CanonicalHeadersResult {
        final String canonicalHeaders;
        final String signedHeaders;

        CanonicalHeadersResult(String canonicalHeaders, String signedHeaders) {
            this.canonicalHeaders = canonicalHeaders;
            this.signedHeaders = signedHeaders;
        }
    }

    /** Signs host, content-type and every x-acs-* header, sorted and lowercased. */
    private static CanonicalHeadersResult buildCanonicalHeaders(Map<String, String> headers) {
        List<String> canonicalizedKeys = new ArrayList<>();
        Map<String, String> valueMap = new HashMap<>();

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String lowerKey = entry.getKey().toLowerCase();
            if (lowerKey.startsWith("x-acs-")
                    || lowerKey.equals(HOST_HEADER)
                    || lowerKey.equals(CONTENT_TYPE_HEADER)) {
                if (!canonicalizedKeys.contains(lowerKey)) {
                    canonicalizedKeys.add(lowerKey);
                }
                valueMap.put(lowerKey, entry.getValue().trim());
            }
        }

        String[] sortedKeys = canonicalizedKeys.toArray(new String[0]);
        Arrays.sort(sortedKeys);

        StringBuilder sb = new StringBuilder();
        for (String key : sortedKeys) {
            sb.append(key).append(":").append(valueMap.get(key)).append("\n");
        }

        return new CanonicalHeadersResult(sb.toString(), String.join(";", sortedKeys));
    }

    /** Build canonical query string with percent-encoding (RFC 3986). */
    private static String buildCanonicalQueryString(Map<String, String> params) throws Exception {
        if (params == null || params.isEmpty()) {
            return "";
        }

        String[] keys = params.keySet().toArray(new String[0]);
        Arrays.sort(keys);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) {
                sb.append("&");
            }
            sb.append(percentEncode(keys[i]));
            sb.append("=");
            String value = params.get(keys[i]);
            if (value != null && !value.isEmpty()) {
                // RESTAuthParameter has already encoded the value once; decode it so the
                // canonical form matches what the gateway rebuilds from the wire
                sb.append(percentEncode(decodeString(value)));
            }
        }
        return sb.toString();
    }

    /** RFC 3986 percent-encoding. */
    static String percentEncode(String value) throws UnsupportedEncodingException {
        if (value == null) {
            return null;
        }
        return URLEncoder.encode(value, "UTF-8")
                .replace("+", "%20")
                .replace("*", "%2A")
                .replace("%7E", "~");
    }

    private static String sha256Hex(String data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return hexEncode(digest.digest(data.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to calculate SHA-256", e);
        }
    }

    private static byte[] hmacSha256(byte[] key, String data) throws Exception {
        Mac mac = Mac.getInstance(HMAC_SHA256);
        mac.init(new SecretKeySpec(key, HMAC_SHA256));
        return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    }

    private static String hexEncode(byte[] raw) {
        if (raw == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : raw) {
            String hex = Integer.toHexString(b & 0xFF);
            if (hex.length() < 2) {
                sb.append(0);
            }
            sb.append(hex);
        }
        return sb.toString();
    }
}
