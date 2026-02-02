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

import org.apache.paimon.shade.guava30.com.google.common.base.Joiner;

import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.paimon.rest.auth.DLFAuthProvider.DLF_AUTH_VERSION_HEADER_KEY;
import static org.apache.paimon.rest.auth.DLFAuthProvider.DLF_CONTENT_MD5_HEADER_KEY;
import static org.apache.paimon.rest.auth.DLFAuthProvider.DLF_CONTENT_SHA56_HEADER_KEY;
import static org.apache.paimon.rest.auth.DLFAuthProvider.DLF_CONTENT_TYPE_KEY;
import static org.apache.paimon.rest.auth.DLFAuthProvider.DLF_DATE_HEADER_KEY;
import static org.apache.paimon.rest.auth.DLFAuthProvider.DLF_SECURITY_TOKEN_HEADER_KEY;

/**
 * Signer for DLF default VPC endpoint authentication. This is the default signer for backward
 * compatibility.
 */
public class DLFDefaultSigner implements DLFRequestSigner {

    public static final String IDENTIFIER = "default";
    public static final String VERSION = "v1";

    private static final String SIGNATURE_ALGORITHM = "DLF4-HMAC-SHA256";
    private static final String PRODUCT = "DlfNext";
    private static final String HMAC_SHA256 = "HmacSHA256";
    private static final String REQUEST_TYPE = "aliyun_v4_request";
    private static final String SIGNATURE_KEY = "Signature";
    private static final String NEW_LINE = "\n";
    private static final List<String> SIGNED_HEADERS =
            Arrays.asList(
                    DLF_CONTENT_MD5_HEADER_KEY.toLowerCase(),
                    DLF_CONTENT_TYPE_KEY.toLowerCase(),
                    DLF_CONTENT_SHA56_HEADER_KEY.toLowerCase(),
                    DLF_DATE_HEADER_KEY.toLowerCase(),
                    DLF_AUTH_VERSION_HEADER_KEY.toLowerCase(),
                    DLF_SECURITY_TOKEN_HEADER_KEY.toLowerCase());

    private final String region;

    public DLFDefaultSigner(String region) {
        this.region = region;
    }

    @Override
    public Map<String, String> signHeaders(
            @Nullable String body, Instant now, @Nullable String securityToken, String host) {
        try {
            String dateTime =
                    ZonedDateTime.ofInstant(now, ZoneOffset.UTC)
                            .format(DLFAuthProvider.AUTH_DATE_TIME_FORMATTER);
            return generateSignHeaders(body, dateTime, securityToken);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate sign headers", e);
        }
    }

    @Override
    public String authorization(
            RESTAuthParameter restAuthParameter,
            DLFToken token,
            String host,
            Map<String, String> signHeaders)
            throws Exception {
        String dateTime = signHeaders.get(DLFAuthProvider.DLF_DATE_HEADER_KEY);
        String date = dateTime.substring(0, 8);
        return getAuthorization(restAuthParameter, region, token, signHeaders, dateTime, date);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private static Map<String, String> generateSignHeaders(
            String data, String dateTime, String securityToken) throws Exception {
        Map<String, String> signHeaders = new HashMap<>();
        signHeaders.put(DLF_DATE_HEADER_KEY, dateTime);
        signHeaders.put(DLF_CONTENT_SHA56_HEADER_KEY, DLFAuthProvider.DLF_CONTENT_SHA56_VALUE);
        signHeaders.put(DLF_AUTH_VERSION_HEADER_KEY, VERSION);
        if (data != null && !data.isEmpty()) {
            signHeaders.put(DLF_CONTENT_TYPE_KEY, DLFAuthProvider.MEDIA_TYPE);
            signHeaders.put(DLF_CONTENT_MD5_HEADER_KEY, md5(data));
        }
        if (securityToken != null) {
            signHeaders.put(DLF_SECURITY_TOKEN_HEADER_KEY, securityToken);
        }
        return signHeaders;
    }

    private static String getAuthorization(
            RESTAuthParameter restAuthParameter,
            String region,
            DLFToken dlfToken,
            Map<String, String> headers,
            String dateTime,
            String date)
            throws Exception {
        String canonicalRequest = getCanonicalRequest(restAuthParameter, headers);
        String stringToSign =
                Joiner.on(NEW_LINE)
                        .join(
                                SIGNATURE_ALGORITHM,
                                dateTime,
                                String.format("%s/%s/%s/%s", date, region, PRODUCT, REQUEST_TYPE),
                                sha256Hex(canonicalRequest));
        byte[] dateKey = hmacSha256(("aliyun_v4" + dlfToken.getAccessKeySecret()).getBytes(), date);
        byte[] dateRegionKey = hmacSha256(dateKey, region);
        byte[] dateRegionServiceKey = hmacSha256(dateRegionKey, PRODUCT);
        byte[] signingKey = hmacSha256(dateRegionServiceKey, REQUEST_TYPE);
        byte[] result = hmacSha256(signingKey, stringToSign);
        String signature = hexEncode(result);
        return Joiner.on(",")
                .join(
                        String.format(
                                "%s Credential=%s/%s/%s/%s/%s",
                                SIGNATURE_ALGORITHM,
                                dlfToken.getAccessKeyId(),
                                date,
                                region,
                                PRODUCT,
                                REQUEST_TYPE),
                        String.format("%s=%s", SIGNATURE_KEY, signature));
    }

    private static String md5(String raw) throws Exception {
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        messageDigest.update(raw.getBytes(UTF_8));
        byte[] md5 = messageDigest.digest();
        return Base64.getEncoder().encodeToString(md5);
    }

    private static byte[] hmacSha256(byte[] key, String data) {
        try {
            SecretKeySpec secretKeySpec = new SecretKeySpec(key, HMAC_SHA256);
            Mac mac = Mac.getInstance(HMAC_SHA256);
            mac.init(secretKeySpec);
            return mac.doFinal(data.getBytes());
        } catch (Exception e) {
            throw new RuntimeException("Failed to calculate HMAC-SHA256", e);
        }
    }

    private static String getCanonicalRequest(
            RESTAuthParameter restAuthParameter, Map<String, String> headers) {
        String canonicalRequest =
                Joiner.on(NEW_LINE)
                        .join(restAuthParameter.method(), restAuthParameter.resourcePath());
        // Canonical Query String + "\n" +
        TreeMap<String, String> orderMap = new TreeMap<>();
        if (restAuthParameter.parameters() != null) {
            orderMap.putAll(restAuthParameter.parameters());
        }
        String separator = "";
        StringBuilder canonicalPart = new StringBuilder();
        for (Map.Entry<String, String> param : orderMap.entrySet()) {
            canonicalPart.append(separator).append(StringUtils.trim(param.getKey()));
            if (param.getValue() != null && !param.getValue().isEmpty()) {
                canonicalPart.append("=").append((StringUtils.trim(param.getValue())));
            }
            separator = "&";
        }
        canonicalRequest = Joiner.on(NEW_LINE).join(canonicalRequest, canonicalPart);

        // Canonical Headers + "\n" +
        TreeMap<String, String> sortedSignedHeadersMap = buildSortedSignedHeadersMap(headers);
        for (Map.Entry<String, String> header : sortedSignedHeadersMap.entrySet()) {
            canonicalRequest =
                    Joiner.on(NEW_LINE)
                            .join(
                                    canonicalRequest,
                                    String.format("%s:%s", header.getKey(), header.getValue()));
        }
        String contentSha56 =
                headers.getOrDefault(
                        DLF_CONTENT_SHA56_HEADER_KEY, DLFAuthProvider.DLF_CONTENT_SHA56_VALUE);
        return Joiner.on(NEW_LINE).join(canonicalRequest, contentSha56);
    }

    private static TreeMap<String, String> buildSortedSignedHeadersMap(
            Map<String, String> headers) {
        TreeMap<String, String> orderMap = new TreeMap<>();
        if (headers != null) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                String key = header.getKey().toLowerCase();
                if (SIGNED_HEADERS.contains(key)) {
                    orderMap.put(key, StringUtils.trim(header.getValue()));
                }
            }
        }
        return orderMap;
    }

    private static String sha256Hex(String raw) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(raw.getBytes(UTF_8));
        return hexEncode(hash);
    }

    private static String hexEncode(byte[] raw) {
        if (raw == null) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder();

            for (byte b : raw) {
                String hex = Integer.toHexString(b & 255);
                if (hex.length() < 2) {
                    sb.append(0);
                }

                sb.append(hex);
            }

            return sb.toString();
        }
    }
}
