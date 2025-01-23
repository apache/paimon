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

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** generate authorization for dlf. */
public class DlfAuthSignature {
    private static final String SIGNATURE_ALGORITHM = "DLF4-HMAC-SHA256";
    private static final String PAYLOAD = "UNSIGNED-PAYLOAD";
    private static final String PRODUCT = "DlfNext";
    private static final String CHARSET_NAME = "UTF-8";

    public static String getAuthorization(
            String pathname,
            String method,
            Map<String, String> query,
            Map<String, String> headers,
            String accessKeySecret,
            String securityToken,
            String accessKeyId,
            String region,
            String date)
            throws Exception {
        byte[] signingKey = getSigningKey(accessKeySecret, PRODUCT, region, date);
        String signature = getSignature(pathname, method, query, headers, signingKey);
        Map<String, String> h = new HashMap<>(headers);
        if (securityToken != null) {
            h.put("x-acs-accesskey-id", accessKeyId);
            h.put("x-acs-security-token", securityToken);
        }
        List<String> signedHeaders = getSignedHeaders(h);
        String signedHeadersStr = Joiner.on(";").join(signedHeaders);
        return SIGNATURE_ALGORITHM
                + " Credential="
                + accessKeyId
                + "/"
                + date
                + "/"
                + region
                + "/"
                + PRODUCT
                + "/aliyun_v4_request,SignedHeaders="
                + signedHeadersStr
                + ",Signature="
                + signature
                + "";
    }

    private static String getSignature(
            String pathname,
            String method,
            Map<String, String> query,
            Map<String, String> headers,
            byte[] signingkey)
            throws Exception {
        String canonicalURI = "/";
        if (!StringUtils.isEmpty(pathname)) {
            canonicalURI = pathname;
        }

        String stringToSign = "";
        String canonicalizedResource = buildCanonicalizedResource(query);
        String canonicalizedHeaders = buildCanonicalizedHeaders(headers);
        List<String> signedHeaders = getSignedHeaders(headers);
        String signedHeadersStr = Joiner.on(";").join(signedHeaders);
        stringToSign =
                Joiner.on("\n")
                        .join(
                                method,
                                canonicalURI,
                                canonicalizedResource,
                                canonicalizedHeaders,
                                signedHeadersStr,
                                PAYLOAD);
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(stringToSign.getBytes(CHARSET_NAME));
        String hex = hexEncode(hash);
        stringToSign = SIGNATURE_ALGORITHM + "\n" + hex;
        byte[] signature = hmacSHA256SignByBytes(stringToSign, signingkey);
        return hexEncode(signature);
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

    private static String buildCanonicalizedHeaders(Map<String, String> headers) throws Exception {
        String canonicalizedHeaders = "";
        List<String> sortedHeaders = getSignedHeaders(headers);
        for (String header : sortedHeaders) {
            canonicalizedHeaders =
                    canonicalizedHeaders + header + ":" + headers.get(header).trim() + "\n";
        }
        return canonicalizedHeaders;
    }

    private static String buildCanonicalizedResource(Map<String, String> query) throws Exception {
        String canonicalizedResource = "";
        if (query != null) {
            List<String> queryArray = new ArrayList(query.keySet());
            List<String> sortedQueryArray = ascSort(queryArray);
            String separator = "";
            for (String key : sortedQueryArray) {
                canonicalizedResource = canonicalizedResource + separator + percentEncode(key);
                if (!StringUtils.isEmpty(query.get(key))) {
                    canonicalizedResource =
                            canonicalizedResource + "=" + percentEncode(query.get(key));
                }

                separator = "&";
            }
        }

        return canonicalizedResource;
    }

    private static String percentEncode(String raw) throws UnsupportedEncodingException {
        return raw != null
                ? URLEncoder.encode(raw, CHARSET_NAME)
                        .replace("+", "%20")
                        .replace("*", "%2A")
                        .replace("%7E", "~")
                : null;
    }

    private static List<String> getSignedHeaders(Map<String, String> headers) throws Exception {
        List<String> headersArray = new ArrayList(headers.keySet());
        List<String> sortedHeadersArray = ascSort(headersArray);
        String tmp = "";
        String separator = "";
        for (String key : sortedHeadersArray) {
            String lowerKey = key.toLowerCase();
            if (lowerKey.startsWith("x-acs-")
                    || "host".equals(lowerKey)
                    || "content-type".equals(lowerKey)) {
                if (!tmp.contains(lowerKey)) {
                    tmp = tmp + separator + lowerKey;
                    separator = ";";
                }
            }
        }
        return split(tmp, ";", null);
    }

    private static List<String> split(String raw, String sep, Integer limit) {
        return limit == null
                ? Arrays.asList(raw.split(Pattern.quote(sep)))
                : Arrays.asList(raw.split(Pattern.quote(sep), limit));
    }

    private static List<String> ascSort(List<String> raw) {
        if (null == raw) {
            throw new IllegalArgumentException("not a valid value for parameter");
        } else {
            String[] sorted = raw.toArray(new String[raw.size()]);
            Arrays.sort(sorted);
            return Arrays.asList(sorted);
        }
    }

    private static byte[] getSigningKey(String secret, String product, String region, String date)
            throws Exception {
        String sc1 = "aliyun_v4" + secret;
        byte[] sc2 = hmacSHA256Sign(date, sc1);
        byte[] sc3 = hmacSHA256SignByBytes(region, sc2);
        byte[] sc4 = hmacSHA256SignByBytes(product, sc3);
        return hmacSHA256SignByBytes("aliyun_v4_request", sc4);
    }

    private static byte[] hmacSHA256Sign(String stringToSign, String secret) throws Exception {
        return hmacSHA256SignByBytes(stringToSign, secret.getBytes(CHARSET_NAME));
    }

    private static byte[] hmacSHA256SignByBytes(String stringToSign, byte[] secret)
            throws Exception {
        Mac sha256HMAC = Mac.getInstance("HmacSHA256");
        SecretKeySpec secretKey = new SecretKeySpec(secret, "HmacSHA256");
        sha256HMAC.init(secretKey);
        byte[] signData = sha256HMAC.doFinal(stringToSign.getBytes());
        return signData;
    }
}
