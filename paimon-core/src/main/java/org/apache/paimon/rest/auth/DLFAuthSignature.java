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

import org.apache.paimon.shade.guava30.com.google.common.base.Joiner;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.security.MessageDigest;

import static java.nio.charset.StandardCharsets.UTF_8;

/** generate authorization for <b>Ali CLoud</b> DLF. */
public class DLFAuthSignature {

    private static final String SIGNATURE_ALGORITHM = "DLF4-HMAC-SHA256";
    private static final String PAYLOAD = "UNSIGNED-PAYLOAD";
    private static final String PRODUCT = "DlfNext";
    private static final String HMAC_SHA256 = "HmacSHA256";
    private static final String REQUEST_TYPE = "aliyun_v4_request";
    private static final String ADDITIONAL_HEADERS = "AdditionalHeaders";
    private static final String SIGNATURE_KEY = "Signature";

    public static String getAuthorization(
            RESTAuthParameter restAuthParameter, DLFToken dlfToken, String dataMd5Hex, String date)
            throws Exception {
        String canonicalRequest = getCanonicalRequest(restAuthParameter, dataMd5Hex, date);
        String stringToSign =
                Joiner.on("\n")
                        .join(
                                SIGNATURE_ALGORITHM,
                                String.format("%s/%s/%s", date, PRODUCT, REQUEST_TYPE),
                                sha256Hex(canonicalRequest));
        byte[] dateKey = hmacSha256(("aliyun_v4" + dlfToken.getAccessKeySecret()).getBytes(), date);
        byte[] dateRegionServiceKey = hmacSha256(dateKey, PRODUCT);
        byte[] signingKey = hmacSha256(dateRegionServiceKey, REQUEST_TYPE);
        byte[] result = hmacSha256(signingKey, stringToSign);
        String signature = hexEncode(result);
        String authorization =
                Joiner.on(",")
                        .join(
                                String.format(
                                        "%s Credential=%s/%s/%s/%s",
                                        SIGNATURE_ALGORITHM,
                                        dlfToken.getAccessKeyId(),
                                        date,
                                        PRODUCT,
                                        REQUEST_TYPE),
                                String.format(
                                        "%s=%s",
                                        ADDITIONAL_HEADERS, DLFAuthProvider.DLF_HOST_HEADER_KEY),
                                String.format("%s=%s", SIGNATURE_KEY, signature));
        return authorization;
    }

    public static String md5Hex(String raw) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        byte[] hash = digest.digest(raw.getBytes(UTF_8.name()));
        return hexEncode(hash);
    }

    private static byte[] hmacSha256(byte[] key, String data) {
        try {
            SecretKeySpec secretKeySpec = new SecretKeySpec(key, HMAC_SHA256);
            Mac mac = Mac.getInstance(HMAC_SHA256);
            mac.init(secretKeySpec);
            byte[] hmacBytes = mac.doFinal(data.getBytes());
            return hmacBytes;
        } catch (Exception e) {
            throw new RuntimeException("Failed to calculate HMAC-SHA256", e);
        }
    }

    private static String getCanonicalRequest(
            RESTAuthParameter restAuthParameter, String dataMd5Hex, String date) throws Exception {
        return Joiner.on("\n")
                .join(
                        restAuthParameter.method(),
                        restAuthParameter.path(),
                        String.format(
                                "%s:%s", DLFAuthProvider.DLF_DATA_MD5_HEX_HEADER_KEY, dataMd5Hex),
                        String.format(
                                "%s:%s",
                                DLFAuthProvider.DLF_HOST_HEADER_KEY, restAuthParameter.host()),
                        String.format("%s:%s", DLFAuthProvider.DLF_DATE_HEADER_KEY, date),
                        DLFAuthProvider.DLF_HOST_HEADER_KEY,
                        PAYLOAD);
    }

    private static String sha256Hex(String raw) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(raw.getBytes(UTF_8.name()));
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
