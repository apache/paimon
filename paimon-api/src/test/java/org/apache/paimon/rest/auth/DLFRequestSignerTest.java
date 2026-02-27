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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link DLFRequestSigner}. */
public class DLFRequestSignerTest {

    @Test
    public void testOpenApiSignHeadersWithBody() throws Exception {
        DLFOpenApiSigner signer = new DLFOpenApiSigner();
        String body = "{\"CategoryName\":\"test\",\"CategoryType\":\"UNSTRUCTURED\"}";
        Instant now = ZonedDateTime.of(2025, 4, 16, 3, 44, 46, 0, ZoneOffset.UTC).toInstant();
        String host = "dlfnext.cn-beijing.aliyuncs.com";

        Map<String, String> headers = signer.signHeaders(body, now, null, host);

        assertNotNull(headers.get("Date"));
        assertEquals("application/json", headers.get("Accept"));
        assertNotNull(headers.get("Content-MD5"));
        assertEquals("application/json", headers.get("Content-Type"));
        assertEquals(host, headers.get("Host"));
        assertEquals("HMAC-SHA1", headers.get("x-acs-signature-method"));
        assertNotNull(headers.get("x-acs-signature-nonce"));
        assertEquals("1.0", headers.get("x-acs-signature-version"));
        assertEquals("2026-01-18", headers.get("x-acs-version"));
    }

    @Test
    public void testOpenApiSignHeadersWithoutBody() throws Exception {
        DLFOpenApiSigner signer = new DLFOpenApiSigner();
        Instant now = ZonedDateTime.of(2025, 4, 16, 3, 44, 46, 0, ZoneOffset.UTC).toInstant();
        String host = "dlfnext.cn-beijing.aliyuncs.com";

        Map<String, String> headers = signer.signHeaders(null, now, null, host);

        assertNotNull(headers.get("Date"));
        assertEquals("application/json", headers.get("Accept"));
        // Content-MD5 and Content-Type should not be present for empty body
        assertTrue(!headers.containsKey("Content-MD5") || headers.get("Content-MD5").isEmpty());
        assertTrue(!headers.containsKey("Content-Type") || headers.get("Content-Type").isEmpty());
        assertEquals(host, headers.get("Host"));
    }

    @Test
    public void testOpenApiSignHeadersWithSecurityToken() throws Exception {
        DLFOpenApiSigner signer = new DLFOpenApiSigner();
        Instant now = Instant.now();
        String host = "dlfnext.cn-beijing.aliyuncs.com";
        String securityToken = "test-security-token";

        Map<String, String> headers = signer.signHeaders(null, now, securityToken, host);

        assertEquals(securityToken, headers.get("x-acs-security-token"));
    }

    @Test
    public void testOpenApiAuthorization() throws Exception {
        DLFOpenApiSigner signer = new DLFOpenApiSigner();
        String host = "dlfnext.cn-beijing.aliyuncs.com";
        DLFToken token =
                new DLFToken("YourAccessKeyId", "YourAccessKeySecret", "securityToken", null);

        // Fixed timestamp for deterministic test
        Instant now = ZonedDateTime.of(2025, 4, 16, 3, 44, 46, 0, ZoneOffset.UTC).toInstant();
        String body = "{\"CategoryName\":\"test\",\"CategoryType\":\"UNSTRUCTURED\"}";

        Map<String, String> signHeaders =
                signer.signHeaders(body, now, token.getSecurityToken(), host);

        // Create a fixed nonce for deterministic test
        signHeaders.put("x-acs-signature-nonce", "ef34aae7-7bd2-413d-a541-680cd2c48538");

        Map<String, String> parameters = new HashMap<>();
        String path = "/llm-p2e4XXXXXXXXsvtn/datacenter/category";
        RESTAuthParameter restAuthParameter = new RESTAuthParameter(path, parameters, "POST", body);

        String authorization = signer.authorization(restAuthParameter, token, host, signHeaders);

        // Verify Authorization format: acs AccessKeyId:Signature
        assertTrue(authorization.startsWith("acs " + token.getAccessKeyId() + ":"));
        String signature =
                authorization.substring(("acs " + token.getAccessKeyId() + ":").length());
        assertNotNull(signature);
        // Signature should be base64 encoded
        assertTrue(signature.length() > 0);
    }

    @Test
    public void testOpenApiCanonicalizedHeaders() throws Exception {
        DLFOpenApiSigner signer = new DLFOpenApiSigner();
        String host = "dlfnext.cn-beijing.aliyuncs.com";
        DLFToken token = new DLFToken("YourAccessKeyId", "YourAccessKeySecret", null, null);

        Instant now = ZonedDateTime.of(2025, 4, 16, 3, 44, 46, 0, ZoneOffset.UTC).toInstant();
        Map<String, String> signHeaders = signer.signHeaders(null, now, null, host);

        // Set fixed nonce for deterministic test
        signHeaders.put("x-acs-signature-nonce", "ef34aae7-7bd2-413d-a541-680cd2c48538");

        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("/test/path", new HashMap<>(), "GET", null);

        String authorization = signer.authorization(restAuthParameter, token, host, signHeaders);

        // Verify that authorization is generated
        assertNotNull(authorization);
        assertTrue(authorization.startsWith("acs "));
    }

    @Test
    public void testOpenApiCanonicalizedResourceWithQueryParams() throws Exception {
        DLFOpenApiSigner signer = new DLFOpenApiSigner();
        String host = "dlfnext.cn-beijing.aliyuncs.com";
        DLFToken token = new DLFToken("YourAccessKeyId", "YourAccessKeySecret", null, null);

        Instant now = Instant.now();
        Map<String, String> signHeaders = signer.signHeaders(null, now, null, host);

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("k2", "v2");
        queryParams.put("k1", "v1");

        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("/test/path", queryParams, "GET", null);

        String authorization = signer.authorization(restAuthParameter, token, host, signHeaders);

        // Verify that authorization is generated with query params
        assertNotNull(authorization);
        assertTrue(authorization.startsWith("acs "));
    }

    @Test
    public void testIdentifier() {
        DLFDefaultSigner defaultSigner = new DLFDefaultSigner("region");
        assertEquals(DLFDefaultSigner.IDENTIFIER, defaultSigner.identifier());

        DLFOpenApiSigner signer = new DLFOpenApiSigner();
        assertEquals(DLFOpenApiSigner.IDENTIFIER, signer.identifier());
    }

    @Test
    public void testDlfNextEndpoint() {
        assertEquals(
                DLFOpenApiSigner.IDENTIFIER,
                DLFAuthProviderFactory.parseSigningAlgoFromUri("dlfnext.cn-hangzhou.aliyuncs.com"));
        assertEquals(
                DLFOpenApiSigner.IDENTIFIER,
                DLFAuthProviderFactory.parseSigningAlgoFromUri(
                        "dlfnext-vpc.cn-hangzhou.aliyuncs.com"));
        assertEquals(
                DLFOpenApiSigner.IDENTIFIER,
                DLFAuthProviderFactory.parseSigningAlgoFromUri(
                        "https://dlfnext.cn-hangzhou.aliyuncs.com"));
    }

    @Test
    public void testDlfEndpoint() {
        assertEquals(
                DLFDefaultSigner.IDENTIFIER,
                DLFAuthProviderFactory.parseSigningAlgoFromUri("cn-hangzhou-vpc.dlf.aliyuncs.com"));
        assertEquals(
                DLFDefaultSigner.IDENTIFIER,
                DLFAuthProviderFactory.parseSigningAlgoFromUri(
                        "cn-hangzhou-intranet.dlf.aliyuncs.com"));
        assertEquals(
                DLFDefaultSigner.IDENTIFIER,
                DLFAuthProviderFactory.parseSigningAlgoFromUri(
                        "https://cn-hangzhou-vpc.dlf.aliyuncs.com"));
    }

    @Test
    public void testUnknownEndpoint() {
        assertEquals(
                DLFDefaultSigner.IDENTIFIER,
                DLFAuthProviderFactory.parseSigningAlgoFromUri("unknown.example.com"));
        assertEquals(
                DLFDefaultSigner.IDENTIFIER,
                DLFAuthProviderFactory.parseSigningAlgoFromUri("127.0.0.1"));
        assertEquals(
                DLFDefaultSigner.IDENTIFIER,
                DLFAuthProviderFactory.parseSigningAlgoFromUri("http://127.0.0.1:8080"));
    }

    @Test
    public void testEmptyHost() {
        assertEquals(
                DLFDefaultSigner.IDENTIFIER, DLFAuthProviderFactory.parseSigningAlgoFromUri(""));
        assertEquals(
                DLFDefaultSigner.IDENTIFIER, DLFAuthProviderFactory.parseSigningAlgoFromUri(null));
    }

    @Test
    public void testOpenApiSignHeadersWithEnhancedNonce() throws Exception {
        DLFOpenApiSigner signer = new DLFOpenApiSigner();
        String body = "{\"CategoryName\":\"test\",\"CategoryType\":\"UNSTRUCTURED\"}";
        Instant now = ZonedDateTime.of(2025, 4, 16, 3, 44, 46, 0, ZoneOffset.UTC).toInstant();
        String host = "dlfnext.cn-beijing.aliyuncs.com";

        Map<String, String> headers = signer.signHeaders(body, now, null, host);

        assertNotNull(headers.get("Date"));
        assertEquals("application/json", headers.get("Accept"));
        assertNotNull(headers.get("Content-MD5"));
        assertEquals("application/json", headers.get("Content-Type"));
        assertEquals(host, headers.get("Host"));
        assertEquals("HMAC-SHA1", headers.get("x-acs-signature-method"));

        // Verify nonce format inspired by Alibaba Cloud DataLake SDK
        String nonceValue = headers.get("x-acs-signature-nonce");
        assertNotNull(nonceValue);

        // Verify nonce contains UUID part (should be 32 hex chars + 4 dashes = 36 chars)
        // Find the UUID part by looking for the typical UUID pattern
        java.util.regex.Pattern uuidPattern =
                java.util.regex.Pattern.compile(
                        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
        java.util.regex.Matcher matcher = uuidPattern.matcher(nonceValue);
        assertTrue(matcher.find(), "No UUID pattern found in nonce: " + nonceValue);

        // Verify that nonce contains timestamp-like numbers (long digits)
        // Should contain millisecond timestamp (at least 10 digits)
        java.util.regex.Pattern digitPattern = java.util.regex.Pattern.compile("\\d+");
        java.util.regex.Matcher digitMatcher = digitPattern.matcher(nonceValue);
        boolean timestampFound = false;
        boolean threadIdFound = false;
        while (digitMatcher.find()) {
            String digitSequence = digitMatcher.group();
            if (digitSequence.length() >= 10) { // At least 10 digits for timestamp
                timestampFound = true;
            }
            if (digitSequence.length() >= 1) { // Thread ID could be shorter
                threadIdFound = true;
            }
        }
        assertTrue(timestampFound, "No timestamp-like part found in nonce: " + nonceValue);
        assertTrue(threadIdFound, "No thread ID-like part found in nonce: " + nonceValue);

        assertEquals("1.0", headers.get("x-acs-signature-version"));
        assertEquals("2026-01-18", headers.get("x-acs-version"));
    }

    @Test
    public void testConcurrentNonceGeneration() throws InterruptedException {
        DLFOpenApiSigner signer = new DLFOpenApiSigner();
        String body = "{\"test\":\"data\"}";
        Instant now = Instant.now();
        String host = "test-host";
        int threadCount = 10;
        int iterationsPerThread = 50;

        Set<String> nonces = Collections.synchronizedSet(new HashSet<>());
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(
                    () -> {
                        for (int j = 0; j < iterationsPerThread; j++) {
                            Map<String, String> headers = signer.signHeaders(body, now, null, host);
                            String nonce = headers.get("x-acs-signature-nonce");
                            nonces.add(nonce);
                        }
                        latch.countDown();
                    });
        }

        latch.await();
        executor.shutdown();

        // Verify all generated nonces are unique
        assertEquals((long) threadCount * iterationsPerThread, nonces.size());
    }
}
