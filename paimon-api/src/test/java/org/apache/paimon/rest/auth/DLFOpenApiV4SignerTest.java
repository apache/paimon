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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link DLFOpenApiV4Signer}. */
public class DLFOpenApiV4SignerTest {

    private static final String EMPTY_BODY_SHA256 =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private static final String HOST = "dlfnext.cn-beijing.aliyuncs.com";

    private static final String REGION = "cn-beijing";

    private static final Instant NOW =
            ZonedDateTime.of(2025, 4, 16, 3, 44, 46, 0, ZoneOffset.UTC).toInstant();

    @Test
    public void testSignHeadersWithBody() {
        DLFOpenApiV4Signer signer = new DLFOpenApiV4Signer(REGION);
        String body = "{\"CategoryName\":\"test\",\"CategoryType\":\"UNSTRUCTURED\"}";

        Map<String, String> headers = signer.signHeaders(body, NOW, null, HOST);

        assertEquals("2025-04-16T03:44:46Z", headers.get("x-acs-date"));
        assertEquals(HOST, headers.get("host"));
        assertNotEquals(EMPTY_BODY_SHA256, headers.get("x-acs-content-sha256"));
        assertEquals("application/json", headers.get("content-type"));
        assertNotNull(headers.get("x-acs-signature-nonce"));
        assertEquals("2026-01-18", headers.get("x-acs-version"));

        // The V1 ROA headers belong to DLFOpenApiSigner, not to this signer
        assertFalse(headers.containsKey("Date"));
        assertFalse(headers.containsKey("Accept"));
        assertFalse(headers.containsKey("Content-MD5"));
        assertFalse(headers.containsKey("x-acs-signature-method"));
        assertFalse(headers.containsKey("x-acs-signature-version"));
    }

    @Test
    public void testSignHeadersWithoutBody() {
        DLFOpenApiV4Signer signer = new DLFOpenApiV4Signer(REGION);

        Map<String, String> headers = signer.signHeaders(null, NOW, null, HOST);

        assertEquals(EMPTY_BODY_SHA256, headers.get("x-acs-content-sha256"));
        assertFalse(headers.containsKey("content-type"));
    }

    @Test
    public void testSignHeadersWithSecurityToken() {
        DLFOpenApiV4Signer signer = new DLFOpenApiV4Signer(REGION);

        Map<String, String> headers = signer.signHeaders(null, NOW, "test-security-token", HOST);

        assertEquals("test-security-token", headers.get("x-acs-security-token"));
    }

    @Test
    public void testDateIsRenderedInUtcForNonUtcInstant() {
        DLFOpenApiV4Signer signer = new DLFOpenApiV4Signer(REGION);
        Instant beijingNoon =
                ZonedDateTime.of(2025, 4, 16, 11, 44, 46, 0, ZoneOffset.ofHours(8)).toInstant();

        Map<String, String> headers = signer.signHeaders(null, beijingNoon, null, HOST);

        assertEquals("2025-04-16T03:44:46Z", headers.get("x-acs-date"));
    }

    /** The credential scope carries the date, the region and the POP product code. */
    @Test
    public void testAuthorizationCarriesCredentialScope() throws Exception {
        DLFOpenApiV4Signer signer = new DLFOpenApiV4Signer(REGION);
        DLFToken token =
                new DLFToken("YourAccessKeyId", "YourAccessKeySecret", "securityToken", null);
        String body = "{\"CategoryName\":\"test\",\"CategoryType\":\"UNSTRUCTURED\"}";

        Map<String, String> signHeaders =
                signer.signHeaders(body, NOW, token.getSecurityToken(), HOST);
        signHeaders.put("x-acs-signature-nonce", "ef34aae7-7bd2-413d-a541-680cd2c48538");

        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter(
                        "/llm-p2e4XXXXXXXXsvtn/datacenter/category", new HashMap<>(), "POST", body);

        String authorization = signer.authorization(restAuthParameter, token, HOST, signHeaders);

        assertTrue(authorization.startsWith("ACS4-HMAC-SHA256 Credential="));
        assertTrue(
                authorization.contains(
                        "Credential=YourAccessKeyId/20250416/cn-beijing/DlfNext/aliyun_v4_request,"));
        assertTrue(authorization.contains("SignedHeaders="));

        String signaturePart = authorization.substring(authorization.indexOf("Signature=") + 10);
        assertEquals(64, signaturePart.length());
        assertTrue(signaturePart.matches("[0-9a-f]+"));
    }

    /** The derived key depends on the region, so the same request signs differently elsewhere. */
    @Test
    public void testSignatureDependsOnRegion() throws Exception {
        DLFToken token = new DLFToken("TestAKId", "TestAKSecret", null, null);
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("/test/path", new HashMap<>(), "GET", null);

        DLFOpenApiV4Signer beijing = new DLFOpenApiV4Signer("cn-beijing");
        Map<String, String> headers = beijing.signHeaders(null, NOW, null, HOST);
        headers.put("x-acs-signature-nonce", "fixed-nonce-for-test");
        String beijingAuth = beijing.authorization(restAuthParameter, token, HOST, headers);

        DLFOpenApiV4Signer hangzhou = new DLFOpenApiV4Signer("cn-hangzhou");
        String hangzhouAuth = hangzhou.authorization(restAuthParameter, token, HOST, headers);

        assertNotEquals(beijingAuth, hangzhouAuth);
        assertTrue(beijingAuth.contains("/cn-beijing/DlfNext/"));
        assertTrue(hangzhouAuth.contains("/cn-hangzhou/DlfNext/"));
    }

    @Test
    public void testDeterministicSignature() throws Exception {
        DLFOpenApiV4Signer signer = new DLFOpenApiV4Signer(REGION);
        DLFToken token = new DLFToken("TestAKId", "TestAKSecret", null, null);

        Map<String, String> signHeaders = signer.signHeaders(null, NOW, null, HOST);
        signHeaders.put("x-acs-signature-nonce", "fixed-nonce-for-test");

        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("/test/path", new HashMap<>(), "GET", null);

        String auth1 = signer.authorization(restAuthParameter, token, HOST, signHeaders);
        String auth2 = signer.authorization(restAuthParameter, token, HOST, signHeaders);

        assertEquals(auth1, auth2);
    }

    @Test
    public void testSignatureCoversQueryParameters() throws Exception {
        DLFOpenApiV4Signer signer = new DLFOpenApiV4Signer(REGION);
        DLFToken token = new DLFToken("TestAKId", "TestAKSecret", null, null);

        Map<String, String> signHeaders = signer.signHeaders(null, NOW, null, HOST);
        signHeaders.put("x-acs-signature-nonce", "fixed-nonce-for-test");

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("k2", "v2");
        queryParams.put("k1", "v1");

        String withParams =
                signer.authorization(
                        new RESTAuthParameter("/test/path", queryParams, "GET", null),
                        token,
                        HOST,
                        signHeaders);
        String withoutParams =
                signer.authorization(
                        new RESTAuthParameter("/test/path", new HashMap<>(), "GET", null),
                        token,
                        HOST,
                        signHeaders);

        assertTrue(withParams.startsWith("ACS4-HMAC-SHA256 "));
        assertNotEquals(withoutParams, withParams);
    }

    /**
     * Known answer for query values that change under encoding. {@link RESTAuthParameter} has
     * already encoded them once, so encoding them again would sign a canonical form the gateway
     * cannot rebuild from the wire. The keys are inserted out of order, so dropping the sort breaks
     * this too. pypaimon pins the same string.
     */
    @Test
    public void testKnownSignatureForEncodedQueryValues() throws Exception {
        DLFOpenApiV4Signer signer = new DLFOpenApiV4Signer("cn-hangzhou");
        DLFToken token = new DLFToken("TestAKId", "TestAKSecret", null, null);
        String host = "dlfnext.cn-hangzhou.aliyuncs.com";

        Map<String, String> signHeaders = signer.signHeaders(null, NOW, null, host);
        signHeaders.put("x-acs-signature-nonce", "fixed-nonce-for-test");

        Map<String, String> params = new LinkedHashMap<>();
        params.put("pageToken", "a b/c");
        params.put("functionNamePattern", "func%");

        String authorization =
                signer.authorization(
                        new RESTAuthParameter("/v1/prefix/functions", params, "GET", null),
                        token,
                        host,
                        signHeaders);

        assertEquals(
                "ACS4-HMAC-SHA256 Credential=TestAKId/20250416/cn-hangzhou/DlfNext/aliyun_v4_request,"
                        + "SignedHeaders=host;x-acs-content-sha256;x-acs-date;x-acs-signature-nonce;"
                        + "x-acs-version,"
                        + "Signature=9812bacd92dd1152e7ea32aacfad0545835d6a18543852364f1a1ea0cba6219a",
                authorization);
    }

    /** Known answer with a body and a resolved action; pypaimon pins the same string. */
    @Test
    public void testKnownSignatureWithAction() throws Exception {
        DLFOpenApiV4Signer signer = new DLFOpenApiV4Signer("cn-hangzhou");
        DLFToken token = new DLFToken("TestAKId", "TestAKSecret", null, null);
        String host = "dlfnext.cn-hangzhou.aliyuncs.com";
        String body = "{\"identifier\":{\"database\":\"db\",\"object\":\"t\"}}";
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter(
                        "/v1/clg-paimon-1/databases/db/tables", new HashMap<>(), "POST", body);

        Map<String, String> signHeaders =
                signer.signRequestHeaders(restAuthParameter, NOW, null, host);
        signHeaders.put("x-acs-signature-nonce", "fixed-nonce-for-test");

        assertEquals("CreateTable", signHeaders.get("x-acs-action"));
        assertEquals(
                "ACS4-HMAC-SHA256 Credential=TestAKId/20250416/cn-hangzhou/DlfNext/aliyun_v4_request,"
                        + "SignedHeaders=content-type;host;x-acs-action;x-acs-content-sha256;"
                        + "x-acs-date;x-acs-signature-nonce;x-acs-version,"
                        + "Signature=420206b5263536e6bc271a7a32582b4a820e23c8c58ae692b708c69f9d19e51d",
                signer.authorization(restAuthParameter, token, host, signHeaders));
    }

    @Test
    public void testSignRequestHeadersOmitsActionForUnregisteredPath() {
        DLFOpenApiV4Signer signer = new DLFOpenApiV4Signer(REGION);
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("/v1/clg-paimon-1/tables", new HashMap<>(), "GET", null);

        Map<String, String> headers = signer.signRequestHeaders(restAuthParameter, NOW, null, HOST);

        assertFalse(headers.containsKey("x-acs-action"));
        assertEquals(signer.signHeaders(null, NOW, null, HOST).keySet(), headers.keySet());
    }

    @Test
    public void testAuthProviderSendsSignedAction() {
        DLFAuthProvider provider =
                DLFAuthProvider.fromAccessKey(
                        "akId",
                        "akSecret",
                        "securityToken",
                        "https://dlfnext.cn-hangzhou.aliyuncs.com",
                        "cn-hangzhou",
                        DLFOpenApiV4Signer.IDENTIFIER);

        Map<String, String> headers =
                provider.mergeAuthHeader(
                        new HashMap<>(),
                        new RESTAuthParameter(
                                "/v1/clg-paimon-1/databases/db/tables/t/token",
                                new HashMap<>(),
                                "GET",
                                null));

        assertEquals("GetTableToken", headers.get("x-acs-action"));
        assertTrue(
                headers.get("Authorization")
                        .contains(
                                "SignedHeaders=host;x-acs-action;x-acs-content-sha256;x-acs-date;"
                                        + "x-acs-security-token;x-acs-signature-nonce;x-acs-version,"));
    }

    @Test
    public void testSignedHeadersIncludeHostAndContentType() throws Exception {
        DLFOpenApiV4Signer signer = new DLFOpenApiV4Signer(REGION);
        DLFToken token = new DLFToken("TestAKId", "TestAKSecret", null, null);
        String body = "{\"test\":\"data\"}";

        Map<String, String> signHeaders = signer.signHeaders(body, NOW, null, HOST);
        signHeaders.put("x-acs-signature-nonce", "fixed-nonce");

        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("/test/path", new HashMap<>(), "POST", body);

        String authorization = signer.authorization(restAuthParameter, token, HOST, signHeaders);

        String signedHeadersPart =
                authorization.substring(
                        authorization.indexOf("SignedHeaders=") + 14,
                        authorization.indexOf(",Signature="));
        // exact, so that dropping the sort in buildCanonicalHeaders fails here
        assertEquals(
                "content-type;host;x-acs-content-sha256;x-acs-date;x-acs-signature-nonce;"
                        + "x-acs-version",
                signedHeadersPart);
    }

    @Test
    public void testPercentEncode() throws Exception {
        assertEquals("hello%20world", DLFOpenApiV4Signer.percentEncode("hello world"));
        assertEquals("a%2Ab", DLFOpenApiV4Signer.percentEncode("a*b"));
        assertEquals("a~b", DLFOpenApiV4Signer.percentEncode("a~b"));
        assertEquals("a%2Fb", DLFOpenApiV4Signer.percentEncode("a/b"));
    }

    @Test
    public void testIdentifier() {
        assertEquals("openapi-v4", new DLFOpenApiV4Signer(REGION).identifier());
    }

    @Test
    public void testAuthProviderSignsWithAcs4WhenConfigured() {
        String uri = "https://dlfnext.cn-hangzhou.aliyuncs.com";
        DLFAuthProvider provider =
                DLFAuthProvider.fromAccessKey(
                        "akId",
                        "akSecret",
                        null,
                        uri,
                        "cn-hangzhou",
                        DLFOpenApiV4Signer.IDENTIFIER);

        Map<String, String> headers =
                provider.mergeAuthHeader(
                        new HashMap<>(),
                        new RESTAuthParameter("/test/path", new HashMap<>(), "GET", null));

        assertTrue(headers.get("Authorization").startsWith("ACS4-HMAC-SHA256 Credential=akId/"));
        assertTrue(
                headers.get("Authorization").contains("/cn-hangzhou/DlfNext/aliyun_v4_request,"));
        assertNotNull(headers.get("x-acs-date"));
    }
}
