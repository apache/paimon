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

import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.rest.RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_ID;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_SECRET;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_REGION;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_SECURITY_TOKEN;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_TOKEN_ECS_METADATA_URL;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_TOKEN_ECS_ROLE_NAME;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_TOKEN_LOADER;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_TOKEN_PATH;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.auth.DLFAuthProvider.DLF_AUTHORIZATION_HEADER_KEY;
import static org.apache.paimon.rest.auth.DLFAuthProvider.DLF_DATE_HEADER_KEY;
import static org.apache.paimon.rest.auth.DLFToken.TOKEN_DATE_FORMATTER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link AuthProvider}. */
public class AuthProviderTest {

    @Rule public TemporaryFolder folder = new TemporaryFolder();
    private static final ObjectMapper OBJECT_MAPPER_INSTANCE = new ObjectMapper();

    @Test
    public void testBearToken() {
        String token = UUID.randomUUID().toString();
        Map<String, String> initialHeaders = new HashMap<>();
        initialHeaders.put("k1", "v1");
        initialHeaders.put("k2", "v2");
        Options options = new Options();
        options.set(TOKEN.key(), token);
        options.set(TOKEN_PROVIDER.key(), "bear");
        AuthProvider authProvider = AuthProviderFactory.createAuthProvider(options);
        Map<String, String> headers = authProvider.mergeAuthHeader(initialHeaders, null);
        assertEquals(
                headers.get(BearTokenAuthProvider.AUTHORIZATION_HEADER_KEY), "Bearer " + token);
    }

    @Test
    public void testRefreshDLFAuthTokenFileAuthProvider() throws IOException {
        String fileName = UUID.randomUUID().toString();
        Pair<File, String> tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String theFirstGenerateToken = tokenFile2Token.getRight();
        File tokenFile = tokenFile2Token.getLeft();
        DLFAuthProvider authProvider = generateDLFAuthProvider(fileName, "serverUrl");
        String theFirstFetchToken =
                OBJECT_MAPPER_INSTANCE.writeValueAsString(authProvider.getFreshToken());
        assertEquals(theFirstFetchToken, theFirstGenerateToken);
        tokenFile.delete();
        tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String theSecondGenerateToken = tokenFile2Token.getRight();
        String theSecondFetchToken =
                OBJECT_MAPPER_INSTANCE.writeValueAsString(authProvider.getFreshToken());
        // if the second fetch token is not equal to the first fetch token, it means refresh success
        // as refresh maybe fail in test environment, so we need to check whether refresh success
        if (!theSecondFetchToken.equals(theFirstFetchToken)) {
            assertEquals(theSecondGenerateToken, theSecondFetchToken);
        }
    }

    @Test
    public void testRefreshAuthProviderIsSoonExpire() throws IOException {
        String fileName = UUID.randomUUID().toString();
        Pair<File, String> tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String token = tokenFile2Token.getRight();
        File tokenFile = tokenFile2Token.getLeft();
        DLFAuthProvider authProvider = generateDLFAuthProvider(fileName, "serverUrl");
        String authToken = OBJECT_MAPPER_INSTANCE.writeValueAsString(authProvider.getFreshToken());
        assertEquals(token, authToken);
        tokenFile.delete();
        tokenFile2Token = generateTokenAndWriteToFile(fileName);
        token = tokenFile2Token.getRight();
        tokenFile = tokenFile2Token.getLeft();
        FileUtils.writeStringToFile(tokenFile, token);
        authToken = OBJECT_MAPPER_INSTANCE.writeValueAsString(authProvider.getFreshToken());
        assertEquals(token, authToken);
    }

    @Test
    public void testCreateDLFAuthProviderByStsToken() throws IOException {
        Options options = new Options();
        String akId = UUID.randomUUID().toString();
        String akSecret = UUID.randomUUID().toString();
        String securityToken = UUID.randomUUID().toString();
        DLFToken token = new DLFToken(akId, akSecret, securityToken, null);
        options.set(DLF_ACCESS_KEY_ID.key(), token.getAccessKeyId());
        options.set(DLF_ACCESS_KEY_SECRET.key(), token.getAccessKeySecret());
        options.set(DLF_SECURITY_TOKEN.key(), token.getSecurityToken());
        options.set(DLF_REGION.key(), "cn-hangzhou");
        options.set(TOKEN_PROVIDER, "dlf");
        DLFAuthProvider authProvider =
                (DLFAuthProvider) AuthProviderFactory.createAuthProvider(options);
        String authToken = OBJECT_MAPPER_INSTANCE.writeValueAsString(authProvider.getFreshToken());
        assertEquals(OBJECT_MAPPER_INSTANCE.writeValueAsString(token), authToken);
    }

    @Test
    public void testCreateDLFAuthProviderByAk() throws IOException {
        Options options = new Options();
        String akId = UUID.randomUUID().toString();
        String akSecret = UUID.randomUUID().toString();
        DLFToken token = new DLFToken(akId, akSecret, null, null);
        options.set(DLF_ACCESS_KEY_ID.key(), token.getAccessKeyId());
        options.set(DLF_ACCESS_KEY_SECRET.key(), token.getAccessKeySecret());
        options.set(DLF_REGION.key(), "cn-hangzhou");
        options.set(TOKEN_PROVIDER, "dlf");
        DLFAuthProvider authProvider =
                (DLFAuthProvider) AuthProviderFactory.createAuthProvider(options);
        String authToken = OBJECT_MAPPER_INSTANCE.writeValueAsString(authProvider.getFreshToken());
        assertEquals(OBJECT_MAPPER_INSTANCE.writeValueAsString(token), authToken);
    }

    @Test
    public void testCreateDlfAuthProviderByFileNoDefineRefresh() throws IOException {
        String fileName = UUID.randomUUID().toString();
        Pair<File, String> tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String token = tokenFile2Token.getRight();
        DLFAuthProvider authProvider = generateDLFAuthProvider(fileName, "serverUrl");
        String authToken = OBJECT_MAPPER_INSTANCE.writeValueAsString(authProvider.getFreshToken());
        assertEquals(authToken, token);
    }

    @Test
    public void testCreateDLFAuthProviderWithoutNeedConf() {
        Options options = new Options();
        options.set(TOKEN_PROVIDER, "dlf");
        assertThrows(
                IllegalArgumentException.class,
                () -> AuthProviderFactory.createAuthProvider(new Options()));
    }

    @Test
    public void testCreateDlfAuthProviderByDLFTokenLoader() throws IOException {
        String fileName = UUID.randomUUID().toString();
        Pair<File, String> tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String theFirstGenerateToken = tokenFile2Token.getRight();
        File tokenFile = tokenFile2Token.getLeft();
        // create options with token loader
        Options options = new Options();
        options.set(DLF_TOKEN_LOADER.key(), "local_file");
        options.set(DLF_TOKEN_PATH.key(), folder.getRoot().getPath() + "/" + fileName);
        options.set(RESTCatalogOptions.URI.key(), "serverUrl");
        options.set(DLF_REGION.key(), "cn-hangzhou");
        options.set(TOKEN_PROVIDER, "dlf");
        DLFAuthProvider authProvider =
                (DLFAuthProvider) AuthProviderFactory.createAuthProvider(options);
        String theFirstFetchToken =
                OBJECT_MAPPER_INSTANCE.writeValueAsString(authProvider.getFreshToken());
        assertEquals(theFirstFetchToken, theFirstGenerateToken);
        tokenFile.delete();
        tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String theSecondGenerateToken = tokenFile2Token.getRight();
        String theSecondFetchToken =
                OBJECT_MAPPER_INSTANCE.writeValueAsString(authProvider.getFreshToken());
        // if the second fetch token is not equal to the first fetch token, it means refresh success
        // as refresh maybe fail in test environment, so we need to check whether refresh success
        if (!theSecondFetchToken.equals(theFirstFetchToken)) {
            assertEquals(theSecondGenerateToken, theSecondFetchToken);
        }
    }

    @Test
    public void testCreateDlfAuthProviderByCustomDLFTokenLoader() {
        DLFToken customToken = generateToken();
        // create options with custom token loader
        Options options = new Options();
        options.set(DLF_TOKEN_LOADER.key(), "custom");
        options.set(DLF_ACCESS_KEY_ID.key(), customToken.getAccessKeyId());
        options.set(DLF_ACCESS_KEY_SECRET.key(), customToken.getAccessKeySecret());
        options.set(DLF_SECURITY_TOKEN.key(), customToken.getSecurityToken());
        options.set(RESTCatalogOptions.URI.key(), "serverUrl");
        options.set(DLF_REGION.key(), "cn-hangzhou");
        options.set(TOKEN_PROVIDER, "dlf");
        DLFAuthProvider authProvider =
                (DLFAuthProvider) AuthProviderFactory.createAuthProvider(options);
        DLFToken fetchToken = authProvider.getFreshToken();
        assertEquals(fetchToken.getAccessKeyId(), customToken.getAccessKeyId());
        assertEquals(fetchToken.getAccessKeySecret(), customToken.getAccessKeySecret());
        assertEquals(fetchToken.getSecurityToken(), customToken.getSecurityToken());
    }

    @Test
    public void testCreateDlfAuthProviderByECSTokenProvider() throws IOException {
        MockECSMetadataService mockECSMetadataService = new MockECSMetadataService("EcsTestRole");
        mockECSMetadataService.start();
        try {
            DLFToken theFirstMockToken = generateToken();
            mockECSMetadataService.setMockToken(theFirstMockToken);
            String theFirstMockTokenStr =
                    OBJECT_MAPPER_INSTANCE.writeValueAsString(theFirstMockToken);
            // create options with token loader
            Options options = new Options();
            options.set(DLF_TOKEN_LOADER.key(), "ecs");
            options.set(
                    DLF_TOKEN_ECS_METADATA_URL.key(),
                    mockECSMetadataService.getUrl() + "latest/meta-data/Ram/security-credentials/");
            options.set(RESTCatalogOptions.URI.key(), "serverUrl");
            options.set(DLF_REGION.key(), "cn-hangzhou");
            options.set(TOKEN_PROVIDER, "dlf");

            DLFAuthProvider authProvider =
                    (DLFAuthProvider) AuthProviderFactory.createAuthProvider(options);

            // first token
            String theFirstFetchTokenStr =
                    OBJECT_MAPPER_INSTANCE.writeValueAsString(authProvider.getFreshToken());
            assertEquals(theFirstFetchTokenStr, theFirstMockTokenStr);

            // second token
            DLFToken theSecondMockToken =
                    generateToken(
                            ZonedDateTime.now(ZoneOffset.UTC)
                                    .plusSeconds(TOKEN_EXPIRATION_SAFE_TIME_MILLIS * 2 / 1000));
            String theSecondMockTokenStr =
                    OBJECT_MAPPER_INSTANCE.writeValueAsString(theSecondMockToken);
            mockECSMetadataService.setMockToken(theSecondMockToken);
            String theSecondFetchTokenStr =
                    OBJECT_MAPPER_INSTANCE.writeValueAsString(authProvider.getFreshToken());
            assertEquals(theSecondFetchTokenStr, theSecondMockTokenStr);

            // third token, should not refresh
            DLFToken theThirdMockToken = generateToken();
            mockECSMetadataService.setMockToken(theThirdMockToken);
            String theThirdFetchTokenStr =
                    OBJECT_MAPPER_INSTANCE.writeValueAsString(authProvider.getFreshToken());
            assertEquals(theThirdFetchTokenStr, theSecondMockTokenStr);
        } finally {
            mockECSMetadataService.shutdown();
        }
    }

    @Test
    public void testCreateDlfAuthProviderByECSTokenProviderWithDefineRole() throws IOException {
        MockECSMetadataService mockECSMetadataService = new MockECSMetadataService("CustomRole");
        mockECSMetadataService.start();
        try {
            DLFToken theFirstMockToken = generateToken();
            mockECSMetadataService.setMockToken(theFirstMockToken);
            String theFirstMockTokenStr =
                    OBJECT_MAPPER_INSTANCE.writeValueAsString(theFirstMockToken);
            // create options with token loader
            Options options = new Options();
            options.set(DLF_TOKEN_LOADER.key(), "ecs");
            options.set(
                    DLF_TOKEN_ECS_METADATA_URL.key(),
                    mockECSMetadataService.getUrl() + "latest/meta-data/Ram/security-credentials/");
            options.set(DLF_TOKEN_ECS_ROLE_NAME.key(), "CustomRole");
            options.set(RESTCatalogOptions.URI.key(), "serverUrl");
            options.set(DLF_REGION.key(), "cn-hangzhou");
            options.set(TOKEN_PROVIDER, "dlf");
            DLFAuthProvider authProvider =
                    (DLFAuthProvider) AuthProviderFactory.createAuthProvider(options);
            String theFirstFetchTokenStr =
                    OBJECT_MAPPER_INSTANCE.writeValueAsString(authProvider.getFreshToken());
            assertEquals(theFirstFetchTokenStr, theFirstMockTokenStr);

            DLFToken theSecondMockToken = generateToken();
            String theSecondMockTokenStr =
                    OBJECT_MAPPER_INSTANCE.writeValueAsString(theSecondMockToken);
            mockECSMetadataService.setMockToken(theSecondMockToken);
            String theSecondFetchTokenStr =
                    OBJECT_MAPPER_INSTANCE.writeValueAsString(authProvider.getFreshToken());
            assertEquals(theSecondFetchTokenStr, theSecondMockTokenStr);
        } finally {
            mockECSMetadataService.shutdown();
        }
    }

    @Test
    public void testCreateDlfAuthProviderByECSTokenProviderWithInvalidRole() throws IOException {
        MockECSMetadataService mockECSMetadataService = new MockECSMetadataService("EcsTestRole");
        mockECSMetadataService.start();
        try {
            DLFToken theFirstMockToken = generateToken();
            mockECSMetadataService.setMockToken(theFirstMockToken);
            // create options with token loader
            Options options = new Options();
            options.set(DLF_TOKEN_LOADER.key(), "ecs");
            options.set(
                    DLF_TOKEN_ECS_METADATA_URL.key(),
                    mockECSMetadataService.getUrl() + "latest/meta-data/Ram/security-credentials/");
            options.set(DLF_TOKEN_ECS_ROLE_NAME.key(), "CustomRole");
            options.set(RESTCatalogOptions.URI.key(), "serverUrl");
            options.set(DLF_REGION.key(), "cn-hangzhou");
            options.set(TOKEN_PROVIDER, "dlf");
            DLFAuthProvider authProvider =
                    (DLFAuthProvider) AuthProviderFactory.createAuthProvider(options);
            assertThrows(RuntimeException.class, authProvider::getFreshToken);
        } finally {
            mockECSMetadataService.shutdown();
        }
    }

    @Test
    public void testDLFAuthProviderAuthHeaderWhenDataIsNotEmpty() throws Exception {
        String fileName = UUID.randomUUID().toString();
        Pair<File, String> tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String tokenStr = tokenFile2Token.getRight();
        String serverUrl = "https://dlf-cn-hangzhou.aliyuncs.com";
        AuthProvider authProvider = generateDLFAuthProvider(fileName, serverUrl);
        DLFToken token = OBJECT_MAPPER_INSTANCE.readValue(tokenStr, DLFToken.class);
        Map<String, String> parameters = new HashMap<>();
        parameters.put("k1", "v1");
        parameters.put("k2", "v2");
        String data = "data";
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("/path", parameters, "method", "data");
        Map<String, String> header =
                authProvider.mergeAuthHeader(new HashMap<>(), restAuthParameter);
        String authorization = header.get(DLF_AUTHORIZATION_HEADER_KEY);
        String[] credentials = authorization.split(",")[0].split(" ")[1].split("/");
        String dateTime = header.get(DLF_DATE_HEADER_KEY);
        String date = credentials[1];
        DLFDefaultSigner signer = new DLFDefaultSigner("cn-hangzhou");
        String newAuthorization =
                signer.authorization(
                        new RESTAuthParameter("/path", parameters, "method", "data"),
                        token,
                        "host",
                        header);
        assertEquals(newAuthorization, authorization);
        assertEquals(
                token.getSecurityToken(),
                header.get(DLFAuthProvider.DLF_SECURITY_TOKEN_HEADER_KEY));
        assertTrue(header.containsKey(DLF_DATE_HEADER_KEY));
        assertEquals(
                DLFDefaultSigner.VERSION, header.get(DLFAuthProvider.DLF_AUTH_VERSION_HEADER_KEY));
        assertEquals(DLFAuthProvider.MEDIA_TYPE, header.get(DLFAuthProvider.DLF_CONTENT_TYPE_KEY));
        // Verify MD5 by checking it matches what's in the header
        assertTrue(header.containsKey(DLFAuthProvider.DLF_CONTENT_MD5_HEADER_KEY));
        assertEquals(
                DLFAuthProvider.DLF_CONTENT_SHA56_VALUE,
                header.get(DLFAuthProvider.DLF_CONTENT_SHA56_HEADER_KEY));
    }

    private Pair<File, String> generateTokenAndWriteToFile(String fileName) throws IOException {
        File tokenFile = folder.newFile(fileName);
        String expiration = ZonedDateTime.now(ZoneOffset.UTC).format(TOKEN_DATE_FORMATTER);
        String secret = UUID.randomUUID().toString();
        DLFToken token = new DLFToken("accessKeyId", secret, "securityToken", expiration);
        String tokenStr = OBJECT_MAPPER_INSTANCE.writeValueAsString(token);
        FileUtils.writeStringToFile(tokenFile, tokenStr);
        return Pair.of(tokenFile, tokenStr);
    }

    private DLFToken generateToken() {
        return generateToken(ZonedDateTime.now(ZoneOffset.UTC));
    }

    private DLFToken generateToken(ZonedDateTime expireTime) {
        String accessKeyId = UUID.randomUUID().toString();
        String accessKeySecret = UUID.randomUUID().toString();
        String securityToken = UUID.randomUUID().toString();
        String expiration = expireTime.format(TOKEN_DATE_FORMATTER);
        return new DLFToken(accessKeyId, accessKeySecret, securityToken, expiration);
    }

    private DLFAuthProvider generateDLFAuthProvider(String fileName, String serverUrl) {
        Options options = new Options();
        options.set(DLF_TOKEN_PATH.key(), folder.getRoot().getPath() + "/" + fileName);
        options.set(RESTCatalogOptions.URI.key(), serverUrl);
        options.set(DLF_REGION.key(), "cn-hangzhou");
        options.set(TOKEN_PROVIDER, "dlf");
        return (DLFAuthProvider) AuthProviderFactory.createAuthProvider(options);
    }
}
