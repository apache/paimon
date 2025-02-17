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
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ThreadPoolUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_ID;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_SECRET;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_TOKEN_PATH;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_REFRESH_TIME;
import static org.apache.paimon.rest.auth.AuthSession.MAX_REFRESH_WINDOW_MILLIS;
import static org.apache.paimon.rest.auth.AuthSession.MIN_REFRESH_WAIT_MILLIS;
import static org.apache.paimon.rest.auth.AuthSession.REFRESH_NUM_RETRIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link AuthSession}. */
public class AuthSessionTest {

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
        AuthProvider authProvider =
                AuthProviderFactory.createAuthProvider(AuthProviderEnum.BEAR.identifier(), options);
        AuthSession session = new AuthSession(authProvider);
        Map<String, String> headers = session.getAuthProvider().header(initialHeaders, null);
        assertEquals(
                headers.get(BearTokenAuthProvider.AUTHORIZATION_HEADER_KEY), "Bearer " + token);
    }

    @Test
    public void testRefreshDLFAuthTokenFileAuthProvider() throws IOException, InterruptedException {
        String fileName = UUID.randomUUID().toString();
        Pair<File, String> tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String token = tokenFile2Token.getRight();
        File tokenFile = tokenFile2Token.getLeft();
        long tokenRefreshInMills = 1000;
        AuthProvider authProvider =
                generateDLFAuthProvider(Optional.of(tokenRefreshInMills), fileName);
        ScheduledExecutorService executor =
                ThreadPoolUtils.createScheduledThreadPool(1, "refresh-token");
        AuthSession session = AuthSession.fromRefreshAuthProvider(executor, authProvider);
        DLFAuthProvider dlfAuthProvider = (DLFAuthProvider) session.getAuthProvider();
        String authToken = OBJECT_MAPPER_INSTANCE.writeValueAsString(dlfAuthProvider.token);
        assertEquals(authToken, token);
        tokenFile.delete();
        tokenFile2Token = generateTokenAndWriteToFile(fileName);
        token = tokenFile2Token.getRight();
        Thread.sleep(tokenRefreshInMills * 2);
        authToken = OBJECT_MAPPER_INSTANCE.writeValueAsString(dlfAuthProvider.token);
        assertEquals(authToken, token);
    }

    @Test
    public void testRefreshAuthProviderIsSoonExpire() throws IOException, InterruptedException {
        String fileName = UUID.randomUUID().toString();
        Pair<File, String> tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String token = tokenFile2Token.getRight();
        File tokenFile = tokenFile2Token.getLeft();
        long tokenRefreshInMills = 5000L;
        AuthProvider authProvider =
                generateDLFAuthProvider(Optional.of(tokenRefreshInMills), fileName);
        AuthSession session = AuthSession.fromRefreshAuthProvider(null, authProvider);
        DLFAuthProvider dlfAuthProvider = (DLFAuthProvider) session.getAuthProvider();
        String authToken = OBJECT_MAPPER_INSTANCE.writeValueAsString(dlfAuthProvider.token);
        assertEquals(token, authToken);
        Thread.sleep((long) (tokenRefreshInMills * (1 - DLFAuthProvider.EXPIRED_FACTOR)) + 10L);
        tokenFile.delete();
        tokenFile2Token = generateTokenAndWriteToFile(fileName);
        token = tokenFile2Token.getRight();
        tokenFile = tokenFile2Token.getLeft();
        FileUtils.writeStringToFile(tokenFile, token);
        dlfAuthProvider = (DLFAuthProvider) session.getAuthProvider();
        authToken = OBJECT_MAPPER_INSTANCE.writeValueAsString(dlfAuthProvider.token);
        assertEquals(token, authToken);
    }

    @Test
    public void testRetryWhenRefreshFail() throws Exception {
        AuthProvider authProvider = Mockito.mock(DLFAuthProvider.class);
        long expiresAtMillis = System.currentTimeMillis() - 1000L;
        when(authProvider.expiresAtMillis()).thenReturn(Optional.of(expiresAtMillis));
        when(authProvider.tokenRefreshInMills()).thenReturn(Optional.of(50L));
        when(authProvider.supportRefresh()).thenReturn(true);
        when(authProvider.keepRefreshed()).thenReturn(true);
        when(authProvider.refresh()).thenReturn(false);
        AuthSession session = AuthSession.fromRefreshAuthProvider(null, authProvider);
        AuthSession.scheduleTokenRefresh(
                ThreadPoolUtils.createScheduledThreadPool(1, "refresh-token"),
                session,
                expiresAtMillis);
        Thread.sleep(10_000L);
        verify(authProvider, Mockito.times(REFRESH_NUM_RETRIES + 1)).refresh();
    }

    @Test
    public void testGetTimeToWaitByExpiresInMills() {
        long expiresInMillis = -100L;
        long timeToWait = AuthSession.getTimeToWaitByExpiresInMills(expiresInMillis);
        assertEquals(MIN_REFRESH_WAIT_MILLIS, timeToWait);
        expiresInMillis = (long) (MAX_REFRESH_WINDOW_MILLIS * 0.5);
        timeToWait = AuthSession.getTimeToWaitByExpiresInMills(expiresInMillis);
        assertEquals(MIN_REFRESH_WAIT_MILLIS, timeToWait);
        expiresInMillis = MAX_REFRESH_WINDOW_MILLIS;
        timeToWait = AuthSession.getTimeToWaitByExpiresInMills(expiresInMillis);
        assertEquals(timeToWait, MIN_REFRESH_WAIT_MILLIS);
        expiresInMillis = MAX_REFRESH_WINDOW_MILLIS * 2L;
        timeToWait = AuthSession.getTimeToWaitByExpiresInMills(expiresInMillis);
        assertEquals(timeToWait, MAX_REFRESH_WINDOW_MILLIS);
    }

    @Test
    public void testCreateDLFAuthProviderByAk() throws IOException {
        Options options = new Options();
        String akId = UUID.randomUUID().toString();
        String akSecret = UUID.randomUUID().toString();
        DLFToken token = new DLFToken(akId, akSecret, null, null);
        options.set(DLF_ACCESS_KEY_ID.key(), token.getAccessKeyId());
        options.set(DLF_ACCESS_KEY_SECRET.key(), token.getAccessKeySecret());
        AuthProvider authProvider =
                AuthProviderFactory.createAuthProvider(AuthProviderEnum.DLF.identifier(), options);
        AuthSession session = AuthSession.fromRefreshAuthProvider(null, authProvider);
        DLFAuthProvider dlfAuthProvider = (DLFAuthProvider) session.getAuthProvider();
        String authToken = OBJECT_MAPPER_INSTANCE.writeValueAsString(dlfAuthProvider.token);
        assertEquals(OBJECT_MAPPER_INSTANCE.writeValueAsString(token), authToken);
    }

    @Test
    public void testCreateDlfAuthProviderByFileNoDefineRefresh() throws IOException {
        String fileName = UUID.randomUUID().toString();
        Pair<File, String> tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String token = tokenFile2Token.getRight();
        AuthProvider authProvider = generateDLFAuthProvider(Optional.empty(), fileName);
        ScheduledExecutorService executor =
                ThreadPoolUtils.createScheduledThreadPool(1, "refresh-token");
        AuthSession session = AuthSession.fromRefreshAuthProvider(executor, authProvider);
        DLFAuthProvider dlfAuthProvider = (DLFAuthProvider) session.getAuthProvider();
        String authToken = OBJECT_MAPPER_INSTANCE.writeValueAsString(dlfAuthProvider.token);
        assertEquals(authToken, token);
    }

    @Test
    public void testCreateDLFAuthProviderWithoutNeedConf() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        AuthProviderFactory.createAuthProvider(
                                AuthProviderEnum.DLF.identifier(), new Options()));
    }

    @Test
    public void testDLFAuthProviderAuthHeader() throws Exception {
        String fileName = UUID.randomUUID().toString();
        Pair<File, String> tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String tokenStr = tokenFile2Token.getRight();
        AuthProvider authProvider = generateDLFAuthProvider(Optional.empty(), fileName);
        DLFToken token = OBJECT_MAPPER_INSTANCE.readValue(tokenStr, DLFToken.class);
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("host", "/path", "method", "data");
        Map<String, String> header = authProvider.header(new HashMap<>(), restAuthParameter);
        String date = header.get(DLFAuthProvider.DLF_DATE_HEADER_KEY);
        String dateMd5Hex = header.get(DLFAuthProvider.DLF_DATA_MD5_HEX_HEADER_KEY);
        String authorization =
                DLFAuthSignature.getAuthorization(
                        new RESTAuthParameter("host", "/path", "method", "data"),
                        token,
                        dateMd5Hex,
                        date);
        assertEquals(authorization, header.get(DLFAuthProvider.DLF_AUTHORIZATION_HEADER_KEY));
        assertEquals(restAuthParameter.host(), header.get(DLFAuthProvider.DLF_HOST_HEADER_KEY));
        assertEquals(
                token.getSecurityToken(),
                header.get(DLFAuthProvider.DLF_SECURITY_TOKEN_HEADER_KEY));
        assertEquals(
                token.getAccessKeyId(), header.get(DLFAuthProvider.DLF_ACCESSKEY_ID_HEADER_KEY));
    }

    private Pair<File, String> generateTokenAndWriteToFile(String fileName) throws IOException {
        File tokenFile = folder.newFile(fileName);
        String expiration = DLFAuthProvider.getDate();
        String secret = UUID.randomUUID().toString();
        DLFToken token = new DLFToken("accessKeyId", secret, "securityToken", expiration);
        String tokenStr = OBJECT_MAPPER_INSTANCE.writeValueAsString(token);
        FileUtils.writeStringToFile(tokenFile, tokenStr);
        return Pair.of(tokenFile, tokenStr);
    }

    private AuthProvider generateDLFAuthProvider(
            Optional<Long> tokenRefreshInMillsOpt, String fileName) {
        Options options = new Options();
        options.set(DLF_TOKEN_PATH.key(), folder.getRoot().getPath() + "/" + fileName);
        tokenRefreshInMillsOpt.ifPresent(
                tokenRefreshInMills ->
                        options.set(TOKEN_REFRESH_TIME.key(), tokenRefreshInMills + "ms"));
        return AuthProviderFactory.createAuthProvider(AuthProviderEnum.DLF.identifier(), options);
    }
}
