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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.paimon.rest.auth.AuthSession.MAX_REFRESH_WINDOW_MILLIS;
import static org.apache.paimon.rest.auth.AuthSession.MIN_REFRESH_WAIT_MILLIS;
import static org.apache.paimon.rest.auth.AuthSession.REFRESH_NUM_RETRIES;
import static org.junit.Assert.assertEquals;
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
        AuthProvider authProvider = new BearTokenAuthProvider(token);
        AuthSession session = new AuthSession(authProvider);
        Map<String, String> authHeader = session.getAuthProvider().authHeader(null);
        assertEquals(authHeader.get("Authorization"), "Bearer " + token);
    }

    @Test
    public void testRefreshBearTokenFileAuthProvider() throws IOException, InterruptedException {
        String fileName = "token";
        Pair<File, String> tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String token = tokenFile2Token.getRight();
        File tokenFile = tokenFile2Token.getLeft();
        long tokenRefreshInMills = 1000L;
        AuthProvider authProvider =
                DlfStSTokenAuthProvider.buildRefreshToken(
                        folder.getRoot().getPath(), fileName, tokenRefreshInMills);
        ScheduledExecutorService executor =
                ThreadPoolUtils.createScheduledThreadPool(1, "refresh-token");
        AuthSession session = AuthSession.fromRefreshAuthProvider(executor, authProvider);
        String authToken = session.getAuthProvider().token();
        assertEquals(authToken, token);
        tokenFile.delete();
        tokenFile2Token = generateTokenAndWriteToFile(fileName);
        token = tokenFile2Token.getRight();
        Thread.sleep(tokenRefreshInMills * 2);
        authToken = session.getAuthProvider().token();
        assertEquals(authToken, token);
    }

    @Test
    public void testRefreshAuthProviderIsSoonExpire() throws IOException, InterruptedException {
        String fileName = "token";
        Pair<File, String> tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String token = tokenFile2Token.getRight();
        File tokenFile = tokenFile2Token.getLeft();
        long tokenRefreshInMills = 1000L;
        AuthProvider authProvider =
                DlfStSTokenAuthProvider.buildRefreshToken(
                        folder.getRoot().getPath(), fileName, tokenRefreshInMills);
        AuthSession session = AuthSession.fromRefreshAuthProvider(null, authProvider);
        String authToken = session.getAuthProvider().token();
        assertEquals(token, authToken);
        tokenFile.delete();
        tokenFile2Token = generateTokenAndWriteToFile(fileName);
        token = tokenFile2Token.getRight();
        tokenFile = tokenFile2Token.getLeft();
        FileUtils.writeStringToFile(tokenFile, token);
        Thread.sleep(
                (long) (tokenRefreshInMills * (1 - DlfStSTokenAuthProvider.EXPIRED_FACTOR)) + 10L);
        authToken = session.getAuthProvider().token();
        assertEquals(token, authToken);
    }

    @Test
    public void testRetryWhenRefreshFail() throws Exception {
        AuthProvider authProvider = Mockito.mock(DlfStSTokenAuthProvider.class);
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

    private Pair<File, String> generateTokenAndWriteToFile(String fileName) throws IOException {
        File tokenFile = folder.newFile(fileName);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        String expiration = sdf.format(new Date());
        String secret = UUID.randomUUID().toString();
        DlfStSTokenAuthProvider.DlfStSToken token =
                new DlfStSTokenAuthProvider.DlfStSToken(
                        "accessKeyId", secret, "securityToken", expiration);
        String tokenStr = OBJECT_MAPPER_INSTANCE.writeValueAsString(token);
        FileUtils.writeStringToFile(tokenFile, tokenStr);
        return Pair.of(tokenFile, tokenStr);
    }
}
