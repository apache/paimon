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

import static org.apache.paimon.rest.auth.AuthSession.MAX_REFRESH_WINDOW_MILLIS;
import static org.apache.paimon.rest.auth.AuthSession.MIN_REFRESH_WAIT_MILLIS;
import static org.apache.paimon.rest.auth.AuthSession.TOKEN_REFRESH_NUM_RETRIES;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link AuthSession}. */
public class AuthSessionTest {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testRefreshBearTokenFileCredentialsProvider()
            throws IOException, InterruptedException {
        String fileName = "token";
        Pair<File, String> tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String token = tokenFile2Token.getRight();
        File tokenFile = tokenFile2Token.getLeft();
        Map<String, String> initialHeaders = new HashMap<>();
        long expiresInMillis = 1000L;
        CredentialsProvider credentialsProvider =
                new BearTokenFileCredentialsProvider(tokenFile.getPath(), expiresInMillis);
        ScheduledExecutorService executor =
                ThreadPoolUtils.createScheduledThreadPool(1, "refresh-token");
        AuthSession session =
                AuthSession.fromRefreshCredentialsProvider(
                        executor, initialHeaders, credentialsProvider);
        Map<String, String> header = session.getHeaders();
        assertEquals(header.get("Authorization"), "Bearer " + token);
        tokenFile.delete();
        tokenFile2Token = generateTokenAndWriteToFile(fileName);
        token = tokenFile2Token.getRight();
        Thread.sleep(expiresInMillis + 500L);
        header = session.getHeaders();
        assertEquals(header.get("Authorization"), "Bearer " + token);
    }

    @Test
    public void testRefreshCredentialsProviderIsSoonExpire()
            throws IOException, InterruptedException {
        String fileName = "token";
        Pair<File, String> tokenFile2Token = generateTokenAndWriteToFile(fileName);
        String token = tokenFile2Token.getRight();
        File tokenFile = tokenFile2Token.getLeft();
        Map<String, String> initialHeaders = new HashMap<>();
        long expiresInMillis = 1000L;
        CredentialsProvider credentialsProvider =
                new BearTokenFileCredentialsProvider(tokenFile.getPath(), expiresInMillis);
        AuthSession session =
                AuthSession.fromRefreshCredentialsProvider(
                        null, initialHeaders, credentialsProvider);
        Map<String, String> header = session.getHeaders();
        assertEquals(header.get("Authorization"), "Bearer " + token);
        tokenFile.delete();
        tokenFile2Token = generateTokenAndWriteToFile(fileName);
        token = tokenFile2Token.getRight();
        tokenFile = tokenFile2Token.getLeft();
        FileUtils.writeStringToFile(tokenFile, token);
        Thread.sleep(
                (long) (expiresInMillis * (1 - BearTokenFileCredentialsProvider.EXPIRED_FACTOR))
                        + 10L);
        header = session.getHeaders();
        assertEquals(header.get("Authorization"), "Bearer " + token);
    }

    @Test
    public void testRetryWhenRefreshFail() throws Exception {
        Map<String, String> initialHeaders = new HashMap<>();
        CredentialsProvider credentialsProvider =
                Mockito.mock(BearTokenFileCredentialsProvider.class);
        long expiresAtMillis = System.currentTimeMillis() - 1000L;
        when(credentialsProvider.expiresAtMillis()).thenReturn(Optional.of(expiresAtMillis));
        when(credentialsProvider.expiresInMills()).thenReturn(Optional.of(50L));
        when(credentialsProvider.supportRefresh()).thenReturn(true);
        when(credentialsProvider.keepRefreshed()).thenReturn(true);
        when(credentialsProvider.refresh()).thenReturn(false);
        AuthSession session =
                AuthSession.fromRefreshCredentialsProvider(
                        null, initialHeaders, credentialsProvider);
        AuthSession.scheduleTokenRefresh(
                ThreadPoolUtils.createScheduledThreadPool(1, "refresh-token"),
                session,
                expiresAtMillis);
        Thread.sleep(10_000L);
        verify(credentialsProvider, Mockito.times(TOKEN_REFRESH_NUM_RETRIES + 1)).refresh();
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
        String token = UUID.randomUUID().toString();
        FileUtils.writeStringToFile(tokenFile, token);
        return Pair.of(tokenFile, token);
    }
}
