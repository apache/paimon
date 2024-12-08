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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;

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

    private Pair<File, String> generateTokenAndWriteToFile(String fileName) throws IOException {
        File tokenFile = folder.newFile(fileName);
        String token = UUID.randomUUID().toString();
        FileUtils.writeStringToFile(tokenFile, token);
        return Pair.of(tokenFile, token);
    }
}
