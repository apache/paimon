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

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.time.Duration;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Test for {@link AuthProvider}. */
public class AuthProviderTest {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testCreateBearTokenSuccess() {
        Options options = new Options();
        String token = UUID.randomUUID().toString();
        options.set(RESTCatalogOptions.TOKEN, token);
        BearTokenAuthProvider authProvider = (BearTokenAuthProvider) AuthProvider.create(options);
        assertEquals(token, authProvider.token());
    }

    @Test
    public void testCreateBearTokenFail() {
        Options options = new Options();
        assertThrows(IllegalArgumentException.class, () -> AuthProvider.create(options));
    }

    @Test
    public void testCreateBearTokenFileSuccess() throws Exception {
        Options options = new Options();
        String fileName = "token";
        File tokenFile = folder.newFile(fileName);
        String token = UUID.randomUUID().toString();
        FileUtils.writeStringToFile(tokenFile, token);
        options.set(RESTCatalogOptions.TOKEN_PROVIDER_PATH, tokenFile.getPath());
        BearTokenFileAuthProvider authProvider =
                (BearTokenFileAuthProvider) AuthProvider.create(options);
        assertEquals(token, authProvider.token());
    }

    @Test
    public void testCreateRefreshBearTokenFileSuccess() throws Exception {
        Options options = new Options();
        String fileName = "token";
        File tokenFile = folder.newFile(fileName);
        String token = UUID.randomUUID().toString();
        FileUtils.writeStringToFile(tokenFile, token);
        options.set(RESTCatalogOptions.TOKEN_PROVIDER_PATH, tokenFile.getPath());
        options.set(RESTCatalogOptions.TOKEN_EXPIRATION_TIME, Duration.ofSeconds(10L));
        BearTokenFileAuthProvider authProvider =
                (BearTokenFileAuthProvider) AuthProvider.create(options);
        assertEquals(token, authProvider.token());
    }
}
