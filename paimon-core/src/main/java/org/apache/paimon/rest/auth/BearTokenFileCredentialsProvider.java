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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

/** credentials provider for get bear token from file. */
public class BearTokenFileCredentialsProvider extends BaseBearTokenCredentialsProvider {

    public static final double EXPIRED_FACTOR = 0.4;

    private final String tokenFilePath;
    private String token;
    private boolean keepRefreshed = false;
    private Long expiresAtMillis = null;
    private Long expiresInMills = null;

    public BearTokenFileCredentialsProvider(String tokenFilePath) {
        this.tokenFilePath = tokenFilePath;
        this.token = getTokenFromFile();
    }

    public BearTokenFileCredentialsProvider(String tokenFilePath, Long expiresInMills) {
        this(tokenFilePath);
        this.keepRefreshed = true;
        this.expiresAtMillis = -1L;
        this.expiresInMills = expiresInMills;
    }

    @Override
    String token() {
        return this.token;
    }

    @Override
    public boolean refresh() {
        long start = System.currentTimeMillis();
        this.token = getTokenFromFile();
        this.expiresAtMillis = start + this.expiresInMills;
        if (StringUtils.isNullOrWhitespaceOnly(this.token)) {
            return false;
        }
        return true;
    }

    @Override
    public boolean supportRefresh() {
        return true;
    }

    @Override
    public boolean keepRefreshed() {
        return this.keepRefreshed;
    }

    @Override
    public boolean willSoonExpire() {
        if (keepRefreshed()) {
            return expiresAtMillis().get() - System.currentTimeMillis()
                    < expiresInMills().get() * EXPIRED_FACTOR;
        } else {
            return false;
        }
    }

    @Override
    public Optional<Long> expiresAtMillis() {
        return Optional.ofNullable(this.expiresAtMillis);
    }

    @Override
    public Optional<Long> expiresInMills() {
        return Optional.ofNullable(this.expiresInMills);
    }

    private String getTokenFromFile() {
        try {
            // todo: handle exception
            return new String(Files.readAllBytes(Paths.get(tokenFilePath)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
