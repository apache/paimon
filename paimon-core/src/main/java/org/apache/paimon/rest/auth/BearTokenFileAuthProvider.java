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

import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

/** Auth provider for get bear token from file. */
public class BearTokenFileAuthProvider extends BearTokenAuthProvider {

    public static final double EXPIRED_FACTOR = 0.4;

    private final String tokenFilePath;

    private boolean keepRefreshed = false;
    private Long expiresAtMillis = null;
    private Long expiresInMills = null;

    public BearTokenFileAuthProvider(String tokenFilePath) {
        super(readToken(tokenFilePath));
        this.tokenFilePath = tokenFilePath;
    }

    public BearTokenFileAuthProvider(String tokenFilePath, Long expiresInMills) {
        this(tokenFilePath);
        this.keepRefreshed = true;
        this.expiresAtMillis = -1L;
        this.expiresInMills = expiresInMills;
    }

    @Override
    public boolean refresh() {
        long start = System.currentTimeMillis();
        String newToken = readToken(tokenFilePath);
        if (StringUtils.isNullOrWhitespaceOnly(newToken)) {
            return false;
        }
        this.expiresAtMillis = start + this.expiresInMills;
        this.token = newToken;
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

    private static String readToken(String filePath) {
        try {
            return FileIOUtils.readFileUtf8(new File(filePath));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
