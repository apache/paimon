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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

/** Auth provider for DLF. */
public class DlfAuthProvider implements AuthProvider {
    private static final ObjectMapper OBJECT_MAPPER_INSTANCE = new ObjectMapper();
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final double EXPIRED_FACTOR = 0.4;

    private final String tokenDirPath;
    private final String tokenFileName;

    protected DlfToken token;
    private final boolean keepRefreshed;
    private Long expiresAtMillis;
    private final Long tokenRefreshInMills;

    public static DlfAuthProvider buildRefreshToken(
            String tokenDirPath, String tokenFileName, Long tokenRefreshInMills) {
        DlfToken token = readToken(tokenDirPath, tokenFileName);
        Long expiresAtMillis = token.getExpiresInMills();
        return new DlfAuthProvider(
                tokenDirPath, tokenFileName, token, true, expiresAtMillis, tokenRefreshInMills);
    }

    public static DlfAuthProvider buildAKToken(String accessKeyId, String accessKeySecret) {
        DlfToken token = new DlfToken(accessKeyId, accessKeySecret, null, null);
        Long expiresInMills = -1L;
        Long expiresAtMillis = -1L;
        return new DlfAuthProvider(null, null, token, false, expiresAtMillis, expiresInMills);
    }

    public DlfAuthProvider(
            String tokenDirPath,
            String tokenFileName,
            DlfToken token,
            boolean keepRefreshed,
            Long expiresAtMillis,
            Long tokenRefreshInMills) {
        this.tokenDirPath = tokenDirPath;
        this.tokenFileName = tokenFileName;
        this.token = token;
        this.keepRefreshed = keepRefreshed;
        this.expiresAtMillis = expiresAtMillis;
        this.tokenRefreshInMills = tokenRefreshInMills;
    }

    @Override
    public String generateAuthorization(RestAuthParameter restAuthParameter) {
        String date = getDate();
        String region = getRegion(restAuthParameter.host());
        try {
            return DlfAuthSignature.getAuthorization(
                    restAuthParameter.path(),
                    restAuthParameter.method(),
                    restAuthParameter.query(),
                    restAuthParameter.headers(),
                    token.getAccessKeySecret(),
                    token.getSecurityToken(),
                    token.getAccessKeyId(),
                    region,
                    date);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String token() {
        try {
            return OBJECT_MAPPER_INSTANCE.writeValueAsString(this.token);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean refresh() {
        long start = System.currentTimeMillis();
        DlfToken newToken = readToken(tokenDirPath, tokenFileName);
        if (newToken == null) {
            return false;
        }
        this.expiresAtMillis = start + this.tokenRefreshInMills;
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
                    < tokenRefreshInMills().get() * EXPIRED_FACTOR;
        } else {
            return false;
        }
    }

    @Override
    public Optional<Long> expiresAtMillis() {
        return Optional.ofNullable(this.expiresAtMillis);
    }

    @Override
    public Optional<Long> tokenRefreshInMills() {
        return Optional.ofNullable(this.tokenRefreshInMills);
    }

    private static DlfToken readToken(String tokenDirPath, String tokenFileName) {
        try {
            String tokenStr =
                    FileIOUtils.readFileUtf8(
                            new File(tokenDirPath + File.separator + tokenFileName));
            return OBJECT_MAPPER_INSTANCE.readValue(tokenStr, DlfToken.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String getRegion(String host) {
        try {
            return host.split("\\.")[1];
        } catch (Exception ignore) {

        }
        // fixme
        return "cn-hangzhou";
    }

    private static String getDate() {
        LocalDate currentDate = LocalDate.now();
        return currentDate.format(DATE_FORMATTER);
    }
}
