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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.utils.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Authentication session. */
public class AuthSession {

    static final int REFRESH_NUM_RETRIES = 5;
    static final long MIN_REFRESH_WAIT_MILLIS = 10;
    static final long MAX_REFRESH_WINDOW_MILLIS = 300_000; // 5 minutes

    private static final Logger LOG = LoggerFactory.getLogger(AuthSession.class);

    private final AuthProvider authProvider;

    public AuthSession(AuthProvider authProvider) {
        this.authProvider = authProvider;
    }

    public static AuthSession fromRefreshAuthProvider(
            ScheduledExecutorService executor, AuthProvider authProvider) {
        AuthSession session = new AuthSession(authProvider);

        long startTimeMillis = System.currentTimeMillis();
        Optional<Long> expiresAtMillisOpt = authProvider.expiresAtMillis();

        // when init session if token expire time is in the past, refresh it and update
        // expiresAtMillis
        if (expiresAtMillisOpt.isPresent() && expiresAtMillisOpt.get() <= startTimeMillis) {
            boolean refreshSuccessful = session.refresh();
            if (refreshSuccessful) {
                expiresAtMillisOpt = session.authProvider.expiresAtMillis();
            }
        }

        if (null != executor && expiresAtMillisOpt.isPresent()) {
            scheduleTokenRefresh(executor, session, expiresAtMillisOpt.get());
        }

        return session;
    }

    public AuthProvider getAuthProvider() {
        if (this.authProvider.keepRefreshed() && this.authProvider.willSoonExpire()) {
            refresh();
        }
        return this.authProvider;
    }

    public Boolean refresh() {
        if (this.authProvider.keepRefreshed()
                && this.authProvider.tokenRefreshInMills().isPresent()) {
            return this.authProvider.refresh();
        }

        return false;
    }

    @VisibleForTesting
    static void scheduleTokenRefresh(
            ScheduledExecutorService executor, AuthSession session, long expiresAtMillis) {
        scheduleTokenRefresh(executor, session, expiresAtMillis, 0);
    }

    @VisibleForTesting
    static long getTimeToWaitByExpiresInMills(long expiresInMillis) {
        // how much ahead of time to start the refresh to allow it to complete
        long refreshWindowMillis = Math.min(expiresInMillis, MAX_REFRESH_WINDOW_MILLIS);
        // how much time to wait before expiration
        long waitIntervalMillis = expiresInMillis - refreshWindowMillis;
        // how much time to actually wait
        return Math.max(waitIntervalMillis, MIN_REFRESH_WAIT_MILLIS);
    }

    private static void scheduleTokenRefresh(
            ScheduledExecutorService executor,
            AuthSession session,
            long expiresAtMillis,
            int retryTimes) {
        if (retryTimes < REFRESH_NUM_RETRIES) {
            long expiresInMillis = expiresAtMillis - System.currentTimeMillis();
            long timeToWait = getTimeToWaitByExpiresInMills(expiresInMillis);

            executor.schedule(
                    () -> doRefresh(executor, session, expiresAtMillis, retryTimes),
                    timeToWait,
                    TimeUnit.MILLISECONDS);
        } else {
            LOG.warn("Failed to refresh token after {} retries.", REFRESH_NUM_RETRIES);
        }
    }

    private static void doRefresh(
            ScheduledExecutorService executor,
            AuthSession session,
            long expiresAtMillis,
            int retryTimes) {
        long refreshStartTime = System.currentTimeMillis();
        boolean isSuccessful = session.refresh();
        if (isSuccessful) {
            scheduleTokenRefresh(
                    executor,
                    session,
                    refreshStartTime + session.authProvider.tokenRefreshInMills().get(),
                    0);
        } else {
            scheduleTokenRefresh(executor, session, expiresAtMillis, retryTimes + 1);
        }
    }

    public static AuthSession createAuthSession(
            Options options, ScheduledExecutorService refreshExecutor) {
        String tokenProvider = options.get(RESTCatalogOptions.TOKEN_PROVIDER);
        if (StringUtils.isEmpty(tokenProvider)) {
            throw new IllegalArgumentException("token.provider is not set.");
        }
        AuthProvider authProvider = AuthProviderFactory.createAuthProvider(tokenProvider, options);
        if (authProvider.keepRefreshed()) {
            return AuthSession.fromRefreshAuthProvider(refreshExecutor, authProvider);
        } else {
            return new AuthSession(authProvider);
        }
    }
}
