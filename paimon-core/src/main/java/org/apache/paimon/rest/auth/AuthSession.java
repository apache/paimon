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
import org.apache.paimon.rest.RESTUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Auth session. */
public class AuthSession {

    static final int TOKEN_REFRESH_NUM_RETRIES = 5;
    private static final Logger log = LoggerFactory.getLogger(AuthSession.class);
    private static final long MAX_REFRESH_WINDOW_MILLIS = 300_000; // 5 minutes
    private static final long MIN_REFRESH_WAIT_MILLIS = 10;
    private final CredentialsProvider credentialsProvider;
    private volatile Map<String, String> headers;

    public AuthSession(Map<String, String> headers, CredentialsProvider credentialsProvider) {
        this.headers = headers;
        this.credentialsProvider = credentialsProvider;
    }

    public static AuthSession fromRefreshCredentialsProvider(
            ScheduledExecutorService executor,
            Map<String, String> headers,
            CredentialsProvider credentialsProvider) {
        AuthSession session = new AuthSession(headers, credentialsProvider);

        long startTimeMillis = System.currentTimeMillis();
        Optional<Long> expiresAtMillisOpt = credentialsProvider.expiresAtMillis();

        // when init session if credentials expire time is in the past, refresh it and update
        // expiresAtMillis
        if (expiresAtMillisOpt.isPresent() && expiresAtMillisOpt.get() <= startTimeMillis) {
            boolean refreshSuccessful = session.refresh();
            if (refreshSuccessful) {
                expiresAtMillisOpt = session.credentialsProvider.expiresAtMillis();
            }
        }

        if (null != executor && expiresAtMillisOpt.isPresent()) {
            scheduleTokenRefresh(executor, session, expiresAtMillisOpt.get());
        }

        return session;
    }

    public Map<String, String> getHeaders() {
        if (this.credentialsProvider.keepRefreshed() && this.credentialsProvider.willSoonExpire()) {
            refresh();
        }
        return headers;
    }

    @VisibleForTesting
    static void scheduleTokenRefresh(
            ScheduledExecutorService executor, AuthSession session, long expiresAtMillis) {
        scheduleTokenRefresh(executor, session, expiresAtMillis, 0);
    }

    private static void scheduleTokenRefresh(
            ScheduledExecutorService executor,
            AuthSession session,
            long expiresAtMillis,
            int retryTimes) {
        if (retryTimes < TOKEN_REFRESH_NUM_RETRIES) {
            long expiresInMillis = expiresAtMillis - System.currentTimeMillis();
            // how much ahead of time to start the refresh to allow it to complete
            long refreshWindowMillis = Math.min(expiresInMillis, MAX_REFRESH_WINDOW_MILLIS);
            // how much time to wait before expiration
            long waitIntervalMillis = expiresInMillis - refreshWindowMillis;
            // how much time to actually wait
            long timeToWait = Math.max(waitIntervalMillis, MIN_REFRESH_WAIT_MILLIS);

            executor.schedule(
                    () -> {
                        long refreshStartTime = System.currentTimeMillis();
                        boolean isSuccessful = session.refresh();
                        if (isSuccessful) {
                            scheduleTokenRefresh(
                                    executor,
                                    session,
                                    refreshStartTime
                                            + session.credentialsProvider.expiresInMills().get(),
                                    0);
                        } else {
                            scheduleTokenRefresh(
                                    executor, session, expiresAtMillis, retryTimes + 1);
                        }
                    },
                    timeToWait,
                    TimeUnit.MILLISECONDS);
        } else {
            log.warn("Failed to refresh token after {} retries.", TOKEN_REFRESH_NUM_RETRIES);
        }
    }

    public Boolean refresh() {
        if (this.credentialsProvider.supportRefresh()
                && this.credentialsProvider.keepRefreshed()
                && this.credentialsProvider.expiresInMills().isPresent()) {
            boolean isSuccessful = this.credentialsProvider.refresh();
            if (isSuccessful) {
                Map<String, String> currentHeaders = this.headers;
                this.headers =
                        RESTUtil.merge(currentHeaders, this.credentialsProvider.authHeader());
            }
            return isSuccessful;
        }

        return false;
    }
}
