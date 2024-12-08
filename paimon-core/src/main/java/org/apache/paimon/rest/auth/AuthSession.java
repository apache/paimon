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

import org.apache.paimon.rest.RESTUtil;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Auth session. */
public class AuthSession {
    private static final Logger log = LoggerFactory.getLogger(AuthSession.class);
    private static final int TOKEN_REFRESH_NUM_RETRIES = 5;
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

        if (expiresAtMillisOpt.isPresent() && expiresAtMillisOpt.get() <= startTimeMillis) {
            Pair<Boolean, Long> refreshResult = session.refresh();

            // if expiration is non-null, then token refresh was successful
            boolean isSuccessful = refreshResult.getKey();
            if (isSuccessful) {
                if (session.credentialsProvider.expiresAtMillis().isPresent()) {
                    // use the new expiration time from the refreshed token
                    expiresAtMillisOpt = session.credentialsProvider.expiresAtMillis();
                } else {
                    // otherwise use the expiration time from the token response
                    expiresAtMillisOpt = Optional.of(startTimeMillis + refreshResult.getValue());
                }
            } else {
                // token refresh failed, don't reattempt with the original expiration
                expiresAtMillisOpt = Optional.empty();
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

    private static void scheduleTokenRefresh(
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
            // how much ahead of time to start the request to allow it to complete
            long refreshWindowMillis = Math.min(expiresInMillis / 10, MAX_REFRESH_WINDOW_MILLIS);
            // how much time to wait before expiration
            long waitIntervalMillis = expiresInMillis - refreshWindowMillis;
            // how much time to actually wait
            long timeToWait = Math.max(waitIntervalMillis, MIN_REFRESH_WAIT_MILLIS);

            executor.schedule(
                    () -> {
                        long refreshStartTime = System.currentTimeMillis();
                        Pair<Boolean, Long> refreshResult = session.refresh();
                        boolean isSuccessful = refreshResult.getKey();
                        if (isSuccessful) {
                            scheduleTokenRefresh(
                                    executor,
                                    session,
                                    refreshStartTime + refreshResult.getValue(),
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

    public Pair<Boolean, Long> refresh() {
        if (this.credentialsProvider.supportRefresh()
                && this.credentialsProvider.keepRefreshed()
                && this.credentialsProvider.expiresInMills().isPresent()) {
            boolean isSuccessful = this.credentialsProvider.refresh();
            if (!isSuccessful) {
                return Pair.of(false, 0L);
            }
            Map<String, String> currentHeaders = this.headers;
            this.headers = RESTUtil.merge(currentHeaders, this.credentialsProvider.authHeader());
            return Pair.of(true, credentialsProvider.expiresInMills().get());
        }

        return Pair.of(false, 0L);
    }
}
