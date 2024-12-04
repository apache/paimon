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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Auth session. */
public class AuthSession {
    private static int tokenRefreshNumRetries = 5;
    private static final long MAX_REFRESH_WINDOW_MILLIS = 300_000; // 5 minutes
    private static final long MIN_REFRESH_WAIT_MILLIS = 10;
    private volatile Map<String, String> headers;
    private final CredentialsProvider credentialsProvider;

    public AuthSession(Map<String, String> headers, CredentialsProvider credentialsProvider) {
        this.headers = headers;
        this.credentialsProvider = credentialsProvider;
    }

    public static AuthSession fromTokenPath(
            ScheduledExecutorService executor,
            Map<String, String> headers,
            CredentialsProvider credentialsProvider,
            Long defaultExpiresAtMillis) {
        AuthSession session = new AuthSession(headers, credentialsProvider);

        long startTimeMillis = System.currentTimeMillis();
        Optional<Long> expiresAtMillisOpt = credentialsProvider.expiresAtMillis();

        if (expiresAtMillisOpt.isPresent() && expiresAtMillisOpt.get() <= startTimeMillis) {
            Pair<Long, TimeUnit> expiration = session.refresh();

            // if expiration is non-null, then token refresh was successful
            if (expiration != null) {
                if (session.credentialsProvider.expiresAtMillis().isPresent()) {
                    // use the new expiration time from the refreshed token
                    expiresAtMillisOpt = session.credentialsProvider.expiresAtMillis();
                } else {
                    // otherwise use the expiration time from the token response
                    expiresAtMillisOpt = Optional.of(startTimeMillis + expiration.getKey());
                }
            } else {
                // token refresh failed, don't reattempt with the original expiration
                expiresAtMillisOpt = Optional.empty();
            }
        } else if (expiresAtMillisOpt.isPresent() && defaultExpiresAtMillis != null) {
            expiresAtMillisOpt = Optional.of(defaultExpiresAtMillis);
        }

        if (null != executor && expiresAtMillisOpt.isPresent()) {
            scheduleTokenRefresh(executor, session, expiresAtMillisOpt.get());
        }

        return session;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    private static void scheduleTokenRefresh(
            ScheduledExecutorService executor, AuthSession session, long expiresAtMillis) {
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
                    Pair<Long, TimeUnit> expiration = session.refresh();
                    if (expiration != null) {
                        scheduleTokenRefresh(
                                executor, session, refreshStartTime + expiration.getKey());
                    }
                },
                timeToWait,
                TimeUnit.MILLISECONDS);
    }

    public Pair<Long, TimeUnit> refresh() {
        if (this.credentialsProvider.supportRefresh() && this.credentialsProvider.keepRefreshed()) {
            boolean isSuccessful = this.credentialsProvider.refresh();
            if (!isSuccessful) {
                return null;
            }
            Map<String, String> currentHeaders = this.headers;
            this.headers = RESTUtil.merge(currentHeaders, this.credentialsProvider.authHeader());

            if (credentialsProvider.expiresInMills().isPresent()) {
                return Pair.of(credentialsProvider.expiresInMills().get(), TimeUnit.SECONDS);
            }
        }

        return null;
    }
}
