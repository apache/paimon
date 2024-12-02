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

import org.apache.paimon.rest.RESTClient;
import org.apache.paimon.rest.RESTUtil;
import org.apache.paimon.utils.Pair;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.rest.auth.AuthUtil.authHeaders;

/** Auth session. */
public class AuthSession {
    private static int tokenRefreshNumRetries = 5;
    private static final long MAX_REFRESH_WINDOW_MILLIS = 300_000; // 5 minutes
    private static final long MIN_REFRESH_WAIT_MILLIS = 10;
    private volatile Map<String, String> headers;
    private volatile AuthOptions config;

    public AuthSession(Map<String, String> headers, AuthOptions config) {
        this.headers = headers;
        this.config = config;
    }

    public static AuthSession fromAccessToken(
            RESTClient client,
            ScheduledExecutorService executor,
            String token,
            Map<String, String> headers,
            AuthOptions config,
            Long defaultExpiresAtMillis) {
        AuthSession session = new AuthSession(headers, config);

        long startTimeMillis = System.currentTimeMillis();
        Long expiresAtMillis = session.config.expiresAtMillis();

        if (null != expiresAtMillis && expiresAtMillis <= startTimeMillis) {
            Pair<Long, TimeUnit> expiration = session.refresh(client);
            // if expiration is non-null, then token refresh was successful
            if (expiration != null) {
                if (null != config.expiresAtMillis()) {
                    // use the new expiration time from the refreshed token
                    expiresAtMillis = config.expiresAtMillis();
                } else {
                    // otherwise use the expiration time from the token response
                    expiresAtMillis = startTimeMillis + expiration.getKey();
                }
            } else {
                // token refresh failed, don't reattempt with the original expiration
                expiresAtMillis = null;
            }
        } else if (null == expiresAtMillis && defaultExpiresAtMillis != null) {
            expiresAtMillis = defaultExpiresAtMillis;
        }

        if (null != executor && null != expiresAtMillis) {
            scheduleTokenRefresh(client, executor, session, expiresAtMillis);
        }

        return session;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    private static void scheduleTokenRefresh(
            RESTClient client,
            ScheduledExecutorService executor,
            AuthSession session,
            long expiresAtMillis) {
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
                    Pair<Long, TimeUnit> expiration = session.refresh(client);
                    if (expiration != null) {
                        scheduleTokenRefresh(
                                client, executor, session, refreshStartTime + expiration.getKey());
                    }
                },
                timeToWait,
                TimeUnit.MILLISECONDS);
    }

    public Pair<Long, TimeUnit> refresh(RESTClient client) {
        if (config.token() != null && config.keepRefreshed()) {
            long startTimeMillis = System.currentTimeMillis();
            AuthOptions authOptions = refreshExpiredToken(client);
            boolean isSuccessful = authOptions.token() != null;
            if (!isSuccessful) {
                return null;
            }
            long expiresAtMillis = startTimeMillis + authOptions.expiresInMills();
            this.config =
                    new AuthOptions(
                            authOptions.token(),
                            config.keepRefreshed(),
                            expiresAtMillis,
                            authOptions.expiresInMills());
            Map<String, String> currentHeaders = this.headers;
            this.headers = RESTUtil.merge(currentHeaders, authHeaders(config.token()));

            if (authOptions.expiresInMills() != null) {
                return Pair.of(authOptions.expiresInMills(), TimeUnit.SECONDS);
            }
        }

        return null;
    }

    private AuthOptions refreshExpiredToken(RESTClient client) {
        // todo: update the token
        return new AuthOptions("token", config.keepRefreshed(), null, this.config.expiresInMills());
    }
}
