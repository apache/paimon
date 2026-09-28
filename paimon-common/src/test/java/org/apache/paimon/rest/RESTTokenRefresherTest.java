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

package org.apache.paimon.rest;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.GetTableTokenResponse;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link RESTTokenRefresher}. */
class RESTTokenRefresherTest {

    private static final long NOW = 1700000000000L;

    private final Identifier identifier = new Identifier("db", "table", "b1");
    private final AtomicLong now = new AtomicLong(NOW);
    private final RESTApi api = mock(RESTApi.class);

    @Test
    void testKeepsTheTokenUntilItIsAboutToExpire() {
        when(api.loadTableToken(identifier)).thenReturn(response("second", hours(4)));
        RESTTokenRefresher refresher = refresher(token("first", hours(2)));

        assertThat(refresher.token().token()).containsEntry("k", "first");
        verify(api, never()).loadTableToken(identifier);

        // 30 minutes left is inside the refresh window
        now.addAndGet(Duration.ofMinutes(90).toMillis());
        assertThat(refresher.token().token()).containsEntry("k", "second");
        assertThat(refresher.token().token()).containsEntry("k", "second");
        verify(api, times(1)).loadTableToken(identifier);
    }

    @Test
    void testKeepsTheCurrentTokenWhileRefreshFailsAndRetriesLater() {
        when(api.loadTableToken(identifier))
                .thenThrow(new IllegalStateException("REST server unavailable"))
                .thenReturn(response("second", hours(4)));
        RESTTokenRefresher refresher = refresher(token("first", Duration.ofMinutes(30)));
        // past half of its lifetime, so it is due for a reload
        now.addAndGet(Duration.ofMinutes(20).toMillis());

        assertThat(refresher.token().token()).containsEntry("k", "first");
        assertThat(refresher.token().token()).containsEntry("k", "first");
        verify(api, times(1)).loadTableToken(identifier);

        now.addAndGet(RESTTokenRefresher.RELOAD_INTERVAL_MILLIS);
        assertThat(refresher.token().token()).containsEntry("k", "second");
        verify(api, times(2)).loadTableToken(identifier);
    }

    @Test
    void testOtherCallersKeepTheValidTokenWhileOneReloads() throws Exception {
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(api.loadTableToken(identifier))
                .thenAnswer(
                        invocation -> {
                            loading.countDown();
                            release.await();
                            return response("second", hours(4));
                        });
        RESTTokenRefresher refresher = refresher(token("first", Duration.ofMinutes(30)));
        now.addAndGet(Duration.ofMinutes(20).toMillis());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<RESTToken> reloading = executor.submit(refresher::token);
            loading.await();

            // does not wait for the reload in progress
            assertThat(refresher.token().token()).containsEntry("k", "first");
            release.countDown();
            assertThat(reloading.get().token()).containsEntry("k", "second");
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testShortLivedTokenIsReusedUntilHalfOfItsLifetime() {
        when(api.loadTableToken(identifier))
                .thenAnswer(invocation -> response("next", Duration.ofMinutes(30)));
        RESTTokenRefresher refresher = refresher(token("first", Duration.ofMinutes(30)));

        now.addAndGet(Duration.ofMinutes(16).toMillis());
        for (int i = 0; i < 20; i++) {
            assertThat(refresher.token().token()).containsEntry("k", "next");
        }
        verify(api, times(1)).loadTableToken(identifier);

        now.addAndGet(Duration.ofMinutes(16).toMillis());
        refresher.token();
        verify(api, times(2)).loadTableToken(identifier);
    }

    @Test
    void testExpiredTokenFailsFastWithinTheRetryInterval() {
        when(api.loadTableToken(identifier))
                .thenThrow(new IllegalStateException("REST server unavailable"));
        RESTTokenRefresher refresher = refresher(token("first", Duration.ofMinutes(-1)));

        assertThatThrownBy(refresher::token).hasMessageContaining("REST server unavailable");
        assertThatThrownBy(refresher::token)
                .hasMessageContaining("reloading it failed")
                .hasRootCauseMessage("REST server unavailable");
        verify(api, times(1)).loadTableToken(identifier);
    }

    @Test
    void testConfigureRoundTripsTheTable() {
        Options options = new Options();
        options.set("k", "first");
        Identifier dotted = new Identifier("my.db", "table", "b1");
        long expiresAt = System.currentTimeMillis() + hours(2).toMillis();
        RESTTokenRefresher.configure(options, dotted, expiresAt);

        assertThat(RESTTokenRefresher.isConfigured(options)).isTrue();
        assertThat(RESTTokenRefresher.isConfigured(new Options())).isFalse();
        RESTTokenRefresher refresher = RESTTokenRefresher.fromOptions(options);
        // the token already in the options is used without a request
        assertThat(refresher.token().token()).containsEntry("k", "first");
        assertThat(refresher.token().expireAtMillis()).isEqualTo(expiresAt);
        assertThat(options.get(RESTTokenRefresher.DATABASE)).isEqualTo("my.db");
        assertThat(options.get(RESTTokenRefresher.OBJECT)).isEqualTo("table$branch_b1");
    }

    private RESTTokenRefresher refresher(RESTToken token) {
        return new RESTTokenRefresher(new Options(), identifier, api, token) {
            @Override
            long currentTimeMillis() {
                return now.get();
            }
        };
    }

    private RESTToken token(String value, Duration lifetime) {
        return new RESTToken(Collections.singletonMap("k", value), now.get() + lifetime.toMillis());
    }

    private GetTableTokenResponse response(String value, Duration lifetime) {
        return new GetTableTokenResponse(
                Collections.singletonMap("k", value), now.get() + lifetime.toMillis());
    }

    private static Duration hours(int hours) {
        return Duration.ofHours(hours);
    }
}
