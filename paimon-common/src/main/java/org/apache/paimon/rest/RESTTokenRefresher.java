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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.GetTableTokenResponse;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

import static org.apache.paimon.rest.RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_OSS_ENDPOINT;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_ENABLED;

/**
 * Loads the data token of a table from the REST server and reloads it before it expires, for {@link
 * RESTTokenFileIO} and for storage credentials providers created from its options.
 */
public class RESTTokenRefresher {

    private static final Logger LOG = LoggerFactory.getLogger(RESTTokenRefresher.class);

    /** Database of the table whose token is refreshed, set by {@link RESTTokenFileIO}. */
    public static final String DATABASE = "data-token.database";

    /** Object name of the table whose token is refreshed, including any branch. */
    public static final String OBJECT = "data-token.object";

    /** Expiration of the token already present in the options. */
    public static final String EXPIRES_AT_MILLIS = "data-token.expires-at-millis";

    /** Keys of the token already present in the options, replaced as a whole on reload. */
    public static final String TOKEN_KEYS = "data-token.keys";

    // A reloaded token is kept at least this long, and a failed reload is retried after it.
    static final long RELOAD_INTERVAL_MILLIS = 10_000L;

    // Shifts the clock of refreshers that use the system clock, for tests only.
    @VisibleForTesting static volatile long clockOffsetMillis;

    private final Options catalogOptions;
    private final Identifier identifier;
    private final LongSupplier clock;
    private final ReentrantLock lock = new ReentrantLock();

    @Nullable private volatile CachedToken cached;

    // Guarded by lock.
    @Nullable private RESTApi api;
    private long nextAttemptMillis;
    @Nullable private RuntimeException lastFailure;

    RESTTokenRefresher(
            Options catalogOptions,
            Identifier identifier,
            @Nullable RESTApi api,
            @Nullable RESTToken token,
            LongSupplier clock) {
        this.catalogOptions = catalogOptions;
        this.identifier = identifier;
        this.api = api;
        this.clock = clock;
        this.cached = token == null ? null : new CachedToken(token, currentTimeMillis());
    }

    /** Describes the table and the token merged into the options, so a refresher can be created. */
    public static void configure(Options options, Identifier identifier, RESTToken token) {
        options.set(DATABASE, identifier.getDatabaseName());
        options.set(OBJECT, identifier.getObjectName());
        options.set(EXPIRES_AT_MILLIS, String.valueOf(token.expireAtMillis()));
        options.set(TOKEN_KEYS, String.join(",", token.token().keySet()));
    }

    /** Whether the options name a table, see {@link #configure}. */
    public static boolean isConfigured(Options options) {
        return options.containsKey(DATABASE) && options.containsKey(OBJECT);
    }

    /**
     * Creates a refresher from catalog options that also carry the current token and the keys set
     * by {@link #configure}.
     */
    public static RESTTokenRefresher fromOptions(Options options) {
        return fromOptions(options, null);
    }

    @VisibleForTesting
    static RESTTokenRefresher fromOptions(Options options, @Nullable RESTApi api) {
        Identifier identifier = new Identifier(options.get(DATABASE), options.get(OBJECT));
        String expiresAt = options.get(EXPIRES_AT_MILLIS);
        String tokenKeys = options.get(TOKEN_KEYS);
        RESTToken token = null;
        if (expiresAt != null && tokenKeys != null) {
            Map<String, String> values = new LinkedHashMap<>();
            for (String key : tokenKeys.split(",")) {
                String value = options.get(key);
                if (value != null) {
                    values.put(key, value);
                }
            }
            token = new RESTToken(values, Long.parseLong(expiresAt));
        }
        return new RESTTokenRefresher(
                options, identifier, api, token, RESTTokenRefresher::systemTimeMillis);
    }

    /** Returns the current token, reloading it from the catalog when it is about to expire. */
    public RESTToken token() {
        CachedToken current = cached;
        long now = currentTimeMillis();
        if (current != null && now < current.reloadAtMillis) {
            return current.token;
        }
        // While the token is still valid, one caller reloads it and the others keep using it.
        if (current != null && now < current.token.expireAtMillis()) {
            if (!lock.tryLock()) {
                return current.token;
            }
        } else {
            lock.lock();
        }
        try {
            return reload();
        } finally {
            lock.unlock();
        }
    }

    /** Returns the current token, or reloads it when it has less than the given time left. */
    public RESTToken tokenValidFor(long minimumValidityMillis) {
        RESTToken token = token();
        if (remainingMillis(token) >= minimumValidityMillis) {
            return token;
        }
        lock.lock();
        try {
            CachedToken current = cached;
            if (current != null && remainingMillis(current.token) >= minimumValidityMillis) {
                return current.token;
            }
            // A failed reload is not repeated before its retry, as in token().
            if (currentTimeMillis() < nextAttemptMillis) {
                throw new IllegalStateException(
                        "The data token of "
                                + identifier
                                + " has less than "
                                + minimumValidityMillis
                                + " ms left and reloading it failed.",
                        lastFailure);
            }
            try {
                return loadAndCache();
            } catch (RuntimeException e) {
                failed(e);
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    /** Milliseconds left until the token expires. */
    public long remainingMillis(RESTToken token) {
        return token.expireAtMillis() - currentTimeMillis();
    }

    private RESTToken reload() {
        CachedToken current = cached;
        long now = currentTimeMillis();
        if (current != null && now < current.reloadAtMillis) {
            return current.token;
        }
        boolean valid = current != null && now < current.token.expireAtMillis();
        if (now < nextAttemptMillis) {
            if (valid) {
                return current.token;
            }
            throw new IllegalStateException(
                    "The data token of " + identifier + " expired and reloading it failed.",
                    lastFailure);
        }
        try {
            return loadAndCache();
        } catch (RuntimeException e) {
            // A slow request must not outlive the current token.
            long failedAt = failed(e);
            if (current == null || failedAt >= current.token.expireAtMillis()) {
                throw e;
            }
            LOG.warn(
                    "Failed to reload the data token of {}, keeping the current one.",
                    identifier,
                    e);
            return current.token;
        }
    }

    private RESTToken loadAndCache() {
        RESTToken loaded = load();
        cached = new CachedToken(loaded, currentTimeMillis());
        lastFailure = null;
        return loaded;
    }

    /** Records a failed reload and returns when it failed, which starts the retry interval. */
    private long failed(RuntimeException e) {
        long failedAt = currentTimeMillis();
        lastFailure = e;
        nextAttemptMillis = failedAt + RELOAD_INTERVAL_MILLIS;
        return failedAt;
    }

    private RESTToken load() {
        if (api == null) {
            api = new RESTApi(catalogOptions, false);
        }
        LOG.info("begin refresh data token for identifier [{}]", identifier);
        GetTableTokenResponse response = api.loadTableToken(identifier);
        LOG.info(
                "end refresh data token for identifier [{}] expiresAtMillis [{}]",
                identifier,
                response.getExpiresAtMillis());
        return new RESTToken(
                withCatalogOverrides(response.getToken()), response.getExpiresAtMillis());
    }

    private Map<String, String> withCatalogOverrides(Map<String, String> token) {
        Map<String, String> newToken = new LinkedHashMap<>(token);
        // DLF OSS endpoint should override the standard OSS endpoint.
        String dlfOssEndpoint = catalogOptions.get(DLF_OSS_ENDPOINT.key());
        if (dlfOssEndpoint != null && !dlfOssEndpoint.isEmpty()) {
            newToken.put("fs.oss.endpoint", dlfOssEndpoint);
        }
        if (catalogOptions.contains(IO_CACHE_ENABLED)) {
            newToken.put(
                    IO_CACHE_ENABLED.key(), String.valueOf(catalogOptions.get(IO_CACHE_ENABLED)));
        }
        return ImmutableMap.copyOf(newToken);
    }

    private long currentTimeMillis() {
        return clock.getAsLong();
    }

    /** The system clock, shifted by {@link #clockOffsetMillis} in tests. */
    static long systemTimeMillis() {
        return System.currentTimeMillis() + clockOffsetMillis;
    }

    /** A token and the time to reload it, before it expires but not right after it arrived. */
    private static final class CachedToken {

        private final RESTToken token;
        private final long reloadAtMillis;

        private CachedToken(RESTToken token, long now) {
            long expiresAt = token.expireAtMillis();
            // Ahead of expiry by the safe time, or by half the time left when that is shorter.
            long ahead = Math.min(TOKEN_EXPIRATION_SAFE_TIME_MILLIS, (expiresAt - now) / 2);
            this.token = token;
            this.reloadAtMillis =
                    Math.min(expiresAt, Math.max(expiresAt - ahead, now + RELOAD_INTERVAL_MILLIS));
        }
    }
}
