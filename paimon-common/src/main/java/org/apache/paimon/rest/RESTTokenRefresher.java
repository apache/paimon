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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.locks.ReentrantLock;

import static org.apache.paimon.rest.RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS;

/**
 * Loads the data token of a table from the REST server and refreshes it before it expires, so a
 * storage credentials provider can refresh by itself from the options it was created with.
 */
public class RESTTokenRefresher {

    private static final Logger LOG = LoggerFactory.getLogger(RESTTokenRefresher.class);

    /** Database of the table whose token is refreshed, set by {@link RESTTokenFileIO}. */
    public static final String DATABASE = "data-token.database";

    /** Object name of the table whose token is refreshed, including any branch. */
    public static final String OBJECT = "data-token.object";

    /** Expiration of the token already present in the options. */
    public static final String EXPIRES_AT_MILLIS = "data-token.expires-at-millis";

    // A reloaded token is kept at least this long, and a failed reload is retried after it.
    static final long RELOAD_INTERVAL_MILLIS = 10_000L;

    // Shifts the clock of refreshers that providers create from options, for tests only.
    @VisibleForTesting static volatile long clockOffsetMillis;

    private final Options catalogOptions;
    private final Identifier identifier;
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
            @Nullable RESTToken token) {
        this.catalogOptions = catalogOptions;
        this.identifier = identifier;
        this.api = api;
        this.cached = token == null ? null : new CachedToken(token, currentTimeMillis());
    }

    /** Names the table and the expiration of the merged token, so a refresher can be created. */
    public static void configure(Options options, Identifier identifier, long expiresAtMillis) {
        options.set(DATABASE, identifier.getDatabaseName());
        options.set(OBJECT, identifier.getObjectName());
        options.set(EXPIRES_AT_MILLIS, String.valueOf(expiresAtMillis));
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
        Identifier identifier = new Identifier(options.get(DATABASE), options.get(OBJECT));
        String expiresAt = options.get(EXPIRES_AT_MILLIS);
        RESTToken token =
                expiresAt == null
                        ? null
                        : new RESTToken(options.toMap(), Long.parseLong(expiresAt));
        return new RESTTokenRefresher(options, identifier, null, token);
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
            RESTToken loaded = load();
            cached = new CachedToken(loaded, now);
            lastFailure = null;
            return loaded;
        } catch (RuntimeException e) {
            lastFailure = e;
            nextAttemptMillis = now + RELOAD_INTERVAL_MILLIS;
            if (!valid) {
                throw e;
            }
            LOG.warn(
                    "Failed to reload the data token of {}, keeping the current one.",
                    identifier,
                    e);
            return current.token;
        }
    }

    private RESTToken load() {
        if (api == null) {
            api = new RESTApi(catalogOptions, false);
        }
        GetTableTokenResponse response = api.loadTableToken(identifier);
        // Layered over the catalog options, the same way RESTTokenFileIO builds its delegate.
        return new RESTToken(
                RESTUtil.merge(catalogOptions.toMap(), response.getToken()),
                response.getExpiresAtMillis());
    }

    long currentTimeMillis() {
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
