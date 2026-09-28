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

    // After a failed refresh, keep using the current token this long before trying again.
    static final long RETRY_INTERVAL_MILLIS = 10_000L;

    // Shifts the clock of refreshers that providers create from options, for tests only.
    @VisibleForTesting static volatile long clockOffsetMillis;

    private final Options catalogOptions;
    private final Identifier identifier;

    @Nullable private RESTApi api;
    @Nullable private volatile RESTToken token;
    private long nextAttemptMillis;

    RESTTokenRefresher(
            Options catalogOptions,
            Identifier identifier,
            @Nullable RESTApi api,
            @Nullable RESTToken token) {
        this.catalogOptions = catalogOptions;
        this.identifier = identifier;
        this.api = api;
        this.token = token;
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

    /** Returns the current token, loading a new one when it is about to expire. */
    public RESTToken token() {
        RESTToken current = token;
        if (current != null && !expiresSoon(current)) {
            return current;
        }
        synchronized (this) {
            current = token;
            if (current != null && !expiresSoon(current)) {
                return current;
            }
            long now = currentTimeMillis();
            boolean usable = current != null && now < current.expireAtMillis();
            if (usable && now < nextAttemptMillis) {
                return current;
            }
            try {
                RESTToken loaded = load();
                token = loaded;
                return loaded;
            } catch (RuntimeException e) {
                if (!usable) {
                    throw e;
                }
                nextAttemptMillis = now + RETRY_INTERVAL_MILLIS;
                LOG.warn(
                        "Failed to refresh the data token of {}, keeping the current one.",
                        identifier,
                        e);
                return current;
            }
        }
    }

    private boolean expiresSoon(RESTToken token) {
        return token.expireAtMillis() - currentTimeMillis() < TOKEN_EXPIRATION_SAFE_TIME_MILLIS;
    }

    private RESTToken load() {
        if (api == null) {
            api = new RESTApi(catalogOptions, false);
        }
        GetTableTokenResponse response = api.loadTableToken(identifier);
        return new RESTToken(response.getToken(), response.getExpiresAtMillis());
    }

    long currentTimeMillis() {
        return System.currentTimeMillis() + clockOffsetMillis;
    }
}
