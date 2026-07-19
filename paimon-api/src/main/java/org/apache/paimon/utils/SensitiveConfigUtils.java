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

package org.apache.paimon.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Redacts sensitive configuration values (passwords, secrets, tokens, access keys) before they are
 * written to logs or exception messages. Only protects Paimon-produced output; third-party logs
 * (Hive, Hadoop, cloud SDKs) must be handled by the runtime logging config.
 */
public class SensitiveConfigUtils {

    /** Placeholder for a fully-masked (short) sensitive value. */
    public static final String REDACTED = "******";

    /** Values at least this long keep their last {@link #TAIL_LEN} chars to aid troubleshooting. */
    private static final int MIN_LEN_FOR_TAIL = 12;

    private static final int TAIL_LEN = 4;

    /**
     * Substrings that mark a configuration key as sensitive. Keys are normalized (lower-cased with
     * separators removed) before matching, so {@code fs.oss.accessKeySecret}, {@code
     * fs.s3a.access.key}, {@code fs.azure.account.key.*}, {@code fs.azure.sas.*} and {@code
     * fs.s3a.encryption.key} are all detected.
     */
    private static final String[] SENSITIVE_KEY_PATTERNS = {
        "password",
        "secret",
        "token",
        "credential",
        "accesskey",
        "accountkey",
        "encryptionkey",
        "authorization",
        "privatekey",
        "apikey",
        "sas"
    };

    /**
     * Keys whose value is fully masked instead of keeping a trailing hint. Every true secret is
     * here; only identifier-like keys ({@code accessKeyId}) keep a tail. This mirrors AWS/Azure,
     * where the access-key id is loggable but the secret / token / SAS is never shown. Note {@code
     * accessKeySecret} normalizes to contain {@code secret}, so it is fully masked while {@code
     * accessKeyId} (only {@code accesskey}) keeps its tail.
     */
    private static final String[] FULL_MASK_KEY_PATTERNS = {
        "password",
        "token",
        "authorization",
        "secret",
        "credential",
        "privatekey",
        "encryptionkey",
        "apikey",
        "accountkey",
        "sas"
    };

    /**
     * Markers that flag free-form text (e.g. a server error message or a signed URL) as possibly
     * carrying a secret. Matched by a plain lower-cased substring scan, so no regex backtracking.
     */
    private static final String[] SENSITIVE_TEXT_MARKERS = {
        "password",
        "secret",
        "token",
        "credential",
        "access-key",
        "accesskey",
        "account-key",
        "accountkey",
        "authorization",
        "private-key",
        "privatekey",
        "api-key",
        "apikey",
        "encryption-key",
        "encryptionkey",
        "signature",
        "sig=",
        "x-amz-"
    };

    private SensitiveConfigUtils() {}

    /** Returns whether the given configuration key is considered sensitive. */
    public static boolean isSensitive(String key) {
        return matchesAny(key, SENSITIVE_KEY_PATTERNS);
    }

    private static boolean matchesAny(String key, String[] patterns) {
        if (key == null || key.isEmpty()) {
            return false;
        }
        String normalized = key.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
        for (String pattern : patterns) {
            if (normalized.contains(pattern)) {
                return true;
            }
        }
        return false;
    }

    /** Returns the value masked if its key is sensitive, otherwise the value unchanged. */
    public static String redactValue(String key, String value) {
        return isSensitive(key) ? maskValue(value, matchesAny(key, FULL_MASK_KEY_PATTERNS)) : value;
    }

    /**
     * Returns a copy of the map with every sensitive value masked. Insertion order is preserved.
     * Safe to pass {@code null}.
     */
    public static Map<String, String> redactMap(Map<String, String> options) {
        if (options == null) {
            return null;
        }
        Map<String, String> redacted = new LinkedHashMap<>(options.size());
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            redacted.put(key, redactValue(key, entry.getValue()));
        }
        return redacted;
    }

    /**
     * Redacts free-form text (e.g. a server {@code ErrorResponse.message}). Arbitrary text cannot
     * be masked per-secret reliably, so if any sensitive marker is present the whole text is
     * replaced. Runs a plain linear substring scan (no regex backtracking / ReDoS) and is safe to
     * call on attacker-controlled input. Prefer {@link #redactMap} when the keys are known.
     */
    public static String redactText(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        String lower = text.toLowerCase(Locale.ROOT);
        for (String marker : SENSITIVE_TEXT_MARKERS) {
            if (lower.contains(marker)) {
                return REDACTED;
            }
        }
        return text;
    }

    /**
     * Strips the query string and user-info from a URI so signed-URL credentials (AWS/GCS
     * signature, Azure SAS {@code sig}, an embedded {@code user:password}) never reach logs or
     * exceptions. Returns {@code scheme://host[:port]/path}; falls back to the substring before
     * {@code '?'} when the URI cannot be parsed.
     */
    public static String sanitizeUri(String uri) {
        if (uri == null || uri.isEmpty()) {
            return uri;
        }
        try {
            URI parsed = new URI(uri);
            StringBuilder sb = new StringBuilder();
            if (parsed.getScheme() != null) {
                sb.append(parsed.getScheme()).append("://");
            }
            if (parsed.getHost() != null) {
                sb.append(parsed.getHost());
                if (parsed.getPort() != -1) {
                    sb.append(':').append(parsed.getPort());
                }
            }
            if (parsed.getRawPath() != null) {
                sb.append(parsed.getRawPath());
            }
            return sb.length() == 0 ? stripQuery(uri) : sb.toString();
        } catch (URISyntaxException e) {
            return stripQuery(uri);
        }
    }

    private static String stripQuery(String uri) {
        int queryStart = uri.indexOf('?');
        return queryStart >= 0 ? uri.substring(0, queryStart) : uri;
    }

    /**
     * Masks a value. Password/token keys are fully masked; other secrets keep their last {@link
     * #TAIL_LEN} chars when long enough, to aid troubleshooting.
     */
    private static String maskValue(String value, boolean fullMask) {
        if (value == null) {
            return null;
        }
        if (!fullMask && value.length() >= MIN_LEN_FOR_TAIL) {
            return "****" + value.substring(value.length() - TAIL_LEN);
        }
        return REDACTED;
    }
}
