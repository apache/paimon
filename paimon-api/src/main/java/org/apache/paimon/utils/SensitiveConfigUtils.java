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

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
     * fs.s3a.access.key} and {@code fs.s3a.secret.key} are all detected.
     */
    private static final String[] SENSITIVE_KEY_PATTERNS = {
        "password",
        "secret",
        "token",
        "credential",
        "accesskey",
        "authorization",
        "privatekey",
        "apikey"
    };

    /** Matches {@code key:value} / {@code key=value} pairs whose key looks sensitive. */
    private static final Pattern SENSITIVE_TEXT =
            Pattern.compile(
                    "(?i)([\"']?[\\w.-]*"
                            + "(?:secret|token|password|credential|access[._-]?key|authorization"
                            + "|api[._-]?key|private[._-]?key)"
                            + "[\\w.-]*[\"']?\\s*[:=]\\s*)([\"']?)([^\"',&}\\s]+)");

    private SensitiveConfigUtils() {}

    /** Returns whether the given configuration key is considered sensitive. */
    public static boolean isSensitive(String key) {
        if (key == null || key.isEmpty()) {
            return false;
        }
        String normalized = key.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
        for (String pattern : SENSITIVE_KEY_PATTERNS) {
            if (normalized.contains(pattern)) {
                return true;
            }
        }
        return false;
    }

    /** Returns the value masked if its key is sensitive, otherwise the value unchanged. */
    public static String redactValue(String key, String value) {
        return isSensitive(key) ? maskValue(value) : value;
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
            redacted.put(key, isSensitive(key) ? maskValue(entry.getValue()) : entry.getValue());
        }
        return redacted;
    }

    /**
     * Best-effort masking of secret values in free-form text (JSON/form HTTP bodies): masks the
     * value after any sensitive-looking key. Prefer {@link #redactMap} when keys are known.
     */
    public static String redactText(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        Matcher matcher = SENSITIVE_TEXT.matcher(text);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String replacement = matcher.group(1) + matcher.group(2) + maskValue(matcher.group(3));
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /** Masks a value, keeping the last {@link #TAIL_LEN} chars when it is long enough. */
    private static String maskValue(String value) {
        if (value == null) {
            return null;
        }
        if (value.length() >= MIN_LEN_FOR_TAIL) {
            return "****" + value.substring(value.length() - TAIL_LEN);
        }
        return REDACTED;
    }
}
