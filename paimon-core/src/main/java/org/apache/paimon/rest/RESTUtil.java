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

import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Util for REST. */
public class RESTUtil {

    public static Map<String, String> extractPrefixMap(Options options, String prefix) {
        return extractPrefixMap(options.toMap(), prefix);
    }

    public static Map<String, String> extractPrefixMap(
            Map<String, String> properties, String prefix) {
        Preconditions.checkNotNull(properties, "Invalid properties map: null");
        Map<String, String> result = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey() != null && entry.getKey().startsWith(prefix)) {
                result.put(
                        entry.getKey().substring(prefix.length()), properties.get(entry.getKey()));
            }
        }
        return result;
    }

    public static Map<String, String> merge(
            Map<String, String> targets, Map<String, String> updates) {
        if (targets == null) {
            targets = Maps.newHashMap();
        }
        if (updates == null) {
            updates = Maps.newHashMap();
        }
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : targets.entrySet()) {
            if (!updates.containsKey(entry.getKey())) {
                builder.put(entry.getKey(), entry.getValue());
            }
        }
        updates.forEach(builder::put);

        return builder.build();
    }

    public static String encodeString(String toEncode) {
        Preconditions.checkArgument(toEncode != null, "Invalid string to encode: null");
        try {
            return URLEncoder.encode(toEncode, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new UncheckedIOException(
                    String.format(
                            "Failed to URL encode '%s': UTF-8 encoding is not supported", toEncode),
                    e);
        }
    }

    public static String decodeString(String encoded) {
        Preconditions.checkArgument(encoded != null, "Invalid string to decode: null");
        try {
            return URLDecoder.decode(encoded, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new UncheckedIOException(
                    String.format(
                            "Failed to URL decode '%s': UTF-8 encoding is not supported", encoded),
                    e);
        }
    }
}
