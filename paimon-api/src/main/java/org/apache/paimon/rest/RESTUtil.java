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
import org.apache.paimon.rest.exceptions.RESTException;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.base.Joiner;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.net.URIBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Util for REST. */
public class RESTUtil {

    private static final Joiner.MapJoiner FORM_JOINER = Joiner.on("&").withKeyValueSeparator("=");

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

    /**
     * Merges two string maps with override properties taking precedence over base properties.
     *
     * <p>This method combines two maps of string key-value pairs, where the override map's values
     * will override any conflicting keys from the base map. Only non-null values are included in
     * the final result.
     */
    public static Map<String, String> merge(
            Map<String, String> baseProperties, Map<String, String> overrideProperties) {
        if (overrideProperties == null) {
            overrideProperties = Maps.newHashMap();
        }
        if (baseProperties == null) {
            baseProperties = Maps.newHashMap();
        }

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        // First, add all non-null entries from baseProperties that are not in overrideProperties
        for (Map.Entry<String, String> entry : baseProperties.entrySet()) {
            if (entry.getValue() != null && !overrideProperties.containsKey(entry.getKey())) {
                builder.put(entry.getKey(), entry.getValue());
            }
        }

        // Then, add all non-null entries from overrideProperties (these take precedence)
        for (Map.Entry<String, String> entry : overrideProperties.entrySet()) {
            if (entry.getValue() != null) {
                builder.put(entry.getKey(), entry.getValue());
            }
        }

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

    public static void validatePrefixSqlPattern(String pattern) {
        if (pattern != null && !pattern.isEmpty()) {
            boolean escaped = false;
            boolean inWildcardZone = false;

            for (int i = 0; i < pattern.length(); i++) {
                char c = pattern.charAt(i);

                if (escaped) {
                    escaped = false;
                    continue;
                }

                if (c == '\\') {
                    escaped = true;
                    continue;
                }

                if (c == '%') {
                    inWildcardZone = true;
                } else {
                    if (inWildcardZone) {
                        throw new IllegalArgumentException(
                                "Can only support prefix sql like pattern query now.");
                    }
                }
            }
        }
    }

    public static String encodedBody(Object body) {
        if (body instanceof Map) {
            Map<?, ?> formData = (Map<?, ?>) body;
            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            formData.forEach(
                    (key, value) ->
                            builder.put(
                                    encodeString(String.valueOf(key)),
                                    encodeString(String.valueOf(value))));
            return FORM_JOINER.join(builder.build());
        } else if (body != null) {
            try {
                return RESTApi.toJson(body);
            } catch (JsonProcessingException e) {
                throw new RESTException(e, "Failed to encode request body: %s", body);
            }
        }
        return null;
    }

    public static String extractResponseBodyAsString(ClassicHttpResponse response)
            throws IOException, ParseException {
        if (response.getEntity() == null) {
            return null;
        }

        return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
    }

    public static boolean isSuccessful(ClassicHttpResponse response) {
        int code = response.getCode();
        return code == HttpStatus.SC_OK
                || code == HttpStatus.SC_ACCEPTED
                || code == HttpStatus.SC_NO_CONTENT;
    }

    public static String buildRequestUrl(String url, Map<String, String> queryParams) {
        try {
            if (queryParams != null && !queryParams.isEmpty()) {
                URIBuilder builder = new URIBuilder(url);
                for (Map.Entry<String, String> entry : queryParams.entrySet()) {
                    builder.addParameter(entry.getKey(), entry.getValue());
                }
                url = builder.build().toString();
            }
        } catch (URISyntaxException e) {
            throw new RESTException(e, "build request URL failed.");
        }

        return url;
    }
}
