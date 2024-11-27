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

import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import java.util.Map;

public class RESTUtil {
    public static Map<String, String> extractPrefixMap(
            Map<String, String> properties, String prefix) {
        Preconditions.checkNotNull(properties, "Invalid properties map: null");
        Map<String, String> result = Maps.newHashMap();
        properties.forEach(
                (key, value) -> {
                    if (key != null && key.startsWith(prefix)) {
                        result.put(key.substring(prefix.length()), value);
                    }
                });

        return result;
    }

    public static Map<String, String> merge(
            Map<String, String> target, Map<String, String> updates) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        target.forEach(
                (key, value) -> {
                    if (!updates.containsKey(key)) {
                        builder.put(key, value);
                    }
                });

        updates.forEach(builder::put);

        return builder.build();
    }
}
