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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Resolves and fingerprints algorithm options for the primary-key vector index. */
public final class PrimaryKeyVectorIndexOptions {

    private PrimaryKeyVectorIndexOptions() {}

    public static Options resolve(CoreOptions coreOptions) {
        Options resolved = new Options(coreOptions.toConfiguration().toMap());
        for (Map.Entry<String, String> option : algorithmOptions(coreOptions).entrySet()) {
            resolved.setString(option.getKey(), option.getValue());
        }
        return resolved;
    }

    public static byte[] hash(CoreOptions coreOptions) {
        String canonicalJson = JsonSerdeUtil.toJson(algorithmOptions(coreOptions));
        try {
            return MessageDigest.getInstance("SHA-256")
                    .digest(canonicalJson.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available.", e);
        }
    }

    private static Map<String, String> algorithmOptions(CoreOptions coreOptions) {
        String algorithm = coreOptions.primaryKeyVectorIndexType();
        checkArgument(
                algorithm != null && !algorithm.trim().isEmpty(),
                "pk-vector.index.type must be configured before resolving index options.");
        TreeMap<String, String> options = new TreeMap<>();
        String field = coreOptions.primaryKeyVectorIndexColumn();
        String algorithmPrefix = algorithm + ".";
        String fieldPrefix = field == null ? null : "fields." + field + ".";
        for (Map.Entry<String, String> entry : coreOptions.toConfiguration().toMap().entrySet()) {
            if (entry.getKey().startsWith(algorithmPrefix)
                    || (fieldPrefix != null && entry.getKey().startsWith(fieldPrefix))) {
                options.put(entry.getKey(), entry.getValue());
            }
        }
        String serialized = coreOptions.primaryKeyVectorIndexOptions();
        if (serialized != null && !serialized.trim().isEmpty()) {
            LinkedHashMap<String, String> parsed;
            try {
                parsed = JsonSerdeUtil.parseJsonMap(serialized, String.class);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(
                        "pk-vector.index.options must be a JSON object of option key-value pairs.",
                        e);
            }
            for (Map.Entry<String, String> entry : parsed.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                checkArgument(
                        key != null && !key.trim().isEmpty(),
                        "pk-vector.index.options contains an empty option key.");
                checkArgument(
                        value != null,
                        "pk-vector.index.options value for key %s must not be null.",
                        key);
                String qualifiedKey =
                        key.startsWith(algorithmPrefix) || key.startsWith("fields.")
                                ? key
                                : algorithmPrefix + key;
                String previous = options.put(qualifiedKey, value);
                checkArgument(
                        previous == null || previous.equals(value),
                        "pk-vector.index.options defines conflicting values for %s.",
                        qualifiedKey);
            }
        }
        options.put(algorithmPrefix + "metric", coreOptions.primaryKeyVectorDistanceMetric());
        return options;
    }
}
