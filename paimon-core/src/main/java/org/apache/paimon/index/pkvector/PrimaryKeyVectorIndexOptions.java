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
import org.apache.paimon.utils.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Resolves and fingerprints algorithm options for the primary-key vector index. */
public final class PrimaryKeyVectorIndexOptions {

    private PrimaryKeyVectorIndexOptions() {}

    public static Options resolve(CoreOptions coreOptions) {
        return resolve(coreOptions, singleColumn(coreOptions));
    }

    public static Options resolve(CoreOptions coreOptions, String field) {
        Options resolved = new Options(coreOptions.toConfiguration().toMap());
        for (Map.Entry<String, String> option : algorithmOptions(coreOptions, field).entrySet()) {
            resolved.setString(option.getKey(), option.getValue());
        }
        return resolved;
    }

    public static byte[] hash(CoreOptions coreOptions) {
        return hash(coreOptions, singleColumn(coreOptions));
    }

    public static byte[] hash(CoreOptions coreOptions, String field) {
        return sha256(JsonSerdeUtil.toJson(fingerprintOptions(coreOptions, field)));
    }

    public static String definitionId(
            int vectorFieldId, String vectorTypeFingerprint, CoreOptions coreOptions) {
        return definitionId(
                vectorFieldId, vectorTypeFingerprint, coreOptions, singleColumn(coreOptions));
    }

    public static String definitionId(
            int vectorFieldId,
            String vectorTypeFingerprint,
            CoreOptions coreOptions,
            String field) {
        checkArgument(
                vectorTypeFingerprint != null && !vectorTypeFingerprint.trim().isEmpty(),
                "Vector type fingerprint must not be empty.");
        TreeMap<String, String> definition = new TreeMap<>();
        definition.put("field-id", Integer.toString(vectorFieldId));
        definition.put("field-type", vectorTypeFingerprint);
        definition.putAll(fingerprintOptions(coreOptions, field));
        return StringUtils.byteToHexString(sha256(JsonSerdeUtil.toJson(definition)));
    }

    public static String singleColumn(CoreOptions coreOptions) {
        List<String> columns = coreOptions.primaryKeyVectorIndexColumns();
        checkArgument(
                columns.size() == 1,
                "pk-vector.index.columns must contain exactly one column in the first release, but is %s.",
                columns);
        return columns.get(0);
    }

    private static byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available.", e);
        }
    }

    private static Map<String, String> algorithmOptions(CoreOptions coreOptions, String field) {
        String indexTypeKey = "fields." + field + ".pk-vector.index.type";
        String indexOptionsKey = "fields." + field + ".pk-vector.index.options";
        String algorithm = coreOptions.primaryKeyVectorIndexType(field);
        checkArgument(
                algorithm != null && !algorithm.trim().isEmpty(),
                "%s must be configured before resolving index options.",
                indexTypeKey);
        TreeMap<String, String> options = new TreeMap<>();
        String algorithmPrefix = algorithm + ".";
        String fieldPrefix = "fields." + field + ".";
        for (Map.Entry<String, String> entry : coreOptions.toConfiguration().toMap().entrySet()) {
            if (entry.getKey().startsWith(algorithmPrefix)
                    || (entry.getKey().startsWith(fieldPrefix)
                            && !entry.getKey().startsWith(fieldPrefix + "pk-vector."))) {
                options.put(entry.getKey(), entry.getValue());
            }
        }
        String serialized = coreOptions.primaryKeyVectorIndexOptions(field);
        if (serialized != null && !serialized.trim().isEmpty()) {
            LinkedHashMap<String, String> parsed;
            try {
                parsed = JsonSerdeUtil.parseJsonMap(serialized, String.class);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(
                        indexOptionsKey + " must be a JSON object of option key-value pairs.", e);
            }
            for (Map.Entry<String, String> entry : parsed.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                checkArgument(
                        key != null && !key.trim().isEmpty(),
                        "%s contains an empty option key.",
                        indexOptionsKey);
                checkArgument(
                        value != null,
                        "%s value for key %s must not be null.",
                        indexOptionsKey,
                        key);
                String qualifiedKey =
                        key.startsWith(algorithmPrefix) || key.startsWith("fields.")
                                ? key
                                : algorithmPrefix + key;
                String previous = options.put(qualifiedKey, value);
                checkArgument(
                        previous == null || previous.equals(value),
                        "%s defines conflicting values for %s.",
                        indexOptionsKey,
                        qualifiedKey);
            }
        }
        options.put(algorithmPrefix + "metric", coreOptions.primaryKeyVectorDistanceMetric(field));
        return options;
    }

    private static Map<String, String> fingerprintOptions(CoreOptions coreOptions, String field) {
        String fieldPrefix = "fields." + field + ".";
        TreeMap<String, String> fingerprint = new TreeMap<>();
        Map<String, String> options = algorithmOptions(coreOptions, field);
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (!entry.getKey().startsWith(fieldPrefix)) {
                fingerprint.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (entry.getKey().startsWith(fieldPrefix)) {
                fingerprint.put(entry.getKey().substring(fieldPrefix.length()), entry.getValue());
            }
        }
        return fingerprint;
    }
}
