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

package org.apache.paimon.index.pk;

import org.apache.paimon.options.Options;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

/** Resolved definition of one source-backed primary-key index. */
public class PrimaryKeyIndexDefinition {

    /** Built-in primary-key index families. */
    public enum Family {
        VECTOR,
        BTREE,
        BITMAP,
        FULL_TEXT
    }

    private final String column;
    private final int fieldId;
    private final String indexType;
    private final Options options;
    private final Family family;
    private final int compactionLevelFanout;
    private final double compactionStaleRatioThreshold;
    private final String definitionFingerprint;

    public PrimaryKeyIndexDefinition(
            String column,
            int fieldId,
            String indexType,
            Options options,
            Family family,
            int compactionLevelFanout,
            double compactionStaleRatioThreshold) {
        this.column = column;
        this.fieldId = fieldId;
        this.indexType = indexType;
        this.options = options;
        this.family = family;
        this.compactionLevelFanout = compactionLevelFanout;
        this.compactionStaleRatioThreshold = compactionStaleRatioThreshold;
        this.definitionFingerprint = definitionFingerprint(fieldId, indexType, options, family);
    }

    public String column() {
        return column;
    }

    public int fieldId() {
        return fieldId;
    }

    public String indexType() {
        return indexType;
    }

    public Options options() {
        return options;
    }

    public Family family() {
        return family;
    }

    public int compactionLevelFanout() {
        return compactionLevelFanout;
    }

    public double compactionStaleRatioThreshold() {
        return compactionStaleRatioThreshold;
    }

    public String definitionFingerprint() {
        return definitionFingerprint;
    }

    private static String definitionFingerprint(
            int fieldId, String indexType, Options options, Family family) {
        StringBuilder definition = new StringBuilder();
        append(definition, family.name());
        append(definition, Integer.toString(fieldId));
        append(definition, indexType);
        for (Map.Entry<String, String> entry : new TreeMap<>(options.toMap()).entrySet()) {
            append(definition, entry.getKey());
            append(definition, entry.getValue());
        }
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(definition.toString().getBytes(StandardCharsets.UTF_8));
            char[] hex = new char[digest.length * 2];
            char[] digits = "0123456789abcdef".toCharArray();
            for (int i = 0; i < digest.length; i++) {
                int value = digest[i] & 0xff;
                hex[i * 2] = digits[value >>> 4];
                hex[i * 2 + 1] = digits[value & 0x0f];
            }
            return new String(hex);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 is not available.", e);
        }
    }

    private static void append(StringBuilder target, String value) {
        target.append(value.length()).append(':').append(value);
    }
}
