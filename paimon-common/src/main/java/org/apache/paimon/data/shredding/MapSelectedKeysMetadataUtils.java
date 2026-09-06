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

package org.apache.paimon.data.shredding;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Utils for marking a temporary read {@link RowType} field as selected-key MAP output. Uses
 * description field in DataField to encode selected-key metadata.
 *
 * <p>Example: __PAIMON_MAP_SELECTED_KEYS:key1;key2
 */
public class MapSelectedKeysMetadataUtils {

    public static final String METADATA_KEY = "__PAIMON_MAP_SELECTED_KEYS:";
    public static final String KEY_DELIMITER = ";";

    private MapSelectedKeysMetadataUtils() {}

    public static String buildMapSelectedKeysMetadata(List<String> keys) {
        List<String> normalizedKeys = normalizeKeys(keys);
        return METADATA_KEY + String.join(KEY_DELIMITER, normalizedKeys);
    }

    public static boolean isMapSelectedKeysMetadata(@Nullable String description) {
        return description != null && description.startsWith(METADATA_KEY);
    }

    public static boolean isMapSelectedKeysField(DataField field) {
        return isMapSelectedKeysMetadata(field.description()) && field.type() instanceof RowType;
    }

    public static DataField withSelectedKeys(DataField field, DataType rowType, List<String> keys) {
        checkArgument(rowType instanceof RowType, "Selected-key MAP read type must be ROW.");
        return field.newType(rowType).newDescription(buildMapSelectedKeysMetadata(keys));
    }

    public static List<String> selectedKeys(String description) {
        checkArgument(
                isMapSelectedKeysMetadata(description),
                "Invalid selected-key MAP metadata: %s",
                description);
        String encoded = description.substring(METADATA_KEY.length());

        String[] parts = encoded.split(KEY_DELIMITER, -1);
        List<String> keys = new ArrayList<>(parts.length);
        for (String part : parts) {
            keys.add(part);
        }
        return keys;
    }

    private static List<String> normalizeKeys(List<String> keys) {
        checkNotNull(keys, "Selected keys must not be null.");
        checkArgument(!keys.isEmpty(), "Selected keys must not be empty.");

        Set<String> seenKeys = new LinkedHashSet<>();
        for (String key : keys) {
            checkNotNull(key, "Selected key must not be null.");
            checkArgument(
                    !key.contains(KEY_DELIMITER),
                    "Selected key must not contain '%s': %s",
                    KEY_DELIMITER,
                    key);
            checkArgument(
                    !key.startsWith(METADATA_KEY),
                    "Selected key must not start with metadata prefix: %s",
                    key);
            checkArgument(!seenKeys.contains(key), "Selected key must not be duplicated: %s", key);
            seenKeys.add(key);
        }
        return new ArrayList<>(seenKeys);
    }
}
