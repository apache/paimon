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

package org.apache.paimon.format.parquet;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/** Utilities shared by map shredding writer and reader. */
public class MapShreddingUtils {

    public static final String DYNAMIC_VALUE_PREFIX = "value_";
    public static final String DYNAMIC_COLUMN_NAME = "dynamic_column";
    public static final String FOOTER_METADATA_PREFIX = "parquet.meta.dynamic.column.map.keys.of.";

    private MapShreddingUtils() {}

    public static boolean isMapShreddingEnabled(Options options) {
        String configured = options.get(CoreOptions.MAP_SHREDDING_COLUMNS);
        return configured != null && !configured.trim().isEmpty();
    }

    public static List<String> parseColumnPaths(@Nullable String configuredColumns) {
        if (configuredColumns == null || configuredColumns.trim().isEmpty()) {
            return Collections.emptyList();
        }

        return Arrays.stream(configuredColumns.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .distinct()
                .collect(Collectors.toList());
    }

    public static List<ResolvedMapShreddingColumn> resolveColumns(
            RowType rowType, List<String> paths) {
        List<ResolvedMapShreddingColumn> resolved = new ArrayList<>(paths.size());
        for (String path : paths) {
            resolved.add(resolveColumn(rowType, path));
        }
        return resolved;
    }

    public static ResolvedMapShreddingColumn resolveColumn(RowType rowType, String path) {
        String[] segments = path.split("\\.");
        if (segments.length == 0) {
            throw new IllegalArgumentException("Invalid map shredding column path: " + path);
        }

        RowType current = rowType;
        int[] positions = new int[segments.length];
        int[] nestedRowFieldCounts = new int[Math.max(segments.length - 1, 0)];
        for (int i = 0; i < segments.length; i++) {
            String segment = segments[i];
            if (!current.containsField(segment)) {
                throw new IllegalArgumentException(
                        String.format(
                                Locale.ROOT,
                                "Cannot resolve map shredding column '%s' from row type '%s'.",
                                path,
                                rowType));
            }

            positions[i] = current.getFieldIndex(segment);
            DataField field = current.getField(segment);
            DataType fieldType = field.type();
            if (i < segments.length - 1) {
                if (!(fieldType instanceof RowType)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    Locale.ROOT,
                                    "Map shredding column '%s' expects ROW on intermediate path '%s' but found '%s'.",
                                    path,
                                    segment,
                                    fieldType));
                }
                nestedRowFieldCounts[i] = ((RowType) fieldType).getFieldCount();
                current = (RowType) fieldType;
                continue;
            }

            if (!(fieldType instanceof MapType)) {
                throw new IllegalArgumentException(
                        String.format(
                                Locale.ROOT,
                                "Map shredding column '%s' must be MAP<STRING, T> but found '%s'.",
                                path,
                                fieldType));
            }

            MapType mapType = (MapType) fieldType;
            if (!isStringType(mapType.getKeyType())) {
                throw new IllegalArgumentException(
                        String.format(
                                Locale.ROOT,
                                "Map shredding column '%s' must use STRING key type but found '%s'.",
                                path,
                                mapType.getKeyType()));
            }
            return new ResolvedMapShreddingColumn(
                    path, segments, positions, nestedRowFieldCounts, mapType);
        }
        throw new IllegalArgumentException("Invalid map shredding column path: " + path);
    }

    public static boolean isMapShreddingPath(Map<String, List<String>> dynamicKeys, String path) {
        List<String> keys = dynamicKeys.get(path);
        return keys != null && !keys.isEmpty();
    }

    public static String sidecarColumnName(String mapColumn, int keyOrdinal) {
        return DYNAMIC_COLUMN_NAME
                + "_"
                + sanitize(mapColumn)
                + "_"
                + DYNAMIC_VALUE_PREFIX
                + keyOrdinal;
    }

    public static String path(String parentPath, String fieldName) {
        return parentPath == null || parentPath.isEmpty()
                ? fieldName
                : parentPath + "." + fieldName;
    }

    public static Map<String, String> toFooterMetadata(Map<String, List<String>> dynamicKeys) {
        Map<String, String> metadata = new LinkedHashMap<>();
        dynamicKeys.forEach(
                (path, keys) -> {
                    if (!keys.isEmpty()) {
                        metadata.put(FOOTER_METADATA_PREFIX + path, String.join(",", keys));
                    }
                });
        return metadata;
    }

    public static Map<String, List<String>> fromFooterMetadata(Map<String, String> keyValueMeta) {
        Map<String, List<String>> result = new LinkedHashMap<>();
        keyValueMeta.forEach(
                (key, value) -> {
                    if (key.startsWith(FOOTER_METADATA_PREFIX)) {
                        String path = key.substring(FOOTER_METADATA_PREFIX.length());
                        result.put(path, parseColumnPaths(value));
                    }
                });
        return result;
    }

    @Nullable
    public static InternalMap extractMap(InternalRow row, ResolvedMapShreddingColumn column) {
        InternalRow current = row;
        for (int i = 0; i < column.fieldPositions.length - 1; i++) {
            int fieldPos = column.fieldPositions[i];
            if (current.isNullAt(fieldPos)) {
                return null;
            }
            current = current.getRow(fieldPos, column.nestedRowFieldCounts[i]);
        }
        int mapPos = column.fieldPositions[column.fieldPositions.length - 1];
        if (current.isNullAt(mapPos)) {
            return null;
        }
        return current.getMap(mapPos);
    }

    private static String sanitize(String value) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
                builder.append(c);
            } else {
                builder.append('_');
            }
        }
        return builder.toString();
    }

    private static boolean isStringType(DataType dataType) {
        DataTypeRoot root = dataType.getTypeRoot();
        return root == DataTypeRoot.VARCHAR || root == DataTypeRoot.CHAR;
    }

    /** Resolved map shredding logical column path. */
    public static class ResolvedMapShreddingColumn {

        private final String path;
        private final String[] fieldNames;
        private final int[] fieldPositions;
        private final int[] nestedRowFieldCounts;
        private final MapType mapType;

        public ResolvedMapShreddingColumn(
                String path,
                String[] fieldNames,
                int[] fieldPositions,
                int[] nestedRowFieldCounts,
                MapType mapType) {
            this.path = path;
            this.fieldNames = fieldNames;
            this.fieldPositions = fieldPositions;
            this.nestedRowFieldCounts = nestedRowFieldCounts;
            this.mapType = mapType;
        }

        public String path() {
            return path;
        }

        public String[] fieldNames() {
            return fieldNames;
        }

        public int[] fieldPositions() {
            return fieldPositions;
        }

        public int[] nestedRowFieldCounts() {
            return nestedRowFieldCounts;
        }

        public MapType mapType() {
            return mapType;
        }

        public DataType valueType() {
            return mapType.getValueType();
        }
    }
}
