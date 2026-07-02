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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.MapStorageLayout;
import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockCompressor;
import org.apache.paimon.compression.BlockDecompressor;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Utility functions for the shared-shredding MAP storage layout.
 *
 * <p>Shared-shredding can be enabled for {@code MAP<STRING, T>} fields. It rewrites a logical MAP
 * field into a physical ROW containing a key-to-column mapping array, a fixed number of value
 * columns, and an overflow MAP for keys that cannot be placed in the fixed columns. This class also
 * serializes and deserializes the per-field shredding metadata stored with the physical data,
 * including optional compression for the field dictionary.
 */
public class MapSharedShreddingUtils {

    private MapSharedShreddingUtils() {}

    public static boolean isShreddingKeyMap(DataType dataType) {
        if (!(dataType instanceof MapType)) {
            return false;
        }
        MapType mapType = (MapType) dataType;
        return mapType.getKeyType().getTypeRoot() == DataTypeRoot.VARCHAR;
    }

    public static List<String> detectShreddingColumns(RowType rowType, CoreOptions options) {
        List<String> fieldNames = new ArrayList<>();
        for (DataField field : rowType.getFields()) {
            if (!isShreddingKeyMap(field.type())) {
                continue;
            }
            if (options.mapStorageLayout(field.name()) == MapStorageLayout.SHARED_SHREDDING) {
                fieldNames.add(field.name());
            }
        }
        return fieldNames;
    }

    public static RowType logicalToPhysicalSchema(
            RowType logicalSchema, Map<String, Integer> fieldToNumColumns) {
        List<DataField> physicalFields = new ArrayList<>();
        for (DataField field : logicalSchema.getFields()) {
            Integer numColumns = fieldToNumColumns.get(field.name());
            if (numColumns == null) {
                physicalFields.add(field);
                continue;
            }

            MapType mapType = (MapType) field.type();
            DataType physicalType =
                    buildPhysicalStructType(mapType.getValueType(), numColumns)
                            .copy(field.type().isNullable());
            physicalFields.add(field.newType(physicalType));
        }
        return new RowType(logicalSchema.isNullable(), physicalFields);
    }

    public static Map<String, Integer> buildColumnToNumColumns(
            List<String> shreddingFieldNames, CoreOptions options) {
        Map<String, Integer> fieldToNumColumns = new HashMap<>();
        for (String fieldName : shreddingFieldNames) {
            fieldToNumColumns.put(fieldName, options.mapSharedShreddingMaxColumns(fieldName));
        }
        return fieldToNumColumns;
    }

    public static void serializeMetadata(
            MapSharedShreddingFieldMeta fieldMeta,
            @Nullable String compression,
            Map<String, String> metadata) {
        metadata.put(
                MapShreddingDefine.STORAGE_LAYOUT,
                MapShreddingDefine.STORAGE_LAYOUT_SHARED_SHREDDING);
        metadata.put(
                MapSharedShreddingDefine.VERSION,
                String.valueOf(MapSharedShreddingDefine.CURRENT_VERSION));

        String fieldDictJson = toJson(new TreeMap<>(fieldMeta.nameToId()));
        metadata.put(
                MapSharedShreddingDefine.FIELD_DICT_ORIGINAL_SIZE,
                String.valueOf(fieldDictJson.getBytes(StandardCharsets.UTF_8).length));
        metadata.put(
                MapSharedShreddingDefine.FIELD_DICT,
                bytesToString(
                        compress(fieldDictJson.getBytes(StandardCharsets.UTF_8), compression)));
        metadata.put(
                MapSharedShreddingDefine.FIELD_COLUMNS,
                toJson(sortedFieldColumns(fieldMeta.fieldToColumns())));
        metadata.put(
                MapSharedShreddingDefine.OVERFLOW_SET,
                toJson(new TreeSet<>(fieldMeta.overflowFieldSet())));
        metadata.put(MapSharedShreddingDefine.NUM_COLUMNS, String.valueOf(fieldMeta.numColumns()));
        metadata.put(
                MapSharedShreddingDefine.MAX_ROW_WIDTH, String.valueOf(fieldMeta.maxRowWidth()));
    }

    public static MapSharedShreddingFieldMeta deserializeMetadata(
            @Nullable Map<String, String> metadata, @Nullable String compression) {
        if (!hasShreddingMetadata(metadata)) {
            throw new IllegalArgumentException(
                    "metadata is null or storage layout is not shared-shredding");
        }

        int version = requiredInt(metadata, MapSharedShreddingDefine.VERSION);
        if (version != MapSharedShreddingDefine.CURRENT_VERSION) {
            throw new IllegalArgumentException(
                    String.format(
                            "unsupported shared-shredding metadata version: %s, expected: %s",
                            version, MapSharedShreddingDefine.CURRENT_VERSION));
        }

        int originalLength =
                requiredInt(metadata, MapSharedShreddingDefine.FIELD_DICT_ORIGINAL_SIZE);
        byte[] fieldDictBytes =
                decompress(
                        stringToBytes(requiredValue(metadata, MapSharedShreddingDefine.FIELD_DICT)),
                        originalLength,
                        compression);
        Map<String, Integer> nameToId =
                fromJson(
                        new String(fieldDictBytes, StandardCharsets.UTF_8),
                        new TypeReference<Map<String, Integer>>() {});
        Map<Integer, List<Integer>> fieldToColumns =
                parseFieldColumns(requiredValue(metadata, MapSharedShreddingDefine.FIELD_COLUMNS));
        Set<Integer> overflowSet =
                fromJson(
                        requiredValue(metadata, MapSharedShreddingDefine.OVERFLOW_SET),
                        new TypeReference<Set<Integer>>() {});

        return new MapSharedShreddingFieldMeta(
                nameToId,
                fieldToColumns,
                overflowSet,
                requiredInt(metadata, MapSharedShreddingDefine.NUM_COLUMNS),
                requiredInt(metadata, MapSharedShreddingDefine.MAX_ROW_WIDTH));
    }

    public static boolean hasShreddingMetadata(@Nullable Map<String, String> metadata) {
        return metadata != null
                && MapShreddingDefine.STORAGE_LAYOUT_SHARED_SHREDDING.equals(
                        metadata.get(MapShreddingDefine.STORAGE_LAYOUT));
    }

    private static RowType buildPhysicalStructType(DataType valueType, int numColumns) {
        RowType.Builder builder = RowType.builder();
        builder.field(MapSharedShreddingDefine.FIELD_MAPPING, new ArrayType(new IntType()));
        for (int i = 0; i < numColumns; i++) {
            builder.field(MapSharedShreddingDefine.physicalColumnName(i), valueType);
        }
        builder.field(MapSharedShreddingDefine.OVERFLOW, new MapType(new IntType(), valueType));
        return builder.build();
    }

    private static Map<Integer, List<Integer>> sortedFieldColumns(
            Map<Integer, List<Integer>> fieldToColumns) {
        Map<Integer, List<Integer>> result = new TreeMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : fieldToColumns.entrySet()) {
            List<Integer> columns = entry.getValue().stream().sorted().collect(Collectors.toList());
            result.put(entry.getKey(), columns);
        }
        return result;
    }

    private static Map<Integer, List<Integer>> parseFieldColumns(String json) {
        Map<String, List<Integer>> parsed =
                fromJson(json, new TypeReference<Map<String, List<Integer>>>() {});
        Map<Integer, List<Integer>> result = new TreeMap<>();
        for (Map.Entry<String, List<Integer>> entry : parsed.entrySet()) {
            result.put(Integer.parseInt(entry.getKey()), entry.getValue());
        }
        return result;
    }

    private static String requiredValue(Map<String, String> metadata, String key) {
        String value = metadata.get(key);
        if (value == null) {
            throw new IllegalArgumentException("missing shredding metadata key: " + key);
        }
        return value;
    }

    private static int requiredInt(Map<String, String> metadata, String key) {
        try {
            return Integer.parseInt(requiredValue(metadata, key));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "malformed shredding metadata value for key: " + key, e);
        }
    }

    private static byte[] compress(byte[] input, @Nullable String compression) {
        if (isNoCompression(compression)) {
            return input;
        }

        BlockCompressionFactory factory =
                BlockCompressionFactory.create(new CompressOptions(compression, 1));
        if (factory == null) {
            return input;
        }
        BlockCompressor compressor = factory.getCompressor();
        byte[] output = new byte[compressor.getMaxCompressedSize(input.length)];
        int actualSize = compressor.compress(input, 0, input.length, output, 0);
        return Arrays.copyOf(output, actualSize);
    }

    private static byte[] decompress(
            byte[] input, int originalLength, @Nullable String compression) {
        if (isNoCompression(compression)) {
            return input;
        }

        BlockCompressionFactory factory =
                BlockCompressionFactory.create(new CompressOptions(compression, 1));
        if (factory == null) {
            return input;
        }
        BlockDecompressor decompressor = factory.getDecompressor();
        byte[] output = new byte[originalLength];
        int actualSize = decompressor.decompress(input, 0, input.length, output, 0);
        return Arrays.copyOf(output, actualSize);
    }

    private static boolean isNoCompression(@Nullable String compression) {
        return compression == null || "none".equalsIgnoreCase(compression);
    }

    private static String bytesToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }

    private static byte[] stringToBytes(String string) {
        return string.getBytes(StandardCharsets.ISO_8859_1);
    }

    private static String toJson(Object object) {
        try {
            return JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize shared-shredding metadata.", e);
        }
    }

    private static <T> T fromJson(String json, TypeReference<T> typeReference) {
        try {
            return JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readValue(json, typeReference);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("malformed shredding metadata", e);
        }
    }
}
