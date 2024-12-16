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

package org.apache.paimon.flink.utils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableSet;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.flink.utils.FlinkDescriptorProperties.COMMENT;
import static org.apache.paimon.flink.utils.FlinkDescriptorProperties.DATA_TYPE;
import static org.apache.paimon.flink.utils.FlinkDescriptorProperties.EXPR;
import static org.apache.paimon.flink.utils.FlinkDescriptorProperties.METADATA;
import static org.apache.paimon.flink.utils.FlinkDescriptorProperties.NAME;
import static org.apache.paimon.flink.utils.FlinkDescriptorProperties.VIRTUAL;
import static org.apache.paimon.flink.utils.FlinkDescriptorProperties.WATERMARK;
import static org.apache.paimon.flink.utils.FlinkDescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.paimon.flink.utils.FlinkDescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.paimon.flink.utils.FlinkDescriptorProperties.WATERMARK_STRATEGY_EXPR;

/**
 * Utilities for ser/deserializing non-physical columns and watermark into/from a map of string
 * properties.
 */
public class FlinkCatalogPropertiesUtil {
    public static final String SCHEMA = "schema";

    /** Serialize non-physical columns of new api. */
    public static Map<String, String> serializeNonPhysicalNewColumns(ResolvedSchema schema) {
        List<Column> nonPhysicalColumns =
                schema.getColumns().stream()
                        .filter(k -> !k.isPhysical())
                        .collect(Collectors.toList());
        Map<String, String> serialized = new HashMap<>();
        List<String> columnNames = schema.getColumnNames();
        for (Column c : nonPhysicalColumns) {
            int index = columnNames.indexOf(c.getName());
            serialized.put(compoundKey(SCHEMA, index, NAME), c.getName());
            serialized.put(
                    compoundKey(SCHEMA, index, DATA_TYPE),
                    c.getDataType().getLogicalType().asSerializableString());
            if (c instanceof Column.ComputedColumn) {
                Column.ComputedColumn computedColumn = (Column.ComputedColumn) c;
                serialized.put(
                        compoundKey(SCHEMA, index, EXPR),
                        computedColumn.getExpression().asSerializableString());
                if (computedColumn.getComment().isPresent()) {
                    serialized.put(
                            compoundKey(SCHEMA, index, COMMENT), computedColumn.getComment().get());
                }
            } else {
                Column.MetadataColumn metadataColumn = (Column.MetadataColumn) c;
                serialized.put(
                        compoundKey(SCHEMA, index, METADATA),
                        metadataColumn.getMetadataKey().orElse(metadataColumn.getName()));
                serialized.put(
                        compoundKey(SCHEMA, index, VIRTUAL),
                        Boolean.toString(metadataColumn.isVirtual()));
                if (metadataColumn.getComment().isPresent()) {
                    serialized.put(
                            compoundKey(SCHEMA, index, COMMENT), metadataColumn.getComment().get());
                }
            }
        }
        return serialized;
    }

    public static Map<String, String> serializeNewWatermarkSpec(
            org.apache.flink.table.catalog.WatermarkSpec watermarkSpec) {
        Map<String, String> serializedWatermarkSpec = new HashMap<>();
        String watermarkPrefix = compoundKey(SCHEMA, WATERMARK, 0);
        serializedWatermarkSpec.put(
                compoundKey(watermarkPrefix, WATERMARK_ROWTIME),
                watermarkSpec.getRowtimeAttribute());
        serializedWatermarkSpec.put(
                compoundKey(watermarkPrefix, WATERMARK_STRATEGY_EXPR),
                watermarkSpec.getWatermarkExpression().asSerializableString());
        serializedWatermarkSpec.put(
                compoundKey(watermarkPrefix, WATERMARK_STRATEGY_DATA_TYPE),
                watermarkSpec
                        .getWatermarkExpression()
                        .getOutputDataType()
                        .getLogicalType()
                        .asSerializableString());

        return serializedWatermarkSpec;
    }

    private static final Pattern SCHEMA_COLUMN_NAME_SUFFIX = Pattern.compile("\\d+\\." + NAME);
    private static final Pattern SCHEMA_COLUMN_METADATA_SUFFIX =
            Pattern.compile("\\d+\\." + METADATA);
    private static final Pattern SCHEMA_COLUMN_EXPR_SUFFIX = Pattern.compile("\\d+\\." + EXPR);
    private static final Pattern SCHEMA_COLUMN_DATATYPE_SUFFIX =
            Pattern.compile("\\d+\\." + DATA_TYPE);
    private static final Pattern SCHEMA_COLUMN_VIRTUAL_SUFFIX =
            Pattern.compile("\\d+\\." + VIRTUAL);
    private static final Set<Pattern> NON_PHYSICAL_KEY_PATTERNS =
            ImmutableSet.of(
                    SCHEMA_COLUMN_NAME_SUFFIX,
                    SCHEMA_COLUMN_METADATA_SUFFIX,
                    SCHEMA_COLUMN_EXPR_SUFFIX,
                    SCHEMA_COLUMN_DATATYPE_SUFFIX,
                    SCHEMA_COLUMN_VIRTUAL_SUFFIX);

    public static int nonPhysicalColumnsCount(
            Map<String, String> tableOptions, List<String> physicalColumns) {
        int count = 0;
        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            if (isColumnNameKey(entry.getKey()) && !physicalColumns.contains(entry.getValue())) {
                count++;
            }
        }

        return count;
    }

    public static boolean isNonPhysicalColumnKey(String key) {
        if (!key.startsWith(SCHEMA)) {
            return false;
        }
        String suffix = key.substring(SCHEMA.length() + 1);
        for (Pattern pattern : NON_PHYSICAL_KEY_PATTERNS) {
            if (pattern.matcher(suffix).matches()) {
                return true;
            }
        }
        return false;
    }

    public static Map<String, Integer> nonPhysicalColumns(
            Map<String, String> tableOptions, List<String> physicalColumns) {
        Map<String, Integer> nonPhysicalColumnIndex = new HashMap<>();
        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            if (isColumnNameKey(entry.getKey()) && !physicalColumns.contains(entry.getValue())) {
                String key = entry.getKey();
                int index =
                        Integer.parseInt(
                                key.substring(
                                        SCHEMA.length() + 1,
                                        key.indexOf(".", SCHEMA.length() + 1)));
                nonPhysicalColumnIndex.put(entry.getValue(), index);
            }
        }
        return nonPhysicalColumnIndex;
    }

    private static boolean isColumnNameKey(String key) {
        return key.startsWith(SCHEMA)
                && SCHEMA_COLUMN_NAME_SUFFIX.matcher(key.substring(SCHEMA.length() + 1)).matches();
    }

    public static void deserializeNonPhysicalColumn(
            Map<String, String> options, int index, Schema.Builder builder) {
        String nameKey = compoundKey(SCHEMA, index, NAME);
        String dataTypeKey = compoundKey(SCHEMA, index, DATA_TYPE);
        String exprKey = compoundKey(SCHEMA, index, EXPR);
        String metadataKey = compoundKey(SCHEMA, index, METADATA);
        String virtualKey = compoundKey(SCHEMA, index, VIRTUAL);

        String name = options.get(nameKey);

        if (options.containsKey(exprKey)) {
            final String expr = options.get(exprKey);
            builder.columnByExpression(name, expr);
        } else if (options.containsKey(metadataKey)) {
            String metadataAlias = options.get(metadataKey);
            boolean isVirtual = Boolean.parseBoolean(options.get(virtualKey));
            DataType dataType =
                    TypeConversions.fromLogicalToDataType(
                            LogicalTypeParser.parse(
                                    options.get(dataTypeKey),
                                    Thread.currentThread().getContextClassLoader()));
            if (metadataAlias.equals(name)) {
                builder.columnByMetadata(name, dataType, isVirtual);
            } else {
                builder.columnByMetadata(name, dataType, metadataAlias, isVirtual);
            }
        } else {
            throw new RuntimeException(
                    String.format(
                            "Failed to build non-physical column. Current index is %s, options are %s",
                            index, options));
        }
    }

    public static void deserializeWatermarkSpec(
            Map<String, String> options, Schema.Builder builder) {
        String watermarkPrefixKey = compoundKey(SCHEMA, WATERMARK);

        String rowtimeKey = compoundKey(watermarkPrefixKey, 0, WATERMARK_ROWTIME);
        String exprKey = compoundKey(watermarkPrefixKey, 0, WATERMARK_STRATEGY_EXPR);

        String rowtimeAttribute = options.get(rowtimeKey);
        String watermarkExpressionString = options.get(exprKey);

        builder.watermark(rowtimeAttribute, watermarkExpressionString);
    }

    public static String compoundKey(Object... components) {
        return Stream.of(components).map(Object::toString).collect(Collectors.joining("."));
    }
}
