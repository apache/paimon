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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunction;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.apache.paimon.flink.action.MultiTablesSinkMode.DIVIDED;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Common utils for CDC Action. */
public class CdcActionCommonUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CdcActionCommonUtils.class);

    public static final String KAFKA_CONF = "kafka_conf";
    public static final String MONGODB_CONF = "mongodb_conf";
    public static final String MYSQL_CONF = "mysql_conf";
    public static final String POSTGRES_CONF = "postgres_conf";
    public static final String PULSAR_CONF = "pulsar_conf";
    public static final String TABLE_PREFIX = "table_prefix";
    public static final String TABLE_SUFFIX = "table_suffix";
    public static final String INCLUDING_TABLES = "including_tables";
    public static final String EXCLUDING_TABLES = "excluding_tables";
    public static final String TYPE_MAPPING = "type_mapping";
    public static final String PARTITION_KEYS = "partition_keys";
    public static final String PRIMARY_KEYS = "primary_keys";
    public static final String COMPUTED_COLUMN = "computed_column";
    public static final String METADATA_COLUMN = "metadata_column";

    public static void assertSchemaCompatible(
            TableSchema paimonSchema, List<DataField> sourceTableFields) {
        if (!schemaCompatible(paimonSchema, sourceTableFields)) {
            throw new IllegalArgumentException(
                    "Paimon schema and source table schema are not compatible.\n"
                            + "Paimon fields are: "
                            + paimonSchema.fields()
                            + ".\nSource table fields are: "
                            + sourceTableFields);
        }
    }

    public static boolean schemaCompatible(
            TableSchema paimonSchema, List<DataField> sourceTableFields) {
        for (DataField field : sourceTableFields) {
            int idx = paimonSchema.fieldNames().indexOf(field.name());
            if (idx < 0) {
                LOG.info(
                        "New fields '{}' found in source table, will be synchronized to Paimon table.",
                        field.name());
            } else {
                DataType type = paimonSchema.fields().get(idx).type();
                if (UpdatedDataFieldsProcessFunction.canConvert(type, field.type())
                        != UpdatedDataFieldsProcessFunction.ConvertAction.CONVERT) {
                    LOG.info(
                            "Cannot convert field '{}' from source table type '{}' to Paimon type '{}'.",
                            field.name(),
                            field.type(),
                            type);
                    return false;
                }
            }
        }
        return true;
    }

    public static List<DataField> fieldNameCaseConvert(
            List<DataField> origin, boolean caseSensitive, String tableName) {
        Set<String> existedFields = new HashSet<>();
        Function<String, String> columnDuplicateErrMsg =
                columnDuplicateErrMsg(tableName == null ? "UNKNOWN" : tableName);
        return origin.stream()
                .map(
                        field -> {
                            if (caseSensitive) {
                                return field;
                            }
                            String columnLowerCase = field.name().toLowerCase();
                            checkArgument(
                                    existedFields.add(columnLowerCase),
                                    columnDuplicateErrMsg.apply(field.name()));
                            return field.newName(columnLowerCase);
                        })
                .collect(Collectors.toList());
    }

    public static <T> LinkedHashMap<String, T> mapKeyCaseConvert(
            LinkedHashMap<String, T> origin,
            boolean caseSensitive,
            Function<String, String> duplicateErrMsg) {
        return mapKeyCaseConvert(origin, caseSensitive, duplicateErrMsg, LinkedHashMap::new);
    }

    public static <T> Map<String, T> mapKeyCaseConvert(
            Map<String, T> origin,
            boolean caseSensitive,
            Function<String, String> duplicateErrMsg) {
        return mapKeyCaseConvert(origin, caseSensitive, duplicateErrMsg, HashMap::new);
    }

    private static <T, M extends Map<String, T>> M mapKeyCaseConvert(
            M origin,
            boolean caseSensitive,
            Function<String, String> duplicateErrMsg,
            Supplier<M> mapSupplier) {
        if (caseSensitive) {
            return origin;
        } else {
            M newMap = mapSupplier.get();
            for (Map.Entry<String, T> entry : origin.entrySet()) {
                String key = entry.getKey();
                checkArgument(!newMap.containsKey(key.toLowerCase()), duplicateErrMsg.apply(key));
                newMap.put(key.toLowerCase(), entry.getValue());
            }
            return newMap;
        }
    }

    public static Function<String, String> columnDuplicateErrMsg(String tableName) {
        return column ->
                String.format(
                        "Failed to convert columns of table '%s' to case-insensitive form because duplicate column found: '%s'.",
                        tableName, column);
    }

    public static Function<String, String> recordKeyDuplicateErrMsg(Map<String, String> record) {
        return column ->
                "Failed to convert record map to case-insensitive form because duplicate column found. Original record map is:\n"
                        + record;
    }

    public static List<String> listCaseConvert(List<String> origin, boolean caseSensitive) {
        return caseSensitive
                ? origin
                : origin.stream().map(String::toLowerCase).collect(Collectors.toList());
    }

    public static String columnCaseConvertAndDuplicateCheck(
            String column,
            Set<String> existedFields,
            boolean caseSensitive,
            Function<String, String> columnDuplicateErrMsg) {
        if (caseSensitive) {
            return column;
        }
        String columnLowerCase = column.toLowerCase();
        checkArgument(existedFields.add(columnLowerCase), columnDuplicateErrMsg.apply(column));
        return columnLowerCase;
    }

    public static Schema buildPaimonSchema(
            String tableName,
            List<String> specifiedPartitionKeys,
            List<String> specifiedPrimaryKeys,
            List<ComputedColumn> computedColumns,
            Map<String, String> tableConfig,
            Schema sourceSchema,
            CdcMetadataConverter[] metadataConverters,
            boolean caseSensitive,
            boolean requirePrimaryKeys) {
        Schema.Builder builder = Schema.newBuilder();

        // options
        builder.options(tableConfig);
        builder.options(sourceSchema.options());

        // fields
        Set<String> existedFields = new HashSet<>();
        Function<String, String> columnDuplicateErrMsg = columnDuplicateErrMsg(tableName);

        for (DataField field : sourceSchema.fields()) {
            String fieldName =
                    columnCaseConvertAndDuplicateCheck(
                            field.name(), existedFields, caseSensitive, columnDuplicateErrMsg);
            builder.column(fieldName, field.type(), field.description());
        }

        for (ComputedColumn computedColumn : computedColumns) {
            String computedColumnName =
                    columnCaseConvertAndDuplicateCheck(
                            computedColumn.columnName(),
                            existedFields,
                            caseSensitive,
                            columnDuplicateErrMsg);
            builder.column(computedColumnName, computedColumn.columnType());
        }

        for (CdcMetadataConverter metadataConverter : metadataConverters) {
            String metadataColumnName =
                    columnCaseConvertAndDuplicateCheck(
                            metadataConverter.columnName(),
                            existedFields,
                            caseSensitive,
                            columnDuplicateErrMsg);
            builder.column(metadataColumnName, metadataConverter.dataType());
        }

        // primary keys
        if (!specifiedPrimaryKeys.isEmpty()) {
            Set<String> sourceColumns =
                    sourceSchema.fields().stream().map(DataField::name).collect(Collectors.toSet());
            sourceColumns.addAll(
                    computedColumns.stream()
                            .map(ComputedColumn::columnName)
                            .collect(Collectors.toSet()));
            for (String key : specifiedPrimaryKeys) {
                checkArgument(
                        sourceColumns.contains(key),
                        "Specified primary key '%s' does not exist in source tables or computed columns %s.",
                        key,
                        sourceColumns);
            }
            builder.primaryKey(listCaseConvert(specifiedPrimaryKeys, caseSensitive));
        } else if (!sourceSchema.primaryKeys().isEmpty()) {
            builder.primaryKey(listCaseConvert(sourceSchema.primaryKeys(), caseSensitive));
        } else if (requirePrimaryKeys) {
            throw new IllegalArgumentException(
                    "Primary keys are not specified. "
                            + "Also, can't infer primary keys from source table schemas because "
                            + "source tables have no primary keys or have different primary keys.");
        }

        // partition keys
        if (!specifiedPartitionKeys.isEmpty()) {
            builder.partitionKeys(listCaseConvert(specifiedPartitionKeys, caseSensitive));
        }

        // comment
        builder.comment(sourceSchema.comment());

        return builder.build();
    }

    public static String tableList(
            MultiTablesSinkMode mode,
            String databasePattern,
            String includingTablePattern,
            List<Identifier> monitoredTables,
            List<Identifier> excludedTables) {
        if (mode == DIVIDED) {
            return dividedModeTableList(monitoredTables);
        } else if (mode == COMBINED) {
            return combinedModeTableList(databasePattern, includingTablePattern, excludedTables);
        }

        throw new UnsupportedOperationException("Unknown MultiTablesSinkMode: " + mode);
    }

    private static String dividedModeTableList(List<Identifier> monitoredTables) {
        // In DIVIDED mode, we only concern about existed tables
        return monitoredTables.stream()
                .map(t -> t.getDatabaseName() + "\\." + t.getObjectName())
                .collect(Collectors.joining("|"));
    }

    public static String combinedModeTableList(
            String databasePattern, String includingTablePattern, List<Identifier> excludedTables) {
        // In COMBINED mode, we should consider both existed tables
        // and possible newly created
        // tables, so we should use regular expression to monitor all valid tables and exclude
        // certain invalid tables

        // The table list is built by template:
        // (?!(^db\\.tbl$)|(^...$))((databasePattern)\\.(including_pattern1|...))

        // The excluding pattern ?!(^db\\.tbl$)|(^...$) can exclude tables whose qualified name
        // is exactly equal to 'db.tbl'
        // The including pattern (databasePattern)\\.(including_pattern1|...) can include tables
        // whose qualified name matches one of the patterns

        // a table can be monitored only when its name meets the including pattern and doesn't
        // be excluded by excluding pattern at the same time
        String includingPattern =
                String.format("(%s)\\.(%s)", databasePattern, includingTablePattern);

        if (excludedTables.isEmpty()) {
            return includingPattern;
        }

        String excludingPattern =
                excludedTables.stream()
                        .map(
                                t ->
                                        String.format(
                                                "(^%s$)",
                                                t.getDatabaseName() + "\\." + t.getObjectName()))
                        .collect(Collectors.joining("|"));
        excludingPattern = "?!" + excludingPattern;
        return String.format("(%s)(%s)", excludingPattern, includingPattern);
    }

    public static void checkRequiredOptions(
            Configuration config, String confName, ConfigOption<?>... configOptions) {
        for (ConfigOption<?> configOption : configOptions) {
            checkArgument(
                    config.contains(configOption),
                    "%s [%s] must be specified.",
                    confName,
                    configOption.key());
        }
    }

    public static void checkOneRequiredOption(
            Configuration config, String confName, ConfigOption<?>... configOptions) {
        checkArgument(
                Arrays.stream(configOptions).filter(config::contains).count() == 1,
                "%s must and can only set one of the following options: %s.",
                confName,
                Arrays.stream(configOptions)
                        .map(ConfigOption::key)
                        .collect(Collectors.joining(",")));
    }
}
