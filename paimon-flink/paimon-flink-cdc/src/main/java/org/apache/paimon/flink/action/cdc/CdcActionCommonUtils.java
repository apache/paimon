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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.apache.paimon.flink.action.MultiTablesSinkMode.DIVIDED;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.StringUtils.toLowerCaseIfNeed;

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
    public static final String TABLE_PREFIX_DB = "table_prefix_db";
    public static final String TABLE_SUFFIX_DB = "table_suffix_db";
    public static final String TABLE_MAPPING = "table_mapping";
    public static final String INCLUDING_TABLES = "including_tables";
    public static final String EXCLUDING_TABLES = "excluding_tables";
    public static final String INCLUDING_DBS = "including_dbs";
    public static final String EXCLUDING_DBS = "excluding_dbs";
    public static final String TYPE_MAPPING = "type_mapping";
    public static final String PARTITION_KEYS = "partition_keys";
    public static final String PRIMARY_KEYS = "primary_keys";
    public static final String COMPUTED_COLUMN = "computed_column";
    public static final String METADATA_COLUMN = "metadata_column";
    public static final String MULTIPLE_TABLE_PARTITION_KEYS = "multiple_table_partition_keys";

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
                LOG.info("Cannot find field '{}' in Paimon table.", field.name());
                return false;
            }
            DataType type = paimonSchema.fields().get(idx).type();
            if (UpdatedDataFieldsProcessFunction.canConvert(field.type(), type)
                    != UpdatedDataFieldsProcessFunction.ConvertAction.CONVERT) {
                LOG.info(
                        "Cannot convert field '{}' from source table type '{}' to Paimon type '{}'.",
                        field.name(),
                        field.type(),
                        type);
                return false;
            }
        }
        return true;
    }

    public static List<String> listCaseConvert(List<String> origin, boolean caseSensitive) {
        return caseSensitive
                ? origin
                : origin.stream().map(String::toLowerCase).collect(Collectors.toList());
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
            boolean strictlyCheckSpecified,
            boolean requirePrimaryKeys) {
        Schema.Builder builder = Schema.newBuilder();

        // options
        builder.options(tableConfig);
        builder.options(sourceSchema.options());

        // fields
        List<String> allFieldNames = new ArrayList<>();

        for (DataField field : sourceSchema.fields()) {
            String fieldName = toLowerCaseIfNeed(field.name(), caseSensitive);
            allFieldNames.add(fieldName);
            builder.column(fieldName, field.type(), field.description());
        }

        for (ComputedColumn computedColumn : computedColumns) {
            String computedColumnName =
                    toLowerCaseIfNeed(computedColumn.columnName(), caseSensitive);
            allFieldNames.add(computedColumnName);
            builder.column(computedColumnName, computedColumn.columnType());
        }

        for (CdcMetadataConverter metadataConverter : metadataConverters) {
            String metadataColumnName =
                    toLowerCaseIfNeed(metadataConverter.columnName(), caseSensitive);
            allFieldNames.add(metadataColumnName);
            builder.column(metadataColumnName, metadataConverter.dataType());
        }

        checkDuplicateFields(tableName, allFieldNames);

        // primary keys
        specifiedPrimaryKeys = listCaseConvert(specifiedPrimaryKeys, caseSensitive);
        List<String> sourceSchemaPrimaryKeys =
                listCaseConvert(sourceSchema.primaryKeys(), caseSensitive);
        setPrimaryKeys(
                tableName,
                builder,
                specifiedPrimaryKeys,
                sourceSchemaPrimaryKeys,
                allFieldNames,
                strictlyCheckSpecified,
                requirePrimaryKeys);

        // partition keys
        specifiedPartitionKeys = listCaseConvert(specifiedPartitionKeys, caseSensitive);
        setPartitionKeys(
                tableName, builder, specifiedPartitionKeys, allFieldNames, strictlyCheckSpecified);

        // comment
        builder.comment(sourceSchema.comment());

        return builder.build();
    }

    private static void setPrimaryKeys(
            String tableName,
            Schema.Builder builder,
            List<String> specifiedPrimaryKeys,
            List<String> sourceSchemaPrimaryKeys,
            List<String> allFieldNames,
            boolean strictlyCheckSpecified,
            boolean requirePrimaryKeys) {
        if (!specifiedPrimaryKeys.isEmpty()) {
            if (allFieldNames.containsAll(specifiedPrimaryKeys)) {
                builder.primaryKey(specifiedPrimaryKeys);
                return;
            }

            String message =
                    String.format(
                            "For sink table %s, not all specified primary keys '%s' exist in source tables or computed columns '%s'.",
                            tableName, specifiedPrimaryKeys, allFieldNames);
            if (strictlyCheckSpecified) {
                throw new IllegalArgumentException(message);
            } else {
                LOG.info(
                        "{} In this case at database-sync, we will set primary keys from source tables if exist, otherwise, primary keys are not set.",
                        message);
            }
        }

        if (!sourceSchemaPrimaryKeys.isEmpty()) {
            builder.primaryKey(sourceSchemaPrimaryKeys);
            return;
        }

        if (requirePrimaryKeys) {
            throw new IllegalArgumentException(
                    "Failed to set specified primary keys for sink table "
                            + tableName
                            + ". Also, can't infer primary keys from source table schemas because "
                            + "source tables have no primary keys or have different primary keys.");
        }
    }

    private static void setPartitionKeys(
            String tableName,
            Schema.Builder builder,
            List<String> specifiedPartitionKeys,
            List<String> allFieldNames,
            boolean strictlyCheckSpecified) {
        if (!specifiedPartitionKeys.isEmpty()) {
            if (allFieldNames.containsAll(specifiedPartitionKeys)) {
                builder.partitionKeys(specifiedPartitionKeys);
                return;
            }

            String message =
                    String.format(
                            "For sink table %s, not all specified partition keys '%s' exist in source tables or computed columns '%s'.",
                            tableName, specifiedPartitionKeys, allFieldNames);
            if (strictlyCheckSpecified) {
                throw new IllegalArgumentException(message);
            } else {
                LOG.info("{} In this case at database-sync, partition keys are not set.", message);
            }
        }
    }

    public static void checkDuplicateFields(String tableName, List<String> fieldNames) {
        List<String> duplicates =
                fieldNames.stream()
                        .filter(name -> Collections.frequency(fieldNames, name) > 1)
                        .collect(Collectors.toList());
        checkState(
                duplicates.isEmpty(),
                "Table %s contains duplicate columns: %s.\n"
                        + "Possible causes are: "
                        + "1. computed columns or metadata columns contain duplicate fields; "
                        + "2. the catalog is case-insensitive and the table columns duplicate after they are all converted to lower-case.",
                tableName,
                duplicates);
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
