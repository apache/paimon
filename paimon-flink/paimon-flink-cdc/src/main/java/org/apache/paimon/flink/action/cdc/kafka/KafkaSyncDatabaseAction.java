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

package org.apache.paimon.flink.action.cdc.kafka;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.kafka.formats.DataFormat;
import org.apache.paimon.flink.action.cdc.kafka.formats.RecordParser;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkBuilder;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordSchemaBuilder;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An {@link Action} which synchronize the Multiple topics into one Paimon database.
 *
 * <p>You should specify Kafka source topic in {@code kafkaConfig}. See <a
 * href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/">document
 * of flink-connectors</a> for detailed keys and values.
 *
 * <p>For each Kafka topic's table to be synchronized, if the corresponding Paimon table does not
 * exist, this action will automatically create the table, and its schema will be derived from all
 * specified Kafka topic's tables. If the Paimon table already exists and its schema is different
 * from that parsed from Kafka record, this action will try to preform schema evolution.
 *
 * <p>This action supports a limited number of schema changes. Currently, the framework can not drop
 * columns, so the behaviors of `DROP` will be ignored, `RENAME` will add a new column. Currently
 * supported schema changes includes:
 *
 * <ul>
 *   <li>Adding columns.
 *   <li>Altering column types. More specifically,
 *       <ul>
 *         <li>altering from a string type (char, varchar, text) to another string type with longer
 *             length,
 *         <li>altering from a binary type (binary, varbinary, blob) to another binary type with
 *             longer length,
 *         <li>altering from an integer type (tinyint, smallint, int, bigint) to another integer
 *             type with wider range,
 *         <li>altering from a floating-point type (float, double) to another floating-point type
 *             with wider range,
 *       </ul>
 *       are supported.
 * </ul>
 *
 * <p>To automatically synchronize new table, This action creates a single sink for all Paimon
 * tables to be written. See {@link MultiTablesSinkMode#COMBINED}.
 */
public class KafkaSyncDatabaseAction extends ActionBase {

    private final String database;
    private final Configuration kafkaConfig;

    private Map<String, String> tableConfig = new HashMap<>();
    private String tablePrefix = "";
    private String tableSuffix = "";
    private String includingTables = ".*";
    @Nullable String excludingTables;
    private TypeMapping typeMapping = TypeMapping.defaultMapping();

    public KafkaSyncDatabaseAction(
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> kafkaConfig) {
        super(warehouse, catalogConfig);
        this.database = database;
        this.kafkaConfig = Configuration.fromMap(kafkaConfig);
    }

    public KafkaSyncDatabaseAction withTableConfig(Map<String, String> tableConfig) {
        this.tableConfig = tableConfig;
        return this;
    }

    public KafkaSyncDatabaseAction withTablePrefix(@Nullable String tablePrefix) {
        if (tablePrefix != null) {
            this.tablePrefix = tablePrefix;
        }
        return this;
    }

    public KafkaSyncDatabaseAction withTableSuffix(@Nullable String tableSuffix) {
        if (tableSuffix != null) {
            this.tableSuffix = tableSuffix;
        }
        return this;
    }

    public KafkaSyncDatabaseAction includingTables(@Nullable String includingTables) {
        if (includingTables != null) {
            this.includingTables = includingTables;
        }
        return this;
    }

    public KafkaSyncDatabaseAction excludingTables(@Nullable String excludingTables) {
        this.excludingTables = excludingTables;
        return this;
    }

    public KafkaSyncDatabaseAction withTypeMapping(TypeMapping typeMapping) {
        this.typeMapping = typeMapping;
        return this;
    }

    @Override
    public void build(StreamExecutionEnvironment env) throws Exception {
        boolean caseSensitive = catalog.caseSensitive();

        if (!caseSensitive) {
            validateCaseInsensitive();
        }

        catalog.createDatabase(database, true);

        KafkaSource<String> source = KafkaActionUtils.buildKafkaSource(kafkaConfig);

        DataFormat format = DataFormat.getDataFormat(kafkaConfig);
        RecordParser recordParser =
                format.createParser(caseSensitive, typeMapping, Collections.emptyList());
        RichCdcMultiplexRecordSchemaBuilder schemaBuilder =
                new RichCdcMultiplexRecordSchemaBuilder(tableConfig, caseSensitive);
        Pattern includingPattern = Pattern.compile(includingTables);
        Pattern excludingPattern =
                excludingTables == null ? null : Pattern.compile(excludingTables);
        TableNameConverter tableNameConverter =
                new TableNameConverter(caseSensitive, true, tablePrefix, tableSuffix);
        EventParser.Factory<RichCdcMultiplexRecord> parserFactory =
                () ->
                        new RichCdcMultiplexRecordEventParser(
                                schemaBuilder,
                                includingPattern,
                                excludingPattern,
                                tableNameConverter);

        new FlinkCdcSyncDatabaseSinkBuilder<RichCdcMultiplexRecord>()
                .withInput(
                        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                                .flatMap(recordParser))
                .withParserFactory(parserFactory)
                .withCatalogLoader(catalogLoader())
                .withDatabase(database)
                .withMode(MultiTablesSinkMode.COMBINED)
                .withTableOptions(tableConfig)
                .build();
    }

    private void validateCaseInsensitive() {
        checkArgument(
                database.equals(database.toLowerCase()),
                String.format(
                        "Database name [%s] cannot contain upper case in case-insensitive catalog.",
                        database));
        checkArgument(
                tablePrefix.equals(tablePrefix.toLowerCase()),
                String.format(
                        "Table prefix [%s] cannot contain upper case in case-insensitive catalog.",
                        tablePrefix));
        checkArgument(
                tableSuffix.equals(tableSuffix.toLowerCase()),
                String.format(
                        "Table suffix [%s] cannot contain upper case in case-insensitive catalog.",
                        tableSuffix));
    }

    @VisibleForTesting
    public Map<String, String> tableConfig() {
        return tableConfig;
    }

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        env.execute(String.format("KAFKA-Paimon Database Sync: %s", database));
    }
}
