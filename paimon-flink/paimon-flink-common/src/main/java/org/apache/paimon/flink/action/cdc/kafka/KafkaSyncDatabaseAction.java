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

import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.cdc.DatabaseSyncMode;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.kafka.canal.CanalRecordParser;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkBuilder;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordSchemaBuilder;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;

import javax.annotation.Nullable;

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
 * tables to be written. See {@link DatabaseSyncMode#COMBINED}.
 */
public class KafkaSyncDatabaseAction extends ActionBase {

    private final Configuration kafkaConfig;
    private final String database;
    private final String tablePrefix;
    private final String tableSuffix;
    @Nullable private final Pattern includingPattern;
    @Nullable private final Pattern excludingPattern;
    private final Map<String, String> tableConfig;

    public KafkaSyncDatabaseAction(
            Map<String, String> kafkaConfig,
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig) {
        this(kafkaConfig, warehouse, database, null, null, null, null, catalogConfig, tableConfig);
    }

    public KafkaSyncDatabaseAction(
            Map<String, String> kafkaConfig,
            String warehouse,
            String database,
            @Nullable String tablePrefix,
            @Nullable String tableSuffix,
            @Nullable String includingTables,
            @Nullable String excludingTables,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig) {
        super(warehouse, catalogConfig);
        this.kafkaConfig = Configuration.fromMap(kafkaConfig);
        this.database = database;
        this.tablePrefix = tablePrefix == null ? "" : tablePrefix;
        this.tableSuffix = tableSuffix == null ? "" : tableSuffix;
        this.includingPattern = includingTables == null ? null : Pattern.compile(includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        this.tableConfig = tableConfig;
    }

    public void build(StreamExecutionEnvironment env) throws Exception {
        boolean caseSensitive = catalog.caseSensitive();

        if (!caseSensitive) {
            validateCaseInsensitive();
        }

        catalog.createDatabase(database, true);
        TableNameConverter tableNameConverter =
                new TableNameConverter(caseSensitive, true, tablePrefix, tableSuffix);

        KafkaSource<String> source = KafkaActionUtils.buildKafkaSource(kafkaConfig);

        EventParser.Factory<RichCdcMultiplexRecord> parserFactory;
        String format = kafkaConfig.get(KafkaConnectorOptions.VALUE_FORMAT);
        if ("canal-json".equals(format)) {
            RichCdcMultiplexRecordSchemaBuilder schemaBuilder =
                    new RichCdcMultiplexRecordSchemaBuilder(tableConfig);
            Pattern includingPattern = this.includingPattern;
            Pattern excludingPattern = this.excludingPattern;
            parserFactory =
                    () ->
                            new RichCdcMultiplexRecordEventParser(
                                    schemaBuilder, includingPattern, excludingPattern);
        } else {
            throw new UnsupportedOperationException("This format: " + format + " is not support.");
        }

        FlinkCdcSyncDatabaseSinkBuilder<RichCdcMultiplexRecord> sinkBuilder =
                new FlinkCdcSyncDatabaseSinkBuilder<RichCdcMultiplexRecord>()
                        .withInput(
                                env.fromSource(
                                                source,
                                                WatermarkStrategy.noWatermarks(),
                                                "Kafka Source")
                                        .flatMap(
                                                new CanalRecordParser(
                                                        caseSensitive, tableNameConverter)))
                        .withParserFactory(parserFactory)
                        .withCatalogLoader(catalogLoader())
                        .withDatabase(database)
                        .withMode(DatabaseSyncMode.COMBINED);
        String sinkParallelism = tableConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        sinkBuilder.build();
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
