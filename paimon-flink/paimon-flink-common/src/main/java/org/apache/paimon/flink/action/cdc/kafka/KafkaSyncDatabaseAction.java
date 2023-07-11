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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.kafka.canal.KafkaSourceConversionProcessFunction;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkBuilder;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An {@link Action} which synchronize the Multiple topics into one Paimon database.
 *
 * <p>You should specify Kafka source topic in {@code kafkaConfig}. See <a
 * href="https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/connectors/table/kafka/">document
 * of flink-connectors</a> for detailed keys and values.
 *
 * <p>For each topic's table to be synchronized, if the corresponding Paimon table does not exist,
 * this action will automatically create the table. Its schema will be derived from all specified
 * tables. If the Paimon table already exists, its schema will be compared against the schema of all
 * specified tables.
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
 * <p>This action creates a Paimon table sink for each Paimon table to be written, so this action is
 * not very efficient in resource saving. We may optimize this action by merging all sinks into one
 * instance in the future.
 */
public class KafkaSyncDatabaseAction extends ActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSyncDatabaseAction.class);

    private final Configuration kafkaConfig;
    private final String database;
    private final int schemaInitMaxRead;
    private final boolean ignoreIncompatible;
    private final String tablePrefix;
    private final String tableSuffix;
    @Nullable private final Pattern includingPattern;
    @Nullable private final Pattern excludingPattern;
    private final Map<String, String> tableConfig;

    KafkaSyncDatabaseAction(
            Map<String, String> kafkaConfig,
            String warehouse,
            String database,
            boolean ignoreIncompatible,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig) {
        this(
                kafkaConfig,
                warehouse,
                database,
                0,
                ignoreIncompatible,
                null,
                null,
                null,
                null,
                catalogConfig,
                tableConfig);
    }

    KafkaSyncDatabaseAction(
            Map<String, String> kafkaConfig,
            String warehouse,
            String database,
            int schemaInitMaxRead,
            boolean ignoreIncompatible,
            @Nullable String tablePrefix,
            @Nullable String tableSuffix,
            @Nullable String includingTables,
            @Nullable String excludingTables,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig) {
        super(warehouse, catalogConfig);
        this.kafkaConfig = Configuration.fromMap(kafkaConfig);
        this.database = database;
        this.schemaInitMaxRead = schemaInitMaxRead;
        this.ignoreIncompatible = ignoreIncompatible;
        this.tablePrefix = tablePrefix == null ? "" : tablePrefix;
        this.tableSuffix = tableSuffix == null ? "" : tableSuffix;
        this.includingPattern = includingTables == null ? null : Pattern.compile(includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        this.tableConfig = tableConfig;
    }

    public void build(StreamExecutionEnvironment env) throws Exception {
        checkArgument(
                kafkaConfig.contains(KafkaConnectorOptions.VALUE_FORMAT),
                KafkaConnectorOptions.VALUE_FORMAT.key() + " cannot be null.");
        checkArgument(
                !CollectionUtil.isNullOrEmpty(kafkaConfig.get(KafkaConnectorOptions.TOPIC)),
                KafkaConnectorOptions.TOPIC.key() + " cannot be null.");

        boolean caseSensitive = catalog.caseSensitive();

        if (!caseSensitive) {
            validateCaseInsensitive();
        }

        Map<String, List<KafkaSchema>> kafkaCanalSchemaMap = getKafkaCanalSchemaMap();

        catalog.createDatabase(database, true);
        TableNameConverter tableNameConverter =
                new TableNameConverter(caseSensitive, tablePrefix, tableSuffix);

        List<FileStoreTable> fileStoreTables = new ArrayList<>();
        List<String> monitoredTopics = new ArrayList<>();
        for (Map.Entry<String, List<KafkaSchema>> kafkaCanalSchemaEntry :
                kafkaCanalSchemaMap.entrySet()) {
            List<KafkaSchema> kafkaSchemaList = kafkaCanalSchemaEntry.getValue();
            String topic = kafkaCanalSchemaEntry.getKey();
            for (KafkaSchema kafkaSchema : kafkaSchemaList) {
                String paimonTableName = tableNameConverter.convert(kafkaSchema.tableName());
                Identifier identifier = new Identifier(database, paimonTableName);
                FileStoreTable table;
                Schema fromCanal =
                        KafkaActionUtils.buildPaimonSchema(
                                kafkaSchema,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                tableConfig,
                                caseSensitive);
                try {
                    table = (FileStoreTable) catalog.getTable(identifier);
                    Supplier<String> errMsg =
                            incompatibleMessage(table.schema(), kafkaSchema, identifier);
                    if (shouldMonitorTable(table.schema(), fromCanal, errMsg)) {
                        monitoredTopics.add(topic);
                        fileStoreTables.add(table);
                    }
                } catch (Catalog.TableNotExistException e) {
                    catalog.createTable(identifier, fromCanal, false);
                    table = (FileStoreTable) catalog.getTable(identifier);
                    monitoredTopics.add(topic);
                    fileStoreTables.add(table);
                }
            }
        }
        monitoredTopics = monitoredTopics.stream().distinct().collect(Collectors.toList());
        Preconditions.checkState(
                !fileStoreTables.isEmpty(),
                "No tables to be synchronized. Possible cause is the schemas of all tables in specified "
                        + "Kafka topic's table are not compatible with those of existed Paimon tables. Please check the log.");

        kafkaConfig.set(KafkaConnectorOptions.TOPIC, monitoredTopics);
        KafkaSource<String> source = KafkaActionUtils.buildKafkaSource(kafkaConfig);

        EventParser.Factory<RichCdcMultiplexRecord> parserFactory;
        String format = kafkaConfig.get(KafkaConnectorOptions.VALUE_FORMAT);
        if ("canal-json".equals(format)) {
            parserFactory = RichCdcMultiplexRecordEventParser::new;
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
                                        .process(
                                                new KafkaSourceConversionProcessFunction(
                                                        caseSensitive, tableNameConverter)))
                        .withParserFactory(parserFactory)
                        .withTables(fileStoreTables)
                        .withCatalogLoader(catalogLoader())
                        .withDatabase(database);
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

    private Map<String, List<KafkaSchema>> getKafkaCanalSchemaMap() throws Exception {
        Map<String, List<KafkaSchema>> kafkaCanalSchemaMap = new HashMap<>();
        List<String> topicList = kafkaConfig.get(KafkaConnectorOptions.TOPIC);
        if (topicList.size() > 1) {
            topicList.forEach(
                    topic -> {
                        try {
                            KafkaSchema kafkaSchema =
                                    KafkaSchema.getKafkaSchema(kafkaConfig, topic);
                            if (shouldMonitorTable(kafkaSchema.tableName())) {
                                kafkaCanalSchemaMap.put(
                                        topic, Collections.singletonList(kafkaSchema));
                            }

                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        } else {
            List<KafkaSchema> kafkaSchemaList =
                    KafkaSchema.getListKafkaSchema(
                            kafkaConfig, topicList.get(0), schemaInitMaxRead);
            kafkaSchemaList =
                    kafkaSchemaList.stream()
                            .filter(kafkaSchema -> shouldMonitorTable(kafkaSchema.tableName()))
                            .collect(Collectors.toList());
            kafkaCanalSchemaMap.put(topicList.get(0), kafkaSchemaList);
        }

        return kafkaCanalSchemaMap;
    }

    private boolean shouldMonitorTable(String mySqlTableName) {
        boolean shouldMonitor = true;
        if (includingPattern != null) {
            shouldMonitor = includingPattern.matcher(mySqlTableName).matches();
        }
        if (excludingPattern != null) {
            shouldMonitor = shouldMonitor && !excludingPattern.matcher(mySqlTableName).matches();
        }
        LOG.debug("Source table {} is monitored? {}", mySqlTableName, shouldMonitor);
        return shouldMonitor;
    }

    private boolean shouldMonitorTable(
            TableSchema tableSchema, Schema schema, Supplier<String> errMsg) {
        if (KafkaActionUtils.schemaCompatible(tableSchema, schema)) {
            return true;
        } else if (ignoreIncompatible) {
            LOG.warn(errMsg.get() + "This table will be ignored.");
            return false;
        } else {
            throw new IllegalArgumentException(
                    errMsg.get()
                            + "If you want to ignore the incompatible tables, please specify --ignore-incompatible to true.");
        }
    }

    private Supplier<String> incompatibleMessage(
            TableSchema paimonSchema, KafkaSchema kafkaSchema, Identifier identifier) {
        return () ->
                String.format(
                        "Incompatible schema found.\n"
                                + "Paimon table is: %s, fields are: %s.\n"
                                + "Kafka's table is: %s.%s, fields are: %s.\n",
                        identifier.getFullName(),
                        paimonSchema.fields(),
                        kafkaSchema.databaseName(),
                        kafkaSchema.tableName(),
                        kafkaSchema.fields());
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
