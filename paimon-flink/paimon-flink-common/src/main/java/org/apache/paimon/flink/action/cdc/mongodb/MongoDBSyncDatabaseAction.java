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

package org.apache.paimon.flink.action.cdc.mongodb;

import static org.apache.paimon.utils.Preconditions.checkArgument;

import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkBuilder;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordSchemaBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * An action class responsible for synchronizing MongoDB databases with a target system.
 *
 * <p>This class provides functionality to read data from a MongoDB source, process it, and then
 * synchronize it with a target system. It supports various configurations, including table
 * prefixes, suffixes, and inclusion/exclusion patterns.
 *
 * <p>Key features include:
 *
 * <ul>
 *   <li>Support for case-sensitive and case-insensitive database and table names.
 *   <li>Configurable table name conversion with prefixes and suffixes.
 *   <li>Ability to include or exclude specific tables using regular expressions.
 *   <li>Integration with Flink's streaming environment for data processing.
 * </ul>
 *
 * <p>Note: This action is primarily intended for use in Flink streaming applications that
 * synchronize MongoDB data with other systems.
 */
public class MongoDBSyncDatabaseAction extends ActionBase {

    private final Configuration mongodbConfig;
    private final String database;
    private final String tablePrefix;
    private final String tableSuffix;
    private final Map<String, String> tableConfig;
    @Nullable private final Pattern includingPattern;
    @Nullable private final Pattern excludingPattern;
    @Nullable private final String includingTables;

    public MongoDBSyncDatabaseAction(
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
        this.mongodbConfig = Configuration.fromMap(kafkaConfig);
        this.database = database;
        this.tablePrefix = tablePrefix == null ? "" : tablePrefix;
        this.tableSuffix = tableSuffix == null ? "" : tableSuffix;
        this.includingTables = includingTables == null ? ".*" : includingTables;
        this.includingPattern = Pattern.compile(this.includingTables);
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
        List<Identifier> excludedTables = new ArrayList<>();

        MongoDBSource<String> source =
                MongoDBActionUtils.buildMongodbSource(
                        mongodbConfig, buildTableList(excludedTables));

        EventParser.Factory<RichCdcMultiplexRecord> parserFactory;
        RichCdcMultiplexRecordSchemaBuilder schemaBuilder =
                new RichCdcMultiplexRecordSchemaBuilder(tableConfig);
        Pattern includingPattern = this.includingPattern;
        Pattern excludingPattern = this.excludingPattern;
        parserFactory =
                () ->
                        new RichCdcMultiplexRecordEventParser(
                                schemaBuilder, includingPattern, excludingPattern);
        FlinkCdcSyncDatabaseSinkBuilder<RichCdcMultiplexRecord> sinkBuilder =
                new FlinkCdcSyncDatabaseSinkBuilder<RichCdcMultiplexRecord>()
                        .withInput(
                                env.fromSource(
                                                source,
                                                WatermarkStrategy.noWatermarks(),
                                                "MongoDB Source")
                                        .flatMap(
                                                new MongoDBRecordParser(
                                                        false, tableNameConverter, mongodbConfig)))
                        .withParserFactory(parserFactory)
                        .withCatalogLoader(catalogLoader())
                        .withDatabase(database)
                        .withMode(MultiTablesSinkMode.COMBINED);
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

    private String buildTableList(List<Identifier> excludedTables) {
        String separatorRex = "\\.";
        // In COMBINED mode, we should consider both existed tables and possible newly added
        // tables, so we should use regular expression to monitor all valid tables and exclude
        // certain invalid tables

        // The table list is built by template:
        // (?!(^db\\.tbl$)|(^...$))(databasePattern\\.(including_pattern1|...))

        // The excluding pattern ?!(^db\\.tbl$)|(^...$) can exclude tables whose qualified name
        // is exactly equal to 'db.tbl'
        // The including pattern databasePattern\\.(including_pattern1|...) can include tables
        // whose qualified name matches one of the patterns

        // a table can be monitored only when its name meets the including pattern and doesn't
        // be excluded by excluding pattern at the same time
        String includingPattern =
                String.format(
                        "%s%s(%s)",
                        mongodbConfig.get(MongoDBSourceOptions.DATABASE),
                        separatorRex,
                        includingTables);
        if (excludedTables.isEmpty()) {
            return includingPattern;
        }

        String excludingPattern =
                excludedTables.stream()
                        .map(
                                t ->
                                        String.format(
                                                "(^%s$)",
                                                t.getDatabaseName()
                                                        + separatorRex
                                                        + t.getObjectName()))
                        .collect(Collectors.joining("|"));
        excludingPattern = "?!" + excludingPattern;
        return String.format("(%s)(%s)", excludingPattern, includingPattern);
    }

    @VisibleForTesting
    public Map<String, String> tableConfig() {
        return tableConfig;
    }

    @VisibleForTesting
    public Map<String, String> catalogConfig() {
        return catalogConfig;
    }

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        env.execute(String.format("MongoDB-Paimon Database Sync: %s", database));
    }
}
