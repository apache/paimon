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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.cdc.SinkMode;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkBuilder;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordSchemaBuilder;

import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An {@link Action} which synchronize the whole MongoDB database into one Paimon database.
 *
 * <p>You should specify MongoDB source database in {@code mongodbConfig}. See <a
 * href="https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mongodb-cdc.html#connector-options">document
 * of flink-cdc-connectors</a> for detailed keys and values.
 *
 * <p>For each MongoDB collections to be synchronized, if the corresponding Paimon table does not
 * exist, this action will automatically create the table. Its schema will be derived from all
 * specified MongoDB collections. If the Paimon table already exists, its schema will be compared
 * against the schema of all specified MongoDB collections.
 *
 * <p>This action supports a limited number of schema changes. Currently, the framework can not drop
 * columns, so the behaviors of `DROP` will be ignored, `RENAME` will add a new column. Currently
 * supported schema changes includes:
 *
 * <ul>
 *   <li>Adding columns.
 * </ul>
 *
 * <p>To automatically synchronize new table, This action creates a single sink for all Paimon
 * tables to be written. See {@link SinkMode#COMBINED}.
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
            Map<String, String> mongodbConfig,
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig) {
        this(
                mongodbConfig,
                warehouse,
                database,
                null,
                null,
                null,
                null,
                catalogConfig,
                tableConfig);
    }

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
                        .withSinkMode(SinkMode.COMBINED);
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
