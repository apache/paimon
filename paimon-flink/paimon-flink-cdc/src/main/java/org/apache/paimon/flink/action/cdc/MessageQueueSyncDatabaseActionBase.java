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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.action.cdc.format.DataFormat;
import org.apache.paimon.flink.action.cdc.format.RecordParser;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkBuilder;
import org.apache.paimon.flink.sink.cdc.NewTableSchemaBuilder;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * An {@link Action} which synchronize the Multiple message queue topics into one Paimon database.
 *
 * <p>For each message queue topic's table to be synchronized, if the corresponding Paimon table
 * does not exist, this action will automatically create the table, and its schema will be derived
 * from all specified message queue topic's tables. If the Paimon table already exists and its
 * schema is different from that parsed from message queue record, this action will try to preform
 * schema evolution.
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
public abstract class MessageQueueSyncDatabaseActionBase extends ActionBase {

    protected final String database;
    protected final Configuration cdcSourceConfig;

    private Map<String, String> tableConfig = new HashMap<>();
    private String tablePrefix = "";
    private String tableSuffix = "";
    private String includingTables = ".*";
    @Nullable String excludingTables;
    private TypeMapping typeMapping = TypeMapping.defaultMapping();

    public MessageQueueSyncDatabaseActionBase(
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> mqConfig) {
        super(warehouse, catalogConfig);
        this.database = database;
        this.cdcSourceConfig = Configuration.fromMap(mqConfig);
    }

    public MessageQueueSyncDatabaseActionBase withTableConfig(Map<String, String> tableConfig) {
        this.tableConfig = tableConfig;
        return this;
    }

    public MessageQueueSyncDatabaseActionBase withTablePrefix(@Nullable String tablePrefix) {
        if (tablePrefix != null) {
            this.tablePrefix = tablePrefix;
        }
        return this;
    }

    public MessageQueueSyncDatabaseActionBase withTableSuffix(@Nullable String tableSuffix) {
        if (tableSuffix != null) {
            this.tableSuffix = tableSuffix;
        }
        return this;
    }

    public MessageQueueSyncDatabaseActionBase includingTables(@Nullable String includingTables) {
        if (includingTables != null) {
            this.includingTables = includingTables;
        }
        return this;
    }

    public MessageQueueSyncDatabaseActionBase excludingTables(@Nullable String excludingTables) {
        this.excludingTables = excludingTables;
        return this;
    }

    public MessageQueueSyncDatabaseActionBase withTypeMapping(TypeMapping typeMapping) {
        this.typeMapping = typeMapping;
        return this;
    }

    protected abstract DataStreamSource<String> buildSource() throws Exception;

    protected abstract String sourceName();

    protected abstract DataFormat getDataFormat();

    protected abstract String jobName();

    @Override
    public void build() throws Exception {
        boolean caseSensitive = catalog.caseSensitive();

        validateCaseInsensitive(caseSensitive);

        catalog.createDatabase(database, true);

        DataFormat format = getDataFormat();
        RecordParser recordParser =
                format.createParser(caseSensitive, typeMapping, Collections.emptyList());
        NewTableSchemaBuilder schemaBuilder = new NewTableSchemaBuilder(tableConfig, caseSensitive);
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
                .withInput(buildSource().flatMap(recordParser).name("Parse"))
                .withParserFactory(parserFactory)
                .withCatalogLoader(catalogLoader())
                .withDatabase(database)
                .withMode(MultiTablesSinkMode.COMBINED)
                .withTableOptions(tableConfig)
                .build();
    }

    private void validateCaseInsensitive(boolean caseSensitive) {
        AbstractCatalog.validateCaseInsensitive(caseSensitive, "Database", database);
        AbstractCatalog.validateCaseInsensitive(caseSensitive, "Table prefix", tablePrefix);
        AbstractCatalog.validateCaseInsensitive(caseSensitive, "Table suffix", tableSuffix);
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
        build();
        execute(String.format("KAFKA-Paimon Database Sync: %s", database));
    }
}
