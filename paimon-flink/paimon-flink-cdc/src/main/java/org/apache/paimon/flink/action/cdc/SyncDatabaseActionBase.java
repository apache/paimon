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
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkBuilder;
import org.apache.paimon.flink.sink.cdc.NewTableSchemaBuilder;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import javax.annotation.Nullable;

import java.util.*;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;

/** Base {@link Action} for synchronizing into one Paimon database. */
public abstract class SyncDatabaseActionBase extends ActionBase {

    protected final String database;
    protected final Configuration cdcSourceConfig;
    protected Map<String, String> tableConfig = new HashMap<>();
    protected boolean mergeShards = true;
    protected MultiTablesSinkMode mode = COMBINED;
    protected String tablePrefix = "";
    protected String tableSuffix = "";
    protected String includingTables = ".*";
    @Nullable protected String excludingTables;
    protected TypeMapping typeMapping = TypeMapping.defaultMapping();

    protected CdcMetadataConverter[] metadataConverters = new CdcMetadataConverter[] {};
    protected List<FileStoreTable> tables = new ArrayList<>();

    public SyncDatabaseActionBase(
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> cdcSourceConfig) {
        super(warehouse, catalogConfig);
        this.database = database;
        this.cdcSourceConfig = Configuration.fromMap(cdcSourceConfig);
    }

    public SyncDatabaseActionBase withTableConfig(Map<String, String> tableConfig) {
        this.tableConfig = tableConfig;
        return this;
    }

    public SyncDatabaseActionBase mergeShards(boolean mergeShards) {
        this.mergeShards = mergeShards;
        return this;
    }

    public SyncDatabaseActionBase withMode(MultiTablesSinkMode mode) {
        this.mode = mode;
        return this;
    }

    public SyncDatabaseActionBase withTablePrefix(@Nullable String tablePrefix) {
        if (tablePrefix != null) {
            this.tablePrefix = tablePrefix;
        }
        return this;
    }

    public SyncDatabaseActionBase withTableSuffix(@Nullable String tableSuffix) {
        if (tableSuffix != null) {
            this.tableSuffix = tableSuffix;
        }
        return this;
    }

    public SyncDatabaseActionBase includingTables(@Nullable String includingTables) {
        if (includingTables != null) {
            this.includingTables = includingTables;
        }
        return this;
    }

    public SyncDatabaseActionBase excludingTables(@Nullable String excludingTables) {
        this.excludingTables = excludingTables;
        return this;
    }

    public SyncDatabaseActionBase withTypeMapping(TypeMapping typeMapping) {
        this.typeMapping = typeMapping;
        return this;
    }

    public SyncDatabaseActionBase withMetadataColumns(String... metadataColumns) {
        return withMetadataColumns(Arrays.asList(metadataColumns));
    }

    public SyncDatabaseActionBase withMetadataColumns(List<String> metadataColumns) {
        this.metadataConverters =
                metadataColumns.stream()
                        .map(this::metadataConverter)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .toArray(CdcMetadataConverter[]::new);
        return this;
    }

    protected Optional<CdcMetadataConverter<?>> metadataConverter(String column) {
        return Optional.empty();
    }

    protected void checkCdcSourceArgument() {}

    protected abstract DataStreamSource<String> buildSource() throws Exception;

    protected abstract String sourceName();

    protected abstract FlatMapFunction<String, RichCdcMultiplexRecord> recordParse();

    @Override
    public void build() throws Exception {
        checkCdcSourceArgument();
        boolean caseSensitive = catalog.caseSensitive();

        validateCaseInsensitive(caseSensitive);

        catalog.createDatabase(database, true);

        NewTableSchemaBuilder schemaBuilder = new NewTableSchemaBuilder(tableConfig, caseSensitive);
        Pattern includingPattern = Pattern.compile(includingTables);
        Pattern excludingPattern =
                excludingTables == null ? null : Pattern.compile(excludingTables);
        TableNameConverter tableNameConverter =
                new TableNameConverter(caseSensitive, mergeShards, tablePrefix, tableSuffix);

        DataStream<RichCdcMultiplexRecord> input =
                buildSource().flatMap(recordParse()).name("Parse");
        EventParser.Factory<RichCdcMultiplexRecord> parserFactory =
                () ->
                        new RichCdcMultiplexRecordEventParser(
                                schemaBuilder,
                                includingPattern,
                                excludingPattern,
                                tableNameConverter);

        new FlinkCdcSyncDatabaseSinkBuilder<RichCdcMultiplexRecord>()
                .withInput(input)
                .withParserFactory(parserFactory)
                .withCatalogLoader(catalogLoader())
                .withDatabase(database)
                .withTables(tables)
                .withMode(mode)
                .withTableOptions(tableConfig)
                .build();
    }

    protected void validateCaseInsensitive(boolean caseSensitive) {
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

    protected abstract String jobName();

    @Override
    public void run() throws Exception {
        build();
        execute(jobName());
    }
}
