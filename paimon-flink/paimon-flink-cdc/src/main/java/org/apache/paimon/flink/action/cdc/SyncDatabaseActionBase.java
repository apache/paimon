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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogUtils;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkBuilder;
import org.apache.paimon.flink.sink.cdc.NewTableSchemaBuilder;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;

/** Base {@link Action} for synchronizing into one Paimon database. */
public abstract class SyncDatabaseActionBase extends SynchronizationActionBase {

    protected boolean mergeShards = true;
    protected MultiTablesSinkMode mode = COMBINED;
    protected String tablePrefix = "";
    protected String tableSuffix = "";
    protected Map<String, String> tableMapping = new HashMap<>();
    protected Map<String, String> dbPrefix = new HashMap<>();
    protected Map<String, String> dbSuffix = new HashMap<>();
    protected String includingTables = ".*";
    protected List<String> partitionKeys = new ArrayList<>();
    protected List<String> primaryKeys = new ArrayList<>();
    @Nullable protected String excludingTables;
    protected List<FileStoreTable> tables = new ArrayList<>();
    protected Map<String, List<String>> partitionKeyMultiple = new HashMap<>();

    public SyncDatabaseActionBase(
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> cdcSourceConfig,
            SyncJobHandler.SourceType sourceType) {
        super(
                warehouse,
                database,
                catalogConfig,
                cdcSourceConfig,
                new SyncJobHandler(sourceType, cdcSourceConfig, database));
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

    public SyncDatabaseActionBase withDbPrefix(Map<String, String> dbPrefix) {
        if (dbPrefix != null) {
            this.dbPrefix =
                    dbPrefix.entrySet().stream()
                            .collect(
                                    HashMap::new,
                                    (m, e) -> m.put(e.getKey().toLowerCase(), e.getValue()),
                                    HashMap::putAll);
        }
        return this;
    }

    public SyncDatabaseActionBase withDbSuffix(Map<String, String> dbSuffix) {
        if (dbSuffix != null) {
            this.dbSuffix =
                    dbSuffix.entrySet().stream()
                            .collect(
                                    HashMap::new,
                                    (m, e) -> m.put(e.getKey().toLowerCase(), e.getValue()),
                                    HashMap::putAll);
        }
        return this;
    }

    public SyncDatabaseActionBase withTableMapping(Map<String, String> tableMapping) {
        if (tableMapping != null) {
            this.tableMapping = tableMapping;
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

    public SyncDatabaseActionBase withPartitionKeys(String... partitionKeys) {
        this.partitionKeys.addAll(Arrays.asList(partitionKeys));
        return this;
    }

    public SyncDatabaseActionBase withPrimaryKeys(String... primaryKeys) {
        this.primaryKeys.addAll(Arrays.asList(primaryKeys));
        return this;
    }

    @Override
    protected void validateCaseSensitivity() {
        CatalogUtils.validateCaseInsensitive(caseSensitive, "Database", database);
        CatalogUtils.validateCaseInsensitive(caseSensitive, "Table prefix", tablePrefix);
        CatalogUtils.validateCaseInsensitive(caseSensitive, "Table suffix", tableSuffix);
    }

    @Override
    protected FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> recordParse() {
        return syncJobHandler.provideRecordParser(
                Collections.emptyList(), typeMapping, metadataConverters);
    }

    public SyncDatabaseActionBase withPartitionKeyMultiple(
            Map<String, List<String>> partitionKeyMultiple) {
        if (partitionKeyMultiple != null) {
            this.partitionKeyMultiple = partitionKeyMultiple;
        }
        return this;
    }

    @Override
    protected EventParser.Factory<RichCdcMultiplexRecord> buildEventParserFactory() {
        NewTableSchemaBuilder schemaBuilder =
                new NewTableSchemaBuilder(
                        tableConfig,
                        caseSensitive,
                        partitionKeys,
                        primaryKeys,
                        requirePrimaryKeys(),
                        partitionKeyMultiple,
                        metadataConverters);
        Pattern includingPattern = Pattern.compile(includingTables);
        Pattern excludingPattern =
                excludingTables == null ? null : Pattern.compile(excludingTables);
        TableNameConverter tableNameConverter =
                new TableNameConverter(
                        caseSensitive,
                        mergeShards,
                        dbPrefix,
                        dbSuffix,
                        tablePrefix,
                        tableSuffix,
                        tableMapping);
        Set<String> createdTables;
        try {
            createdTables = new HashSet<>(catalog.listTables(database));
        } catch (Catalog.DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }
        return () ->
                new RichCdcMultiplexRecordEventParser(
                        schemaBuilder,
                        includingPattern,
                        excludingPattern,
                        tableNameConverter,
                        createdTables);
    }

    protected abstract boolean requirePrimaryKeys();

    @Override
    protected void buildSink(
            DataStream<RichCdcMultiplexRecord> input,
            EventParser.Factory<RichCdcMultiplexRecord> parserFactory) {
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
}
