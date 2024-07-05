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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.cdc.watermark.CdcWatermarkStrategy;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.TagCreationMode.WATERMARK;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_GROUP;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_IDLE_TIMEOUT;
import static org.apache.paimon.flink.action.cdc.watermark.CdcTimestampExtractorFactory.createExtractor;

/** Base {@link Action} for table/database synchronizing job. */
public abstract class SynchronizationActionBase extends ActionBase {

    private static final long DEFAULT_CHECKPOINT_INTERVAL = 3 * 60 * 1000;

    protected final String database;
    protected final Configuration cdcSourceConfig;
    protected final SyncJobHandler syncJobHandler;
    protected final boolean caseSensitive;

    protected Map<String, String> tableConfig = new HashMap<>();
    protected TypeMapping typeMapping = TypeMapping.defaultMapping();
    protected CdcMetadataConverter[] metadataConverters = new CdcMetadataConverter[] {};

    public SynchronizationActionBase(
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> cdcSourceConfig,
            SyncJobHandler syncJobHandler) {
        super(warehouse, catalogConfig);
        this.database = database;
        this.cdcSourceConfig = Configuration.fromMap(cdcSourceConfig);
        this.syncJobHandler = syncJobHandler;
        this.caseSensitive = catalog.caseSensitive();

        this.syncJobHandler.registerJdbcDriver();
    }

    public SynchronizationActionBase withTableConfig(Map<String, String> tableConfig) {
        this.tableConfig = tableConfig;
        return this;
    }

    public SynchronizationActionBase withTypeMapping(TypeMapping typeMapping) {
        this.typeMapping = typeMapping;
        return this;
    }

    public SynchronizationActionBase withMetadataColumns(List<String> metadataColumns) {
        this.metadataConverters =
                metadataColumns.stream()
                        .map(this.syncJobHandler::provideMetadataConverter)
                        .toArray(CdcMetadataConverter[]::new);
        return this;
    }

    @VisibleForTesting
    public Map<String, String> tableConfig() {
        return tableConfig;
    }

    @Override
    public void build() throws Exception {
        syncJobHandler.checkRequiredOption();

        catalog.createDatabase(database, true);

        validateCaseSensitivity();

        beforeBuildingSourceSink();

        DataStream<RichCdcMultiplexRecord> input =
                buildDataStreamSource(buildSource()).flatMap(recordParse()).name("Parse");

        EventParser.Factory<RichCdcMultiplexRecord> parserFactory = buildEventParserFactory();

        buildSink(input, parserFactory);
    }

    protected abstract void validateCaseSensitivity();

    protected void beforeBuildingSourceSink() throws Exception {}

    protected Object buildSource() {
        return syncJobHandler.provideSource();
    }

    private DataStreamSource<CdcSourceRecord> buildDataStreamSource(Object source) {
        if (source instanceof Source) {
            boolean isAutomaticWatermarkCreationEnabled =
                    tableConfig.containsKey(CoreOptions.TAG_AUTOMATIC_CREATION.key())
                            && Objects.equals(
                                    tableConfig.get(CoreOptions.TAG_AUTOMATIC_CREATION.key()),
                                    WATERMARK.toString());

            Options options = Options.fromMap(tableConfig);
            Duration idleTimeout = options.get(SCAN_WATERMARK_IDLE_TIMEOUT);
            String watermarkAlignGroup = options.get(SCAN_WATERMARK_ALIGNMENT_GROUP);
            WatermarkStrategy<CdcSourceRecord> watermarkStrategy =
                    isAutomaticWatermarkCreationEnabled
                            ? watermarkAlignGroup != null
                                    ? new CdcWatermarkStrategy(createExtractor(source))
                                            .withWatermarkAlignment(
                                                    watermarkAlignGroup,
                                                    options.get(SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT),
                                                    options.get(
                                                            SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL))
                                    : new CdcWatermarkStrategy(createExtractor(source))
                            : WatermarkStrategy.noWatermarks();
            if (idleTimeout != null) {
                watermarkStrategy = watermarkStrategy.withIdleness(idleTimeout);
            }
            return env.fromSource(
                    (Source<CdcSourceRecord, ?, ?>) source,
                    watermarkStrategy,
                    syncJobHandler.provideSourceName());
        }
        if (source instanceof SourceFunction) {
            return env.addSource(
                    (SourceFunction<CdcSourceRecord>) source, syncJobHandler.provideSourceName());
        }
        throw new UnsupportedOperationException("Unrecognized source type");
    }

    protected abstract FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> recordParse();

    protected abstract EventParser.Factory<RichCdcMultiplexRecord> buildEventParserFactory();

    protected abstract void buildSink(
            DataStream<RichCdcMultiplexRecord> input,
            EventParser.Factory<RichCdcMultiplexRecord> parserFactory);

    protected FileStoreTable alterTableOptions(Identifier identifier, FileStoreTable table) {
        // doesn't support altering bucket here
        Map<String, String> dynamicOptions = new HashMap<>(tableConfig);
        dynamicOptions.remove(CoreOptions.BUCKET.key());

        // remove immutable options and options with equal values
        Map<String, String> oldOptions = table.options();
        Set<String> immutableOptionKeys = CoreOptions.getImmutableOptionKeys();
        dynamicOptions
                .entrySet()
                .removeIf(
                        entry ->
                                immutableOptionKeys.contains(entry.getKey())
                                        || Objects.equals(
                                                oldOptions.get(entry.getKey()), entry.getValue()));

        // alter the table dynamic options
        List<SchemaChange> optionChanges =
                dynamicOptions.entrySet().stream()
                        .map(entry -> SchemaChange.setOption(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());

        try {
            catalog.alterTable(identifier, optionChanges, false);
        } catch (Catalog.TableNotExistException
                | Catalog.ColumnAlreadyExistException
                | Catalog.ColumnNotExistException e) {
            throw new RuntimeException("This is unexpected.", e);
        }

        return table.copy(dynamicOptions);
    }

    @Override
    public void run() throws Exception {
        build();
        if (!env.getCheckpointConfig().isCheckpointingEnabled()) {
            env.enableCheckpointing(DEFAULT_CHECKPOINT_INTERVAL);
        }
        execute(syncJobHandler.provideDefaultJobName());
    }
}
