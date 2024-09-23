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

package org.apache.paimon.flink.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.LogChangelogMode;
import org.apache.paimon.CoreOptions.LogConsistency;
import org.apache.paimon.flink.FlinkConnectorOptions.WatermarkEmitStrategy;
import org.apache.paimon.flink.PaimonDataStreamScanProvider;
import org.apache.paimon.flink.log.LogSourceProvider;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.flink.lookup.FileStoreLookupFunction;
import org.apache.paimon.flink.lookup.LookupRuntimeProviderFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Projection;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.LookupTableSource.LookupContext;
import org.apache.flink.table.connector.source.LookupTableSource.LookupRuntimeProvider;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.CoreOptions.LOG_CONSISTENCY;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_ASYNC;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_ASYNC_THREAD_NUMBER;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_REMOVE_NORMALIZE;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_GROUP;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_EMIT_STRATEGY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_IDLE_TIMEOUT;

/**
 * Table source to create {@link StaticFileStoreSource} or {@link ContinuousFileStoreSource} under
 * batch mode or streaming mode.
 */
public abstract class BaseDataTableSource extends FlinkTableSource
        implements LookupTableSource, SupportsWatermarkPushDown {

    protected final ObjectIdentifier tableIdentifier;
    protected final boolean streaming;
    protected final DynamicTableFactory.Context context;
    @Nullable protected final LogStoreTableFactory logStoreTableFactory;

    @Nullable protected WatermarkStrategy<RowData> watermarkStrategy;

    public BaseDataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy) {
        super(table, predicate, projectFields, limit);
        this.tableIdentifier = tableIdentifier;
        this.streaming = streaming;
        this.context = context;
        this.logStoreTableFactory = logStoreTableFactory;
        this.predicate = predicate;
        this.projectFields = projectFields;
        this.limit = limit;
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (!streaming) {
            // batch merge all, return insert only
            return ChangelogMode.insertOnly();
        }

        if (table.primaryKeys().isEmpty()) {
            return ChangelogMode.insertOnly();
        } else {
            Options options = Options.fromMap(table.options());

            if (new CoreOptions(options).mergeEngine() == CoreOptions.MergeEngine.FIRST_ROW) {
                return ChangelogMode.insertOnly();
            }

            if (options.get(SCAN_REMOVE_NORMALIZE)) {
                return ChangelogMode.all();
            }

            if (logStoreTableFactory == null
                    && options.get(CHANGELOG_PRODUCER) != ChangelogProducer.NONE) {
                return ChangelogMode.all();
            }

            // optimization: transaction consistency and all changelog mode avoid the generation of
            // normalized nodes. See FlinkTableSink.getChangelogMode validation.
            return options.get(LOG_CONSISTENCY) == LogConsistency.TRANSACTIONAL
                            && options.get(LOG_CHANGELOG_MODE) == LogChangelogMode.ALL
                    ? ChangelogMode.all()
                    : ChangelogMode.upsert();
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        LogSourceProvider logSourceProvider = null;
        if (logStoreTableFactory != null) {
            logSourceProvider =
                    logStoreTableFactory.createSourceProvider(context, scanContext, projectFields);
        }

        WatermarkStrategy<RowData> watermarkStrategy = this.watermarkStrategy;
        Options options = Options.fromMap(table.options());
        if (watermarkStrategy != null) {
            WatermarkEmitStrategy emitStrategy = options.get(SCAN_WATERMARK_EMIT_STRATEGY);
            if (emitStrategy == WatermarkEmitStrategy.ON_EVENT) {
                watermarkStrategy = new OnEventWatermarkStrategy(watermarkStrategy);
            }
            Duration idleTimeout = options.get(SCAN_WATERMARK_IDLE_TIMEOUT);
            if (idleTimeout != null) {
                watermarkStrategy = watermarkStrategy.withIdleness(idleTimeout);
            }
            String watermarkAlignGroup = options.get(SCAN_WATERMARK_ALIGNMENT_GROUP);
            if (watermarkAlignGroup != null) {
                watermarkStrategy =
                        WatermarkAlignUtils.withWatermarkAlignment(
                                watermarkStrategy,
                                watermarkAlignGroup,
                                options.get(SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT),
                                options.get(SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL));
            }
        }

        FlinkSourceBuilder sourceBuilder =
                new FlinkSourceBuilder(table)
                        .sourceName(tableIdentifier.asSummaryString())
                        .sourceBounded(!streaming)
                        .logSourceProvider(logSourceProvider)
                        .projection(projectFields)
                        .predicate(predicate)
                        .limit(limit)
                        .watermarkStrategy(watermarkStrategy)
                        .dynamicPartitionFilteringFields(dynamicPartitionFilteringFields());

        return new PaimonDataStreamScanProvider(
                !streaming,
                env ->
                        sourceBuilder
                                .sourceParallelism(inferSourceParallelism(env))
                                .env(env)
                                .build());
    }

    protected abstract List<String> dynamicPartitionFilteringFields();

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        if (limit != null) {
            throw new RuntimeException(
                    "Limit push down should not happen in Lookup source, but it is " + limit);
        }
        int[] projection =
                projectFields == null
                        ? IntStream.range(0, table.rowType().getFieldCount()).toArray()
                        : Projection.of(projectFields).toTopLevelIndexes();
        int[] joinKey = Projection.of(context.getKeys()).toTopLevelIndexes();
        Options options = new Options(table.options());
        boolean enableAsync = options.get(LOOKUP_ASYNC);
        int asyncThreadNumber = options.get(LOOKUP_ASYNC_THREAD_NUMBER);
        return LookupRuntimeProviderFactory.create(
                new FileStoreLookupFunction(table, projection, joinKey, predicate),
                enableAsync,
                asyncThreadNumber);
    }

    @Override
    public String asSummaryString() {
        return "Paimon-DataSource";
    }

    @Override
    public boolean isStreaming() {
        return streaming;
    }
}
