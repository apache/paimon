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
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Projection;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsAggregatePushDown;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.CoreOptions.LOG_CONSISTENCY;
import static org.apache.paimon.CoreOptions.MergeEngine.FIRST_ROW;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_ASYNC;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_ASYNC_THREAD_NUMBER;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_REMOVE_NORMALIZE;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_GROUP;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_EMIT_STRATEGY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_IDLE_TIMEOUT;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Table source to create {@link StaticFileStoreSource} or {@link ContinuousFileStoreSource} under
 * batch mode or streaming mode.
 */
public abstract class BaseDataTableSource extends FlinkTableSource
        implements LookupTableSource, SupportsWatermarkPushDown, SupportsAggregatePushDown {

    private static final List<ConfigOption<?>> TIME_TRAVEL_OPTIONS =
            Arrays.asList(
                    CoreOptions.SCAN_TIMESTAMP,
                    CoreOptions.SCAN_TIMESTAMP_MILLIS,
                    CoreOptions.SCAN_WATERMARK,
                    CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS,
                    CoreOptions.SCAN_SNAPSHOT_ID,
                    CoreOptions.SCAN_TAG_NAME,
                    CoreOptions.SCAN_VERSION);

    protected final ObjectIdentifier tableIdentifier;
    protected final boolean streaming;
    protected final DynamicTableFactory.Context context;
    @Nullable protected final LogStoreTableFactory logStoreTableFactory;

    @Nullable protected WatermarkStrategy<RowData> watermarkStrategy;
    @Nullable protected Long countPushed;

    public BaseDataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy,
            @Nullable Long countPushed) {
        super(table, predicate, projectFields, limit);
        this.tableIdentifier = tableIdentifier;
        this.streaming = streaming;
        this.context = context;
        this.logStoreTableFactory = logStoreTableFactory;
        this.predicate = predicate;
        this.projectFields = projectFields;
        this.limit = limit;
        this.watermarkStrategy = watermarkStrategy;
        this.countPushed = countPushed;
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

            if (new CoreOptions(options).mergeEngine() == FIRST_ROW) {
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
        if (countPushed != null) {
            return createCountStarScan();
        }

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

    private ScanRuntimeProvider createCountStarScan() {
        checkNotNull(countPushed);
        NumberSequenceRowSource source = new NumberSequenceRowSource(countPushed, countPushed);
        return new SourceProvider() {
            @Override
            public Source<RowData, ?, ?> createSource() {
                return source;
            }

            @Override
            public boolean isBounded() {
                return true;
            }

            @Override
            public Optional<Integer> getParallelism() {
                return Optional.of(1);
            }
        };
    }

    protected abstract List<String> dynamicPartitionFilteringFields();

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    "Currently, lookup dim table only support FileStoreTable but is "
                            + table.getClass().getName());
        }

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
                getFileStoreLookupFunction(
                        context,
                        timeTravelDisabledTable((FileStoreTable) table),
                        projection,
                        joinKey),
                enableAsync,
                asyncThreadNumber);
    }

    protected FileStoreLookupFunction getFileStoreLookupFunction(
            LookupContext context, Table table, int[] projection, int[] joinKey) {
        return new FileStoreLookupFunction(table, projection, joinKey, predicate);
    }

    private FileStoreTable timeTravelDisabledTable(FileStoreTable table) {
        Map<String, String> newOptions = new HashMap<>(table.options());
        TIME_TRAVEL_OPTIONS.stream().map(ConfigOption::key).forEach(newOptions::remove);

        CoreOptions.StartupMode startupMode = CoreOptions.fromMap(newOptions).startupMode();
        if (startupMode != CoreOptions.StartupMode.COMPACTED_FULL) {
            startupMode = CoreOptions.StartupMode.LATEST_FULL;
        }
        newOptions.put(CoreOptions.SCAN_MODE.key(), startupMode.toString());

        TableSchema newSchema = table.schema().copy(newOptions);
        return table.copy(newSchema);
    }

    @Override
    public boolean applyAggregates(
            List<int[]> groupingSets,
            List<AggregateExpression> aggregateExpressions,
            DataType producedDataType) {
        if (isStreaming()) {
            return false;
        }

        if (!(table instanceof DataTable)) {
            return false;
        }

        if (groupingSets.size() != 1) {
            return false;
        }

        if (groupingSets.get(0).length != 0) {
            return false;
        }

        if (aggregateExpressions.size() != 1) {
            return false;
        }

        if (!aggregateExpressions
                .get(0)
                .getFunctionDefinition()
                .getClass()
                .getName()
                .equals(
                        "org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction")) {
            return false;
        }

        List<Split> splits =
                table.newReadBuilder().dropStats().withFilter(predicate).newScan().plan().splits();
        long countPushed = 0;
        for (Split s : splits) {
            if (!(s instanceof DataSplit)) {
                return false;
            }
            DataSplit split = (DataSplit) s;
            if (!split.mergedRowCountAvailable()) {
                return false;
            }

            countPushed += split.mergedRowCount();
        }

        this.countPushed = countPushed;
        return true;
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
