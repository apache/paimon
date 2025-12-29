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
import org.apache.paimon.flink.FlinkConnectorOptions.WatermarkEmitStrategy;
import org.apache.paimon.flink.PaimonDataStreamScanProvider;
import org.apache.paimon.flink.lookup.FileStoreLookupFunction;
import org.apache.paimon.flink.lookup.LookupRuntimeProviderFactory;
import org.apache.paimon.flink.lookup.partitioner.BucketIdExtractor;
import org.apache.paimon.flink.lookup.partitioner.BucketShufflePartitioner;
import org.apache.paimon.flink.lookup.partitioner.BucketShuffleStrategy;
import org.apache.paimon.flink.lookup.partitioner.ShuffleStrategy;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.BucketSpec;
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
import org.apache.flink.table.connector.source.abilities.SupportsLookupCustomShuffle;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
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
        implements LookupTableSource,
                SupportsWatermarkPushDown,
                SupportsAggregatePushDown,
                SupportsLookupCustomShuffle {
    private static final Logger LOG = LoggerFactory.getLogger(BaseDataTableSource.class);

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
    protected final boolean unbounded;
    protected final DynamicTableFactory.Context context;
    @Nullable private BucketShufflePartitioner bucketShufflePartitioner;
    @Nullable protected WatermarkStrategy<RowData> watermarkStrategy;
    @Nullable protected Long countPushed;

    public BaseDataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean unbounded,
            DynamicTableFactory.Context context,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy,
            @Nullable Long countPushed) {
        super(table, predicate, projectFields, limit);

        this.tableIdentifier = tableIdentifier;
        this.unbounded = unbounded;
        this.context = context;

        this.watermarkStrategy = watermarkStrategy;
        this.countPushed = countPushed;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (!unbounded) {
            // batch merge all, return insert only
            return ChangelogMode.insertOnly();
        }

        if (table.primaryKeys().isEmpty()) {
            return ChangelogMode.insertOnly();
        }

        Options options = Options.fromMap(table.options());

        if (new CoreOptions(options).mergeEngine() == FIRST_ROW) {
            return ChangelogMode.insertOnly();
        }

        if (options.get(SCAN_REMOVE_NORMALIZE)) {
            return ChangelogMode.all();
        }

        if (options.get(CHANGELOG_PRODUCER) != CoreOptions.ChangelogProducer.NONE) {
            return ChangelogMode.all();
        }

        return ChangelogMode.upsert();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        if (countPushed != null) {
            return createCountStarScan();
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
                        .sourceBounded(!unbounded)
                        .projection(projectFields)
                        .predicate(predicate)
                        .partitionPredicate(partitionPredicate)
                        .limit(limit)
                        .watermarkStrategy(watermarkStrategy)
                        .dynamicPartitionFilteringFields(dynamicPartitionFilteringFields());

        return new PaimonDataStreamScanProvider(
                !unbounded,
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
            LookupContext context, FileStoreTable table, int[] projection, int[] joinKey) {
        // 1.Get the join key field names from join key indexes.
        List<String> joinKeyFieldNames =
                Arrays.stream(joinKey)
                        .mapToObj(i -> table.rowType().getFieldNames().get(projection[i]))
                        .collect(Collectors.toList());
        // 2.Get the bucket key field names from bucket key indexes.
        List<String> bucketKeyFieldNames = table.schema().bucketKeys();
        boolean useCustomShuffle =
                supportBucketShufflePartitioner(joinKeyFieldNames, bucketKeyFieldNames)
                        && RuntimeContextUtils.preferCustomShuffle(context);
        int numBuckets;
        ShuffleStrategy strategy = null;
        if (useCustomShuffle) {
            numBuckets = table.store().options().bucket();
            BucketIdExtractor extractor =
                    new BucketIdExtractor(
                            numBuckets, table.schema(), joinKeyFieldNames, bucketKeyFieldNames);

            strategy = new BucketShuffleStrategy(numBuckets);
            bucketShufflePartitioner = new BucketShufflePartitioner(strategy, extractor);
        }

        if (strategy != null) {
            LOG.info("Paimon connector is using bucket shuffle partitioning strategy.");
        }
        return new FileStoreLookupFunction(table, projection, joinKey, predicate, strategy);
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
        if (isUnbounded()) {
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
                table.newReadBuilder()
                        .dropStats()
                        .withProjection(new int[0])
                        .withFilter(predicate)
                        .withPartitionFilter(partitionPredicate)
                        .newScan()
                        .plan()
                        .splits();
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
    public boolean isUnbounded() {
        return unbounded;
    }

    @Override
    public Optional<InputDataPartitioner> getPartitioner() {
        return Optional.ofNullable(bucketShufflePartitioner);
    }

    private boolean supportBucketShufflePartitioner(
            List<String> joinKeyFieldNames, List<String> bucketKeyFieldNames) {
        BucketSpec bucketSpec = ((FileStoreTable) table).bucketSpec();
        return bucketSpec.getBucketMode() == BucketMode.HASH_FIXED
                && new HashSet<>(joinKeyFieldNames).containsAll(bucketKeyFieldNames);
    }
}
