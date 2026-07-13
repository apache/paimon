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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.pk.PrimaryKeyIndexDefinition;
import org.apache.paimon.index.pk.PrimaryKeyIndexDefinitions;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StartingScanner.ScannedResult;
import org.apache.paimon.tag.BatchReadTagCreator;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.apache.paimon.table.source.PushDownUtils.minmaxAvailable;

/** {@link TableScan} implementation for batch planning. */
public class DataTableBatchScan extends AbstractDataTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(DataTableBatchScan.class);

    /** Validates {@link CoreOptions#SCAN_BUCKET} for primary-key fixed-bucket tables. */
    public static void validateScanBucketOption(
            TableSchema schema, CoreOptions coreOptions, int bucket) {
        AbstractDataTableScan.validateScanBucketOption(schema, coreOptions, bucket);
    }

    private StartingScanner startingScanner;
    private boolean hasNext;

    private Integer pushDownLimit;
    private TopN topN;

    private final SchemaManager schemaManager;
    @Nullable private String readProtectionTagName;
    @Nullable private GlobalIndexSplitResult globalIndexSplitResult;
    @Nullable private Predicate filter;

    @Nullable
    private final PrimaryKeySortedIndexScan.ReaderFactory primaryKeySortedIndexReaderFactory;

    public DataTableBatchScan(
            TableSchema schema,
            SchemaManager schemaManager,
            CoreOptions options,
            SnapshotReader snapshotReader,
            TableQueryAuth queryAuth) {
        this(schema, schemaManager, options, snapshotReader, queryAuth, null);
    }

    DataTableBatchScan(
            TableSchema schema,
            SchemaManager schemaManager,
            CoreOptions options,
            SnapshotReader snapshotReader,
            TableQueryAuth queryAuth,
            @Nullable PrimaryKeySortedIndexScan.ReaderFactory primaryKeySortedIndexReaderFactory) {
        super(schema, options, snapshotReader, queryAuth);

        this.hasNext = true;
        this.schemaManager = schemaManager;
        this.primaryKeySortedIndexReaderFactory = primaryKeySortedIndexReaderFactory;
        if (!schema.primaryKeys().isEmpty() && options.batchScanSkipLevel0()) {
            if (options.toConfiguration()
                    .get(CoreOptions.BATCH_SCAN_MODE)
                    .equals(CoreOptions.BatchScanMode.NONE)) {
                snapshotReader.withLevelFilter(level -> level > 0).enableValueFilter();
            }
        }
        if (options.bucket() == BucketMode.POSTPONE_BUCKET) {
            snapshotReader.onlyReadRealBuckets();
        }
    }

    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        this.filter = predicate;
        super.withFilter(predicate);
        return this;
    }

    @Override
    public InnerTableScan withLimit(int limit) {
        // Record it; applyPushDownLimit pushes the file-store limit only when safe.
        this.pushDownLimit = limit;
        return this;
    }

    @Override
    public InnerTableScan withTopN(TopN topN) {
        this.topN = topN;
        return this;
    }

    @Override
    protected TableScan.Plan planWithoutAuth() {
        if (globalIndexSplitResult != null) {
            if (!hasNext) {
                throw new EndOfScanException();
            }
            hasNext = false;
            if (globalIndexSplitResult.snapshotId() > 0) {
                maybeCreateReadProtectionTag(globalIndexSplitResult.snapshotId());
            }
            List<Split> splits = new ArrayList<>(globalIndexSplitResult.splits());
            return new PlanImpl(null, globalIndexSplitResult.snapshotId(), splits);
        }
        if (startingScanner == null) {
            startingScanner = createStartingScanner(false);
        }

        if (hasNext) {
            hasNext = false;
            StartingScanner.Result result;
            Optional<StartingScanner.Result> pushed = applyPushDownLimit();
            if (pushed.isPresent()) {
                result = pushed.get();
            } else {
                pushed = applyPushDownTopN();
                result = pushed.orElseGet(() -> startingScanner.scan(snapshotReader));
            }

            if (result instanceof ScannedResult) {
                maybeCreateReadProtectionTag(((ScannedResult) result).currentSnapshotId());
            }

            TableScan.Plan dataPlan = DataFilePlan.fromResult(result);
            if (result instanceof ScannedResult) {
                return applyPrimaryKeySortedIndexes(((ScannedResult) result).plan());
            }
            return dataPlan;
        } else {
            throw new EndOfScanException();
        }
    }

    private TableScan.Plan applyPrimaryKeySortedIndexes(SnapshotReader.Plan dataPlan) {
        if (filter == null
                || !snapshotReader.hasNonPartitionFilter()
                || schema.primaryKeys().isEmpty()
                || !options().deletionVectorsEnabled()
                || options().deletionVectorsMergeOnRead()
                || options().bucket() <= 0
                || dataPlan.snapshotId() == null
                || dataPlan.splits().isEmpty()) {
            return dataPlan;
        }

        List<DataSplit> dataSplits = new ArrayList<>();
        for (Split split : dataPlan.splits()) {
            if (!(split instanceof DataSplit) || ((DataSplit) split).isStreaming()) {
                return dataPlan;
            }
            dataSplits.add((DataSplit) split);
        }

        try {
            long snapshotId = dataPlan.snapshotId();
            Snapshot snapshot = snapshotReader.snapshotManager().snapshot(snapshotId);
            if (snapshot == null) {
                return dataPlan;
            }
            TableSchema snapshotSchema = schemaManager.schema(snapshot.schemaId());
            List<PrimaryKeyIndexDefinition> definitions =
                    PrimaryKeyIndexDefinitions.create(snapshotSchema).definitions();
            Set<Integer> scalarFields = new HashSet<>();
            for (PrimaryKeyIndexDefinition definition : definitions) {
                if (definition.family() == PrimaryKeyIndexDefinition.Family.BTREE
                        || definition.family() == PrimaryKeyIndexDefinition.Family.BITMAP) {
                    scalarFields.add(definition.fieldId());
                }
            }
            if (scalarFields.isEmpty()) {
                return dataPlan;
            }

            IndexFileHandler indexFileHandler = snapshotReader.indexFileHandler();
            if (indexFileHandler == null) {
                return dataPlan;
            }
            List<IndexManifestEntry> indexEntries =
                    indexFileHandler.scan(
                            snapshot,
                            entry -> {
                                GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
                                return entry.kind() == FileKind.ADD
                                        && meta != null
                                        && meta.sourceMeta() != null
                                        && scalarFields.contains(meta.indexFieldId());
                            });
            PrimaryKeySortedIndexScan.Plan indexPlan =
                    PrimaryKeySortedIndexScan.plan(
                            snapshotId, dataSplits, definitions, indexEntries);
            PrimaryKeySortedIndexScan.ReaderFactory readerFactory =
                    primaryKeySortedIndexReaderFactory == null
                            ? PrimaryKeySortedIndexScan.readerFactory(
                                    snapshotReader.snapshotManager().fileIO(),
                                    snapshotReader.pathFactory(),
                                    snapshotSchema.logicalRowType(),
                                    options().toConfiguration())
                            : primaryKeySortedIndexReaderFactory;
            PrimaryKeySortedIndexScan.EvaluatedPlan evaluated =
                    PrimaryKeySortedIndexScan.evaluate(
                            indexPlan,
                            snapshotSchema.logicalRowType(),
                            filter,
                            definitions,
                            readerFactory);
            PrimaryKeySortedIndexResult result = new PrimaryKeySortedIndexResult(evaluated);
            return new PlanImpl(
                    dataPlan.watermark(), dataPlan.snapshotId(), new ArrayList<>(result.splits()));
        } catch (RuntimeException e) {
            if (Thread.currentThread().isInterrupted()) {
                throw e;
            }
            LOG.warn(
                    "Failed to apply primary-key sorted indexes; using the ordinary data plan.", e);
            return dataPlan;
        }
    }

    @Override
    public DataTableBatchScan withGlobalIndexResult(GlobalIndexResult globalIndexResult) {
        if (globalIndexResult instanceof GlobalIndexSplitResult) {
            this.globalIndexSplitResult = (GlobalIndexSplitResult) globalIndexResult;
        }
        return this;
    }

    @Override
    public List<PartitionEntry> listPartitionEntries() {
        if (startingScanner == null) {
            startingScanner = createStartingScanner(false);
        }
        return startingScanner.scanPartitions(snapshotReader);
    }

    private Optional<StartingScanner.Result> applyPushDownLimit() {
        // A read-time filter (WHERE or auth) drops rows after scanning, so only push the limit down
        // when neither is present.
        if (pushDownLimit == null
                || snapshotReader.hasNonPartitionFilter()
                || authHasNonPartitionFilter) {
            return Optional.empty();
        }
        snapshotReader.withLimit(pushDownLimit);

        StartingScanner.Result result = startingScanner.scan(snapshotReader);
        if (!(result instanceof ScannedResult)) {
            return Optional.of(result);
        }

        long scannedRowCount = 0;
        SnapshotReader.Plan plan = ((ScannedResult) result).plan();
        List<Split> splits = plan.splits();
        if (splits.isEmpty()) {
            return Optional.of(result);
        }

        LOG.info("Applying limit pushdown. Original splits count: {}", splits.size());
        List<Split> limitedSplits = new ArrayList<>();
        for (Split split : splits) {
            OptionalLong mergedRowCount = split.mergedRowCount();
            if (mergedRowCount.isPresent()) {
                limitedSplits.add(split);
                scannedRowCount += mergedRowCount.getAsLong();
                if (scannedRowCount >= pushDownLimit) {
                    SnapshotReader.Plan newPlan =
                            new PlanImpl(plan.watermark(), plan.snapshotId(), limitedSplits);
                    LOG.info(
                            "Limit pushdown applied successfully. Original splits: {}, Limited splits: {}, Pushdown limit: {}",
                            splits.size(),
                            limitedSplits.size(),
                            pushDownLimit);
                    return Optional.of(new ScannedResult(newPlan));
                }
            }
        }
        return Optional.of(result);
    }

    private Optional<StartingScanner.Result> applyPushDownTopN() {
        // A read-time filter (WHERE or auth) drops rows after split pruning, so split-level TopN
        // pruning could keep too few splits. Skip it when either is present.
        if (topN == null
                || pushDownLimit != null
                || snapshotReader.hasNonPartitionFilter()
                || authHasNonPartitionFilter
                || !schema.primaryKeys().isEmpty()) {
            return Optional.empty();
        }

        List<SortValue> orders = topN.orders();
        if (orders.size() != 1) {
            return Optional.empty();
        }

        if (topN.limit() > 100) {
            return Optional.empty();
        }

        SortValue order = orders.get(0);
        DataType type = order.field().type();
        if (!minmaxAvailable(type)) {
            return Optional.empty();
        }

        StartingScanner.Result result = startingScanner.scan(snapshotReader.keepStats());
        if (!(result instanceof ScannedResult)) {
            return Optional.of(result);
        }

        SnapshotReader.Plan plan = ((ScannedResult) result).plan();
        List<Split> splits = plan.splits();
        if (splits.isEmpty()) {
            return Optional.of(result);
        }

        TopNDataSplitEvaluator evaluator = new TopNDataSplitEvaluator(schema, schemaManager);
        List<Split> topNSplits = new ArrayList<>(evaluator.evaluate(order, topN.limit(), splits));
        SnapshotReader.Plan newPlan = new PlanImpl(plan.watermark(), plan.snapshotId(), topNSplits);
        return Optional.of(new ScannedResult(newPlan));
    }

    @Override
    public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        snapshotReader.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
        return this;
    }

    @Override
    @Nullable
    public String readProtectionTagName() {
        return readProtectionTagName;
    }

    private void maybeCreateReadProtectionTag(long snapshotId) {
        Duration timeRetained = options().scanPlanAutoTagTimeRetained();
        if (timeRetained == null) {
            return;
        }
        SnapshotManager sm = snapshotReader.snapshotManager();
        TagManager tagMgr = new TagManager(sm.fileIO(), sm.tablePath(), sm.branch());
        BatchReadTagCreator creator = new BatchReadTagCreator(tagMgr, sm, timeRetained);
        this.readProtectionTagName = creator.createReadTag(snapshotId);
    }
}
