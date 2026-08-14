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
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PostponeUtils;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.tag.BatchReadTagCreator;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.partition.PartitionPredicate.splitPartitionPredicatesAndDataPredicates;
import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.excludePredicateWithFields;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Builds a postpone merge read for an execution engine. */
public final class PostponeMergeReadBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;
    private final @Nullable Snapshot snapshot;

    @Nullable private Predicate filter;
    @Nullable private PartitionPredicate partitionFilter;
    @Nullable private RowType readType;
    @Nullable private transient MetricRegistry metricRegistry;
    @Nullable private transient String readProtectionTagName;

    private PostponeMergeReadBuilder(FileStoreTable table, @Nullable Snapshot snapshot) {
        this.table = table;
        this.snapshot = snapshot;
    }

    /** Creates a builder for execution-engine supplied postpone and real-bucket splits. */
    public static PostponeMergeReadBuilder createForSplits(FileStoreTable table) {
        checkArgument(
                table.bucketMode() == BucketMode.POSTPONE_MODE && !table.primaryKeys().isEmpty(),
                "Postpone merge read requires a primary-key postpone bucket table.");
        return new PostponeMergeReadBuilder(table, null);
    }

    /** Creates a snapshot-bound builder when the selected partitions contain postpone files. */
    public static Optional<PostponeMergeReadBuilder> create(
            FileStoreTable table, @Nullable PartitionPredicate partitionFilter) {
        checkArgument(
                table.bucketMode() == BucketMode.POSTPONE_MODE && !table.primaryKeys().isEmpty(),
                "Postpone merge read requires a primary-key postpone bucket table.");

        // Let the ordinary compacted-full scanner select its snapshot.
        if (table.coreOptions().startupMode() == CoreOptions.StartupMode.COMPACTED_FULL) {
            return Optional.empty();
        }

        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(table);
        if (snapshot == null) {
            return Optional.empty();
        }

        PostponeMergeReadBuilder builder = new PostponeMergeReadBuilder(table, snapshot);
        builder.withPartitionFilter(partitionFilter);
        if (!builder.hasPostponeFiles()) {
            return Optional.empty();
        }

        validateReadMode(table);
        return Optional.of(builder);
    }

    /**
     * Creates a builder bound to the snapshot selected now, including when that snapshot contains
     * no postpone files.
     *
     * <p>Execution engines which enable merge-on-read should use this method to avoid probing one
     * snapshot and then falling back to an ordinary source which may select a newer snapshot.
     */
    public static Optional<PostponeMergeReadBuilder> createSnapshotBound(
            FileStoreTable table, @Nullable PartitionPredicate partitionFilter) {
        checkArgument(
                table.bucketMode() == BucketMode.POSTPONE_MODE && !table.primaryKeys().isEmpty(),
                "Postpone merge read requires a primary-key postpone bucket table.");

        // Let the ordinary compacted-full scanner select its snapshot.
        if (table.coreOptions().startupMode() == CoreOptions.StartupMode.COMPACTED_FULL) {
            return Optional.empty();
        }

        validateReadMode(table);
        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(table);
        if (snapshot == null) {
            return Optional.empty();
        }

        PostponeMergeReadBuilder builder = new PostponeMergeReadBuilder(table, snapshot);
        builder.withPartitionFilter(partitionFilter);
        return Optional.of(builder);
    }

    private boolean hasPostponeFiles() {
        SnapshotReader postponeReader =
                table.newSnapshotReader()
                        .withSnapshot(snapshot)
                        .withBucket(BucketMode.POSTPONE_BUCKET);
        if (partitionFilter != null) {
            postponeReader.withPartitionFilter(partitionFilter);
        }
        return postponeReader.readFileIterator().hasNext();
    }

    private PostponeMergeReadBuilder withPartitionFilter(
            @Nullable PartitionPredicate partitionPredicate) {
        if (partitionPredicate == null) {
            return this;
        }
        partitionFilter =
                partitionFilter == null
                        ? partitionPredicate
                        : PartitionPredicate.and(
                                Arrays.asList(partitionFilter, partitionPredicate));
        return this;
    }

    /** Applies safe pushdowns; the execution engine must evaluate the predicate after merging. */
    public PostponeMergeReadBuilder withFilter(@Nullable Predicate predicate) {
        if (predicate == null) {
            return this;
        }
        filter = filter == null ? predicate : PredicateBuilder.and(filter, predicate);
        splitPartitionPredicatesAndDataPredicates(predicate, table.rowType(), table.partitionKeys())
                .getLeft()
                .ifPresent(this::withPartitionFilter);
        return this;
    }

    public PostponeMergeReadBuilder withReadType(RowType readType) {
        this.readType = readType;
        return this;
    }

    public PostponeMergeReadBuilder withMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        return this;
    }

    public PostponeMergePlan plan() {
        checkArgument(snapshot != null, "Snapshot-bound postpone merge plan requires a snapshot.");
        RowType resultReadType = resultReadType();
        RowType mergeReadType = mergeReadType(resultReadType);

        SnapshotReader realReader =
                table.newSnapshotReader()
                        .withSnapshot(snapshot)
                        .onlyReadRealBuckets()
                        .withReadType(resultReadType);
        if (metricRegistry != null) {
            realReader.withMetricRegistry(metricRegistry);
        }
        if (filter != null) {
            realReader.withFilter(filter, safeKeyPredicate(table.schema(), filter));
        }
        if (partitionFilter != null) {
            realReader.withPartitionFilter(partitionFilter);
        }

        SnapshotReader postponeReader =
                table.newSnapshotReader()
                        .withSnapshot(snapshot)
                        .withBucket(BucketMode.POSTPONE_BUCKET);
        if (filter != null) {
            // A normal bucket selector cannot prune bucket -2.
            postponeReader.withFilter(filter, null);
        }
        if (partitionFilter != null) {
            postponeReader.withPartitionFilter(partitionFilter);
        }

        List<DataSplit> realSplits = realReader.read().dataSplits();
        List<DataSplit> postponeSplits =
                PostponeUtils.groupPostponeFiles(postponeReader.read().dataSplits());
        PostponeUtils.PostponeBucketRouter bucketRouter;
        if (postponeSplits.isEmpty()) {
            bucketRouter = PostponeUtils.createPostponeBucketRouter(table, Collections.emptyMap());
        } else {
            List<BinaryRow> postponePartitions =
                    postponeSplits.stream()
                            .map(DataSplit::partition)
                            .distinct()
                            .collect(Collectors.toList());
            bucketRouter =
                    PostponeUtils.createPostponeBucketRouter(
                            table, snapshot.id(), postponePartitions);
        }

        PostponeMergePlan plan =
                new PostponeMergePlan(
                        realSplits,
                        postponeSplits,
                        bucketRouter,
                        keyType(),
                        resultReadType,
                        mergeReadType);
        maybeCreateReadProtectionTag(snapshot.id());
        return plan;
    }

    /** Builds a plan from splits and routing metadata supplied by an execution engine. */
    public PostponeMergePlan plan(
            List<DataSplit> realSplits,
            List<DataSplit> postponeSplits,
            PostponeUtils.PostponeBucketRouter bucketRouter) {
        RowType resultReadType = resultReadType();
        return new PostponeMergePlan(
                realSplits,
                PostponeUtils.groupPostponeFiles(postponeSplits),
                bucketRouter,
                keyType(),
                resultReadType,
                mergeReadType(resultReadType));
    }

    @Nullable
    public String readProtectionTagName() {
        return readProtectionTagName;
    }

    public PostponeMergeRead newRead() {
        RowType resultReadType = resultReadType();
        return new PostponeMergeRead(table, filter, resultReadType, mergeReadType(resultReadType));
    }

    private RowType resultReadType() {
        return readType == null ? table.rowType() : readType;
    }

    private RowType mergeReadType(RowType resultReadType) {
        return ((KeyValueFileStore) table.store()).newRead().adjustReadType(resultReadType);
    }

    private RowType keyType() {
        return new RowType(
                PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR.keyFields(table.schema()));
    }

    private void maybeCreateReadProtectionTag(long snapshotId) {
        if (table.coreOptions().scanPlanAutoTagTimeRetained() == null) {
            return;
        }
        BatchReadTagCreator creator =
                new BatchReadTagCreator(
                        table.tagManager(),
                        table.snapshotManager(),
                        table.coreOptions().scanPlanAutoTagTimeRetained());
        readProtectionTagName = creator.createReadTag(snapshotId);
    }

    @Nullable
    private static Predicate safeKeyPredicate(TableSchema schema, Predicate predicate) {
        Pair<Optional<PartitionPredicate>, List<Predicate>> split =
                splitPartitionPredicatesAndDataPredicates(
                        predicate, schema.logicalRowType(), schema.partitionKeys());
        List<String> primaryKeys = schema.trimmedPrimaryKeys();
        Set<String> nonPrimaryKeys =
                schema.fieldNames().stream()
                        .filter(name -> !primaryKeys.contains(name))
                        .collect(Collectors.toSet());
        List<Predicate> keyFilters = excludePredicateWithFields(split.getRight(), nonPrimaryKeys);
        return keyFilters.isEmpty() ? null : and(keyFilters);
    }

    private static void validateReadMode(FileStoreTable table) {
        if (table.coreOptions().queryAuthEnabled()) {
            throw new UnsupportedOperationException(
                    "Postpone merge-on-read does not support query authorization.");
        }
        CoreOptions.StartupMode startupMode = table.coreOptions().startupMode();
        if (startupMode == CoreOptions.StartupMode.INCREMENTAL
                || startupMode == CoreOptions.StartupMode.LATEST_DELTA
                || startupMode == CoreOptions.StartupMode.FROM_FILE_CREATION_TIME
                || startupMode == CoreOptions.StartupMode.FROM_CREATION_TIMESTAMP) {
            throw new UnsupportedOperationException(
                    "Postpone merge-on-read requires a full snapshot scan, but found scan mode '"
                            + startupMode
                            + "'.");
        }
    }
}
