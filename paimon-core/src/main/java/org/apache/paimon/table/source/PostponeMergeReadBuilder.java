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

    @Nullable private Predicate filter;
    @Nullable private PartitionPredicate partitionFilter;
    @Nullable private RowType readType;
    @Nullable private transient MetricRegistry metricRegistry;
    @Nullable private transient String readProtectionTagName;
    private int defaultBucketNum = 1;

    private PostponeMergeReadBuilder(FileStoreTable table) {
        this.table = table;
    }

    /**
     * Creates a builder when the currently selected snapshot and partitions contain postpone files.
     *
     * <p>This presence check only selects the specialized read path. {@link #plan()} resolves the
     * snapshot again, just like an ordinary scan planned later by an execution engine.
     */
    public static Optional<PostponeMergeReadBuilder> create(
            FileStoreTable table, @Nullable PartitionPredicate partitionFilter) {
        checkArgument(
                table.bucketMode() == BucketMode.POSTPONE_MODE && !table.primaryKeys().isEmpty(),
                "Postpone merge read requires a primary-key postpone bucket table.");

        // This mode intentionally selects an older compacted snapshot. Let the ordinary scan use
        // Paimon's FullCompactedStartingScanner instead of resolving the latest snapshot here.
        if (table.coreOptions().startupMode() == CoreOptions.StartupMode.COMPACTED_FULL) {
            return Optional.empty();
        }

        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(table);
        if (snapshot == null) {
            return Optional.empty();
        }

        SnapshotReader postponeReader =
                table.newSnapshotReader()
                        .withSnapshot(snapshot)
                        .withBucket(BucketMode.POSTPONE_BUCKET);
        if (partitionFilter != null) {
            postponeReader.withPartitionFilter(partitionFilter);
        }
        if (!postponeReader.readFileIterator().hasNext()) {
            return Optional.empty();
        }

        validateReadMode(table);
        PostponeMergeReadBuilder builder = new PostponeMergeReadBuilder(table);
        builder.withPartitionFilter(partitionFilter);
        return Optional.of(builder);
    }

    public PostponeMergeReadBuilder withPartitionFilter(
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

    /**
     * Applies the full query predicate.
     *
     * <p>Only predicates which are safe before the real and postpone inputs are merged are pushed
     * into planning and file readers. The execution engine must still evaluate the full predicate
     * after the merge.
     */
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

    /** Installs the planning metric registry used by the real-bucket snapshot scan. */
    public PostponeMergeReadBuilder withMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        return this;
    }

    /** Sets the fallback used when neither the snapshot nor table options define a bucket count. */
    public PostponeMergeReadBuilder withDefaultBucketNum(int defaultBucketNum) {
        checkArgument(defaultBucketNum > 0, "Default postpone bucket number must be positive.");
        this.defaultBucketNum = defaultBucketNum;
        return this;
    }

    /**
     * Plans both real files and postpone-file tasks from one snapshot resolved at planning time.
     */
    public PostponeMergePlan plan() {
        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(table);
        if (snapshot == null) {
            throw new IllegalStateException(
                    "Cannot plan a postpone merge read without a snapshot.");
        }
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
            // A postpone manifest has bucket -2. A non-partition pushdown would install the
            // ordinary positive-bucket selector, so filtering stays in the file and merge readers.
            postponeReader.withFilter(filter, null);
        }
        if (partitionFilter != null) {
            postponeReader.withPartitionFilter(partitionFilter);
        }

        PostponeMergePlan plan =
                new PostponeMergePlan(
                        realReader.read().dataSplits(),
                        PostponeUtils.planPostponeFileReads(postponeReader.read().dataSplits()),
                        PostponeUtils.createPostponeBucketRouter(
                                table, snapshot.id(), defaultBucketNum, partitionFilter),
                        keyType(),
                        resultReadType,
                        mergeReadType);
        maybeCreateReadProtectionTag(snapshot.id());
        return plan;
    }

    /** Returns the temporary tag created while planning, or {@code null} when disabled. */
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
        Set<String> nonPrimaryKeys =
                schema.fieldNames().stream()
                        .filter(name -> !schema.trimmedPrimaryKeys().contains(name))
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
                || startupMode == CoreOptions.StartupMode.FROM_FILE_CREATION_TIME
                || startupMode == CoreOptions.StartupMode.FROM_CREATION_TIMESTAMP) {
            throw new UnsupportedOperationException(
                    "Postpone merge-on-read requires a full snapshot scan, but found scan mode '"
                            + startupMode
                            + "'.");
        }
    }
}
