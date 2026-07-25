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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.dataevolution.DataEvolutionDeleteOperator;
import org.apache.paimon.flink.dataevolution.DataEvolutionDeleteOperator.DeletionTarget;
import org.apache.paimon.flink.dataevolution.DataEvolutionDeleteOperator.DeletionVectorAggregator;
import org.apache.paimon.flink.dataevolution.DataEvolutionDeleteOperator.DeletionVectorUpdate;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.CommitterOperatorFactory;
import org.apache.paimon.flink.sink.NoopCommittableStateManager;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.operation.DataEvolutionSplitRead;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.DataEvolutionUtils;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Internal implementation which logically deletes rows from a data-evolution append table.
 *
 * <p>The action evaluates a Flink SQL filter against a fixed row-tracking snapshot, maps every
 * matched {@code _ROW_ID} to its data-evolution anchor file, and commits deletion-vector index
 * files. Optional source SQL statements can register bounded external tables used by subqueries in
 * the filter. Existing data and BLOB files are not rewritten by this action.
 *
 * <p>Only one instance of this action should run against the same table at a time. The fixed base
 * snapshot and strict commit mode detect conflicting commits, including append, compaction, and
 * overwrite, instead of silently applying deletion vectors to a stale row-id mapping.
 *
 * <p>The current implementation plans anchor ranges on the coordinator. Row positions are first
 * aggregated per anchor and then shuffled by rewrite group. Anchors backed by the same existing
 * deletion-vector index file always have one writer owner; anchors without existing deletion
 * vectors are split into stable shards. Large deletes should still be split into bounded batches to
 * limit coordinator and deletion-vector memory usage.
 */
class DataEvolutionDelete implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionDelete.class);

    private final DeleteAction action;
    private final String filter;
    private final long baseSnapshotId;

    private int sinkParallelism = 1;

    DataEvolutionDelete(DeleteAction action, String filter) {
        this.action = action;
        Preconditions.checkArgument(
                filter != null && !filter.trim().isEmpty(),
                "Deletion filter must not be null or blank.");
        this.filter = filter;

        if (!(action.table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports Data Evolution delete. The table type is '%s'.",
                            action.table.getClass().getName()));
        }

        FileStoreTable storeTable = (FileStoreTable) action.table;
        Long latestSnapshotId = storeTable.snapshotManager().latestSnapshotId();
        if (latestSnapshotId == null) {
            throw new UnsupportedOperationException(
                    "Data-evolution delete action doesn't support deleting from an empty table.");
        }
        this.baseSnapshotId = latestSnapshotId;

        CoreOptions coreOptions = storeTable.coreOptions();
        if (!storeTable.schema().primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Data-evolution delete action only supports append tables without primary keys.");
        }
        if (!coreOptions.rowTrackingEnabled()) {
            throw new UnsupportedOperationException(
                    "Data-evolution delete action requires row-tracking.enabled to be true.");
        }
        if (!coreOptions.dataEvolutionEnabled()) {
            throw new UnsupportedOperationException(
                    "Data-evolution delete action requires data-evolution.enabled to be true.");
        }
        if (!coreOptions.deletionVectorsEnabled()) {
            throw new UnsupportedOperationException(
                    "Data-evolution delete action requires deletion-vectors.enabled to be true.");
        }
        if (storeTable.bucketMode() != BucketMode.BUCKET_UNAWARE) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Data-evolution delete action only supports unaware bucket mode, but table bucket mode is %s.",
                            storeTable.bucketMode()));
        }

        action.table =
                action.table.copy(
                        Collections.singletonMap(
                                CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key(),
                                latestSnapshotId.toString()));
    }

    DataEvolutionDelete withSinkParallelism(int sinkParallelism) {
        Preconditions.checkArgument(
                sinkParallelism > 0,
                "Sink parallelism must be a positive integer, but is %s.",
                sinkParallelism);
        this.sinkParallelism = sinkParallelism;
        return this;
    }

    /** Builds and executes the Flink batch topology. */
    TableResult runInternal() {
        FileStoreTable storeTable = (FileStoreTable) action.table;
        List<AnchorRange> anchorRanges = planAnchorRanges(storeTable);
        String commitUser =
                CoreOptions.createCommitUser(storeTable.coreOptions().toConfiguration());

        String query =
                String.format(
                        "SELECT `_ROW_ID` FROM `%s`.`%s`.`%s$row_tracking` "
                                + "/*+ OPTIONS('scan.snapshot-id'='%d') */ WHERE %s",
                        action.catalogName,
                        action.identifier.getDatabaseName(),
                        action.identifier.getObjectName(),
                        baseSnapshotId,
                        filter);
        LOG.info("Data-evolution delete source query: {}", query);

        Table matchedRows = action.batchTEnv.sqlQuery(query);
        DataStream<Long> rowIds =
                action.batchTEnv
                        .toDataStream(matchedRows)
                        .map(
                                (MapFunction<Row, Long>) row -> (Long) row.getField(0),
                                TypeInformation.of(Long.class));

        DataStream<DeletionTarget> targets =
                rowIds.rebalance()
                        .map(
                                new RowIdToDeletionTarget(anchorRanges),
                                TypeInformation.of(DeletionTarget.class))
                        // Anchor ranges are part of the mapper closure. Bound the number of copies
                        // by the configured sink parallelism instead of the source scan
                        // parallelism.
                        .setParallelism(sinkParallelism)
                        .partitionCustom(new StringHashPartitioner(), new AnchorKeySelector());

        DataStream<DeletionVectorUpdate> deletionVectorUpdates =
                targets.transform(
                                "AGGREGATE DELETION VECTORS",
                                TypeInformation.of(DeletionVectorUpdate.class),
                                new DeletionVectorAggregator(
                                        storeTable.coreOptions().deletionVectorBitmap64()))
                        .setParallelism(sinkParallelism)
                        .partitionCustom(
                                new StringHashPartitioner(), new RewriteGroupKeySelector());

        DataStream<Committable> written =
                deletionVectorUpdates
                        .transform(
                                "WRITE DELETION VECTORS",
                                new CommittableTypeInfo(),
                                new DataEvolutionDeleteOperator(storeTable, baseSnapshotId))
                        .setParallelism(sinkParallelism);

        CommitterOperatorFactory<Committable, ManifestCommittable> committerOperator =
                new CommitterOperatorFactory<>(
                        false,
                        true,
                        commitUser,
                        context ->
                                new StoreCommitter(
                                        storeTable,
                                        storeTable
                                                .newCommit(context.commitUser())
                                                .withOperation(Snapshot.Operation.DELETE)
                                                .rowIdCheckConflict(baseSnapshotId),
                                        context),
                        new NoopCommittableStateManager());

        DataStream<Committable> committed =
                written.transform("COMMIT OPERATOR", new CommittableTypeInfo(), committerOperator)
                        .setParallelism(1)
                        .setMaxParallelism(1);

        Transformation<?> end =
                committed
                        .sinkTo(new DiscardingSink<>())
                        .name("END")
                        .setParallelism(1)
                        .getTransformation();

        return action.executeInternal(
                Collections.singletonList(end),
                Collections.singletonList(action.identifier.getFullName()));
    }

    private List<AnchorRange> planAnchorRanges(FileStoreTable storeTable) {
        List<AnchorRange> anchorRanges = new ArrayList<>();
        for (DataSplit split :
                storeTable.newSnapshotReader().withSnapshot(baseSnapshotId).read().dataSplits()) {
            Map<String, String> oldIndexFileByDataFile = new HashMap<>();
            if (split.deletionFiles().isPresent()) {
                List<DeletionFile> deletionFiles = split.deletionFiles().get();
                Preconditions.checkState(
                        deletionFiles.size() == split.dataFiles().size(),
                        "Deletion files and data files have different sizes in bucket path %s.",
                        split.bucketPath());
                for (int i = 0; i < deletionFiles.size(); i++) {
                    DeletionFile deletionFile = deletionFiles.get(i);
                    if (deletionFile != null) {
                        oldIndexFileByDataFile.put(
                                split.dataFiles().get(i).fileName(),
                                new Path(deletionFile.path()).getName());
                    }
                }
            }

            for (List<DataFileMeta> group :
                    DataEvolutionSplitRead.mergeRangesAndSort(split.dataFiles())) {
                DataFileMeta anchor = DataEvolutionUtils.retrieveAnchorFile(group, file -> file);
                Range range = anchor.nonNullRowIdRange();
                String anchorFilePath =
                        anchor.externalPath().isPresent()
                                ? anchor.externalPath().get()
                                : split.bucketPath() + "/" + anchor.fileName();
                String rewriteGroup =
                        rewriteGroup(
                                split.bucketPath(),
                                oldIndexFileByDataFile.get(anchor.fileName()),
                                anchorFilePath,
                                sinkParallelism);
                String oldIndexFileName = oldIndexFileByDataFile.get(anchor.fileName());
                anchorRanges.add(
                        new AnchorRange(
                                range.from,
                                range.to,
                                rewriteGroup,
                                split.bucketPath(),
                                oldIndexFileName,
                                SerializationUtils.serializeBinaryRow(split.partition()),
                                anchorFilePath));
            }
        }

        anchorRanges.sort(Comparator.comparingLong(range -> range.from));
        Preconditions.checkState(
                !anchorRanges.isEmpty(),
                "Cannot find data-evolution anchor files in snapshot %s.",
                baseSnapshotId);
        for (int i = 1; i < anchorRanges.size(); i++) {
            AnchorRange previous = anchorRanges.get(i - 1);
            AnchorRange current = anchorRanges.get(i);
            Preconditions.checkState(
                    previous.to < current.from,
                    "Data-evolution anchor ranges overlap: [%s, %s] and [%s, %s].",
                    previous.from,
                    previous.to,
                    current.from,
                    current.to);
        }
        return anchorRanges;
    }

    /**
     * Returns the ownership key for rewriting a deletion-vector index file.
     *
     * <p>An existing index file is the atomic rewrite unit because it may contain deletion vectors
     * for multiple anchors. New anchors have no shared old file and can therefore be distributed
     * over stable shards.
     */
    @VisibleForTesting
    static String rewriteGroup(
            String bucketPath,
            @Nullable String oldIndexFile,
            String anchorFilePath,
            int parallelism) {
        if (oldIndexFile != null) {
            return bucketPath + "\u0000old\u0000" + oldIndexFile;
        }
        int shard = Math.floorMod(anchorFilePath.hashCode(), parallelism);
        return bucketPath + "\u0000new\u0000" + shard;
    }

    /** A data-evolution anchor file and its covered global row-id range. */
    private static class AnchorRange implements Serializable {

        private static final long serialVersionUID = 1L;

        private final long from;
        private final long to;
        private final String rewriteGroup;
        private final String bucketPath;
        @Nullable private final String oldIndexFileName;
        private final byte[] serializedPartition;
        private final String dataFilePath;

        private AnchorRange(
                long from,
                long to,
                String rewriteGroup,
                String bucketPath,
                @Nullable String oldIndexFileName,
                byte[] serializedPartition,
                String dataFilePath) {
            this.from = from;
            this.to = to;
            this.rewriteGroup = rewriteGroup;
            this.bucketPath = bucketPath;
            this.oldIndexFileName = oldIndexFileName;
            this.serializedPartition = serializedPartition;
            this.dataFilePath = dataFilePath;
        }
    }

    /** Maps a global row id to its anchor data file and local deletion-vector position. */
    private static class RowIdToDeletionTarget implements MapFunction<Long, DeletionTarget> {

        private static final long serialVersionUID = 1L;

        private final List<AnchorRange> anchorRanges;

        private RowIdToDeletionTarget(List<AnchorRange> anchorRanges) {
            this.anchorRanges = anchorRanges;
        }

        @Override
        public DeletionTarget map(Long rowId) {
            int low = 0;
            int high = anchorRanges.size() - 1;
            int candidate = -1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                if (anchorRanges.get(mid).from <= rowId) {
                    candidate = mid;
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }

            if (candidate < 0 || rowId > anchorRanges.get(candidate).to) {
                throw new IllegalStateException(
                        String.format(
                                "Cannot find data-evolution deletion-vector anchor range for row id %s.",
                                rowId));
            }

            AnchorRange anchor = anchorRanges.get(candidate);
            return new DeletionTarget(
                    anchor.rewriteGroup,
                    anchor.bucketPath,
                    anchor.oldIndexFileName,
                    anchor.serializedPartition,
                    anchor.dataFilePath,
                    rowId - anchor.from);
        }
    }

    private static class AnchorKeySelector implements KeySelector<DeletionTarget, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(DeletionTarget value) {
            return value.getBucketPath() + "\u0000" + value.getDataFilePath();
        }
    }

    private static class RewriteGroupKeySelector
            implements KeySelector<DeletionVectorUpdate, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(DeletionVectorUpdate value) {
            return value.getRewriteGroup();
        }
    }

    private static class StringHashPartitioner implements Partitioner<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public int partition(String key, int numPartitions) {
            return Math.floorMod(key.hashCode(), numPartitions);
        }
    }
}
