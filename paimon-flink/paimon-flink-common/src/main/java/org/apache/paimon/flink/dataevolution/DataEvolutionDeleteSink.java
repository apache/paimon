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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Writes deletion vectors for row ids of a Data Evolution table. */
public class DataEvolutionDeleteSink implements Serializable {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;
    private final long baseSnapshotId;
    private final int sinkParallelism;

    public DataEvolutionDeleteSink(FileStoreTable table, long baseSnapshotId, int sinkParallelism) {
        validateTable(table);
        Preconditions.checkArgument(
                sinkParallelism > 0,
                "Sink parallelism must be a positive integer, but is %s.",
                sinkParallelism);
        this.table =
                baseSnapshotId == DataEvolutionRowLevelModificationScanContext.EMPTY_TABLE_SNAPSHOT
                        ? table
                        : (FileStoreTable)
                                table.copy(
                                        Collections.singletonMap(
                                                CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT
                                                        .key(),
                                                String.valueOf(baseSnapshotId)));
        this.baseSnapshotId = baseSnapshotId;
        this.sinkParallelism = sinkParallelism;
    }

    public DataStreamSink<?> sinkFrom(DataStream<Long> rowIds) {
        if (baseSnapshotId == DataEvolutionRowLevelModificationScanContext.EMPTY_TABLE_SNAPSHOT) {
            return rowIds.sinkTo(new DiscardingSink<>()).name("END").setParallelism(1);
        }

        List<AnchorRange> anchorRanges = planAnchorRanges();
        DataStream<DeletionTarget> targets =
                rowIds.rebalance()
                        .map(
                                new RowIdToDeletionTarget(anchorRanges),
                                TypeInformation.of(DeletionTarget.class))
                        .setParallelism(sinkParallelism)
                        .partitionCustom(new StringHashPartitioner(), new AnchorKeySelector());

        DataStream<DeletionVectorUpdate> deletionVectorUpdates =
                targets.transform(
                                "AGGREGATE DELETION VECTORS",
                                TypeInformation.of(DeletionVectorUpdate.class),
                                new DeletionVectorAggregator(
                                        table.coreOptions().deletionVectorBitmap64()))
                        .setParallelism(sinkParallelism)
                        .partitionCustom(
                                new StringHashPartitioner(), new RewriteGroupKeySelector());

        DataStream<Committable> written =
                deletionVectorUpdates
                        .transform(
                                "WRITE DELETION VECTORS",
                                new CommittableTypeInfo(),
                                new DataEvolutionDeleteOperator(table, baseSnapshotId))
                        .setParallelism(sinkParallelism);

        String commitUser = CoreOptions.createCommitUser(table.coreOptions().toConfiguration());
        Snapshot baseSnapshot = table.snapshotManager().snapshot(baseSnapshotId);
        String baseSnapshotUuid = baseSnapshot != null ? baseSnapshot.uuid() : null;
        CommitterOperatorFactory<Committable, ManifestCommittable> committerOperator =
                new CommitterOperatorFactory<>(
                        false,
                        true,
                        commitUser,
                        context ->
                                new StoreCommitter(
                                        table,
                                        table.newCommit(context.commitUser())
                                                .withOperation(Snapshot.Operation.DELETE)
                                                .rowIdCheckConflict(
                                                        baseSnapshotId, baseSnapshotUuid),
                                        context),
                        new NoopCommittableStateManager());

        DataStream<Committable> committed =
                written.transform("COMMIT OPERATOR", new CommittableTypeInfo(), committerOperator)
                        .setParallelism(1)
                        .setMaxParallelism(1);

        DataStreamSink<Committable> end =
                committed.sinkTo(new DiscardingSink<>()).name("END").setParallelism(1);
        end.getTransformation().setMaxParallelism(1);
        return end;
    }

    public static void validateTable(FileStoreTable table) {
        CoreOptions coreOptions = table.coreOptions();
        if (!table.schema().primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Data-evolution delete only supports append tables without primary keys.");
        }
        if (!coreOptions.rowTrackingEnabled()) {
            throw new UnsupportedOperationException(
                    "Data-evolution delete requires row-tracking.enabled to be true.");
        }
        if (!coreOptions.dataEvolutionEnabled()) {
            throw new UnsupportedOperationException(
                    "Data-evolution delete requires data-evolution.enabled to be true.");
        }
        if (!coreOptions.deletionVectorsEnabled()) {
            throw new UnsupportedOperationException(
                    "Data-evolution delete requires deletion-vectors.enabled to be true.");
        }
        if (table.bucketMode() != BucketMode.BUCKET_UNAWARE) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Data-evolution delete only supports unaware bucket mode, but table bucket mode is %s.",
                            table.bucketMode()));
        }
    }

    private List<AnchorRange> planAnchorRanges() {
        List<AnchorRange> anchorRanges = new ArrayList<>();
        for (DataSplit split :
                table.newSnapshotReader().withSnapshot(baseSnapshotId).read().dataSplits()) {
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
                String oldIndexFileName = oldIndexFileByDataFile.get(anchor.fileName());
                anchorRanges.add(
                        new AnchorRange(
                                range.from,
                                range.to,
                                rewriteGroup(
                                        split.bucketPath(),
                                        oldIndexFileName,
                                        anchorFilePath,
                                        sinkParallelism),
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

    @VisibleForTesting
    public static String rewriteGroup(
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
