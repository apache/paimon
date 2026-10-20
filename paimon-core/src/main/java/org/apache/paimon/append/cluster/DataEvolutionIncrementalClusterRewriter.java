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

package org.apache.paimon.append.cluster;

import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactDeletionVectorRewriter;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.utils.RangeHelper;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Preserves complete row-id groups and materializes deletion vectors for clustering. */
class DataEvolutionIncrementalClusterRewriter extends IncrementalClusterRewriter {

    private final FileStoreTable table;
    private final Snapshot snapshot;

    DataEvolutionIncrementalClusterRewriter(FileStoreTable table, Snapshot snapshot) {
        this.table = table;
        this.snapshot = snapshot;
    }

    @Override
    List<LevelSortedRun> constructPartitionLevels(List<DataFileMeta> files) {
        checkArgument(
                files.stream()
                        .noneMatch(
                                file ->
                                        isBlobFile(file.fileName())
                                                || isVectorStoreFile(file.fileName())),
                "Data evolution incremental clustering does not support dedicated BLOB/VECTOR files.");
        RangeHelper<DataFileMeta> helper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        List<LevelSortedRun> runs = new ArrayList<>();
        Map<Integer, List<DataFileMeta>> higherLevels = new HashMap<>();
        for (List<DataFileMeta> group : helper.mergeOverlappingRanges(files)) {
            checkArgument(
                    helper.areAllRangesSame(group), "Normal files must have equal row-id ranges.");
            // A partial update makes the complete group eligible again, including its base.
            int level = group.stream().mapToInt(DataFileMeta::level).min().getAsInt();
            if (level == 0) {
                runs.add(new LevelSortedRun(0, SortedRun.fromSorted(group)));
            } else {
                higherLevels.computeIfAbsent(level, ignored -> new ArrayList<>()).addAll(group);
            }
        }
        higherLevels.forEach(
                (level, group) -> runs.add(new LevelSortedRun(level, SortedRun.fromSorted(group))));
        runs.sort(Comparator.comparingInt(LevelSortedRun::level));
        return runs;
    }

    @Override
    void configureSplit(
            DataSplit.Builder builder,
            SplitGenerator.SplitGroup splitGroup,
            @Nullable AppendDeleteFileMaintainer dvMaintainer) {
        builder.rawConvertible(false);
        if (dvMaintainer != null) {
            builder.withDataDeletionFiles(
                    splitGroup.files.stream()
                            .map(file -> dvMaintainer.getDeletionFile(file.fileName()))
                            .collect(Collectors.toList()));
        }
    }

    @Override
    @Nullable
    CommitMessage finishPartition(
            @Nullable AppendDeleteFileMaintainer dvMaintainer, int totalBuckets) {
        // Keep source deletion vectors intact until the rewrite commits.
        return null;
    }

    @Override
    public List<CommitMessage> prepareCommitMessages(List<CommitMessage> messages) {
        Map<BinaryRow, CompactIncrement> increments = new HashMap<>();
        for (CommitMessage message : messages) {
            CommitMessageImpl impl = (CommitMessageImpl) message;
            checkArgument(
                    impl.bucket() == 0 && impl.newFilesIncrement().isEmpty(),
                    "Data evolution clustering requires compact messages for unaware bucket 0.");
            CompactIncrement merged =
                    increments.computeIfAbsent(
                            message.partition(),
                            ignored ->
                                    new CompactIncrement(
                                            new ArrayList<>(),
                                            new ArrayList<>(),
                                            new ArrayList<>(),
                                            new ArrayList<>(),
                                            new ArrayList<>()));
            CompactIncrement increment = impl.compactIncrement();
            merged.compactBefore().addAll(increment.compactBefore());
            merged.compactAfter().addAll(increment.compactAfter());
            merged.changelogFiles().addAll(increment.changelogFiles());
            merged.newIndexFiles().addAll(increment.newIndexFiles());
            merged.deletedIndexFiles().addAll(increment.deletedIndexFiles());
        }
        List<CommitMessage> prepared = new ArrayList<>();
        increments.forEach(
                (partition, increment) -> {
                    checkArgument(
                            !increment.compactBefore().isEmpty(),
                            "Data evolution clustering output must include its source files.");
                    prepared.add(
                            new CommitMessageImpl(
                                    partition, 0, null, DataIncrement.emptyIncrement(), increment));
                });
        // The committer invalidates global indexes against the latest snapshot on every attempt.
        prepared.addAll(
                new DataEvolutionCompactDeletionVectorRewriter(table, snapshot)
                        .rewriteDeletionVectors(prepared));
        return prepared.stream()
                .map(message -> ((CommitMessageImpl) message).withCheckFromSnapshot(snapshot.id()))
                .collect(Collectors.toList());
    }
}
