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
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.SplitGenerator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Table-specific file and deletion-vector semantics for incremental clustering. */
public class IncrementalClusterRewriter {

    public static IncrementalClusterRewriter create(FileStoreTable table, Snapshot snapshot) {
        return table.coreOptions().dataEvolutionEnabled()
                ? new DataEvolutionIncrementalClusterRewriter(table, snapshot)
                : new IncrementalClusterRewriter();
    }

    List<LevelSortedRun> constructPartitionLevels(List<DataFileMeta> partitionFiles) {
        List<LevelSortedRun> partitionLevels = new ArrayList<>();
        Map<Integer, List<DataFileMeta>> levelMap =
                partitionFiles.stream().collect(Collectors.groupingBy(DataFileMeta::level));

        for (Map.Entry<Integer, List<DataFileMeta>> entry : levelMap.entrySet()) {
            int level = entry.getKey();
            if (level == 0) {
                for (DataFileMeta level0File : entry.getValue()) {
                    partitionLevels.add(
                            new LevelSortedRun(level, SortedRun.fromSingle(level0File)));
                }
            } else {
                // don't need to guarantee that the files within the same sorted run are
                // non-overlapping here, so we call SortedRun.fromSorted() to avoid sorting and
                // validation
                partitionLevels.add(
                        new LevelSortedRun(level, SortedRun.fromSorted(entry.getValue())));
            }
        }

        // sort by level
        partitionLevels.sort(Comparator.comparing(LevelSortedRun::level));
        return partitionLevels;
    }

    List<DataEvolutionCompactTask> compactionTasks(
            Map<BinaryRow, List<DataFileMeta>> partitionFiles,
            Map<BinaryRow, CompactUnit> clusteringUnits) {
        return Collections.emptyList();
    }

    void configureSplit(
            DataSplit.Builder builder,
            SplitGenerator.SplitGroup splitGroup,
            @Nullable AppendDeleteFileMaintainer dvMaintainer) {
        builder.rawConvertible(splitGroup.rawConvertible);
        if (dvMaintainer != null) {
            builder.withDataDeletionFiles(
                    splitGroup.files.stream()
                            .map(file -> dvMaintainer.notifyRemovedDeletionVector(file.fileName()))
                            .collect(Collectors.toList()));
        }
    }

    @Nullable
    CommitMessage finishPartition(
            @Nullable AppendDeleteFileMaintainer dvMaintainer, int totalBuckets) {
        if (dvMaintainer == null) {
            return null;
        }
        List<IndexFileMeta> newIndexFiles = new ArrayList<>();
        List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
        for (IndexManifestEntry entry : dvMaintainer.persist()) {
            if (entry.kind() == FileKind.ADD) {
                newIndexFiles.add(entry.indexFile());
            } else {
                deletedIndexFiles.add(entry.indexFile());
            }
        }
        return new CommitMessageImpl(
                dvMaintainer.getPartition(),
                0,
                totalBuckets,
                DataIncrement.emptyIncrement(),
                new CompactIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        newIndexFiles,
                        deletedIndexFiles));
    }

    public List<CommitMessage> prepareCommitMessages(List<CommitMessage> messages) {
        return messages;
    }
}
