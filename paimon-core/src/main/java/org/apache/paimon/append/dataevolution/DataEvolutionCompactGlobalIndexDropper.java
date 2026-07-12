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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;

/** Drops partition-level global indexes invalidated by materialized data-evolution compaction. */
class DataEvolutionCompactGlobalIndexDropper {

    private final FileStoreTable table;
    private final Snapshot snapshot;

    DataEvolutionCompactGlobalIndexDropper(FileStoreTable table, Snapshot snapshot) {
        this.table = table;
        this.snapshot = snapshot;
    }

    List<CommitMessage> dropGlobalIndexes(List<CommitMessage> messages) {
        Set<BinaryRow> partitions = materializedPartitions(messages);
        if (partitions.isEmpty()) {
            return Collections.emptyList();
        }

        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> deletedIndexes = new LinkedHashMap<>();
        for (IndexManifestEntry entry :
                table.store()
                        .newIndexFileHandler()
                        .scan(
                                snapshot,
                                e ->
                                        partitions.contains(e.partition())
                                                && e.indexFile().globalIndexMeta() != null)) {
            deletedIndexes
                    .computeIfAbsent(
                            Pair.of(entry.partition(), entry.bucket()), k -> new ArrayList<>())
                    .add(entry.indexFile());
        }

        List<CommitMessage> result = new ArrayList<>();
        for (Map.Entry<Pair<BinaryRow, Integer>, List<IndexFileMeta>> entry :
                deletedIndexes.entrySet()) {
            result.add(
                    toCommitMessage(
                            entry.getKey().getLeft(), entry.getKey().getRight(), entry.getValue()));
        }
        return result;
    }

    private Set<BinaryRow> materializedPartitions(List<CommitMessage> messages) {
        Set<BinaryRow> result = new LinkedHashSet<>();
        for (CommitMessage message : messages) {
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            CompactIncrement compactIncrement = commitMessage.compactIncrement();
            if (normalFiles(compactIncrement.compactBefore()).isEmpty()) {
                continue;
            }
            // only involve partitions with deletions materialization
            if (compactIncrement.compactAfter().stream()
                    .allMatch(file -> file.firstRowId() == null)) {
                result.add(commitMessage.partition());
            }
        }
        return result;
    }

    private List<DataFileMeta> normalFiles(List<DataFileMeta> files) {
        return files.stream()
                .filter(file -> !isBlobFile(file.fileName()) && !isVectorStoreFile(file.fileName()))
                .collect(Collectors.toList());
    }

    private CommitMessage toCommitMessage(
            BinaryRow partition, int bucket, List<IndexFileMeta> deletedIndexFiles) {
        return new CommitMessageImpl(
                partition,
                bucket,
                null,
                DataIncrement.emptyIncrement(),
                new CompactIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        deletedIndexFiles));
    }
}
