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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** Reprepares stale compaction against a pinned snapshot and the uncommitted raw files. */
public final class FullCompactionRefresh {
    private final CoreOptions options;
    private final SnapshotManager snapshots;
    private final Supplier<FileStoreScan> scans;
    private final Supplier<IndexFileHandler> indexes;
    private final Supplier<FileStoreWrite<?>> writers;

    public FullCompactionRefresh(
            CoreOptions options,
            SnapshotManager snapshots,
            Supplier<FileStoreScan> scans,
            Supplier<IndexFileHandler> indexes,
            Supplier<FileStoreWrite<?>> writers) {
        this.options = options;
        this.snapshots = snapshots;
        this.scans = scans;
        this.indexes = indexes;
        this.writers = writers;
    }

    public List<CommitMessage> refresh(
            Snapshot snapshot,
            List<CommitMessage> messages,
            long identifier,
            @Nullable IOManager borrowedIO) {
        Map<Pair<BinaryRow, Integer>, List<CommitMessageImpl>> groups = new LinkedHashMap<>();
        for (CommitMessage message : messages) {
            CommitMessageImpl original = (CommitMessageImpl) message;
            groups.computeIfAbsent(
                            Pair.of(original.partition(), original.bucket()),
                            ignored -> new ArrayList<>())
                    .add(original);
        }
        WriteRestore committed =
                new FileSystemWriteRestore(
                        options, snapshots, scans.get(), indexes.get(), snapshot.id());
        WriteRestore pending =
                new WriteRestore() {
                    @Override
                    public long latestCommittedIdentifier(String user) {
                        return committed.latestCommittedIdentifier(user);
                    }

                    @Override
                    public RestoreFiles restoreFiles(
                            BinaryRow partition,
                            int bucket,
                            boolean dynamicIndex,
                            boolean deleteVectors,
                            boolean sourceIndexes) {
                        RestoreFiles base =
                                committed.restoreFiles(
                                        partition,
                                        bucket,
                                        dynamicIndex,
                                        deleteVectors,
                                        sourceIndexes);
                        Map<String, DataFileMeta> files = new LinkedHashMap<>();
                        if (base.dataFiles() != null) {
                            for (DataFileMeta file : base.dataFiles()) {
                                files.put(file.fileName(), file);
                            }
                        }
                        List<CommitMessageImpl> originals = groups.get(Pair.of(partition, bucket));
                        for (CommitMessageImpl original : originals) {
                            for (DataFileMeta file : original.newFilesIncrement().deletedFiles()) {
                                files.remove(file.fileName());
                            }
                            for (DataFileMeta file : original.newFilesIncrement().newFiles()) {
                                files.put(file.fileName(), file);
                            }
                        }
                        return new RestoreFiles(
                                snapshot,
                                originals.get(0).totalBuckets(),
                                new ArrayList<>(files.values()),
                                base.dynamicBucketIndex(),
                                base.deleteVectorsIndex(),
                                base.sourceIndexPayloads());
                    }
                };
        try (IOManager ownedIO =
                borrowedIO == null
                        ? IOManager.create(System.getProperties().getProperty("java.io.tmpdir"))
                        : null) {
            Map<Pair<BinaryRow, Integer>, CompactIncrement> refreshed = new LinkedHashMap<>();
            // Prepare each bucket before starting the next one, so a failing prepare cannot
            // abandon readers belonging to other asynchronous compactions.
            for (Map.Entry<Pair<BinaryRow, Integer>, List<CommitMessageImpl>> group :
                    groups.entrySet()) {
                if (group.getValue().stream().noneMatch(m -> !m.compactIncrement().isEmpty())) {
                    continue;
                }
                boolean full =
                        group.getValue().stream()
                                .anyMatch(m -> !m.compactIncrement().changelogFiles().isEmpty());
                FileStoreWrite<?> write = writers.get();
                try (AutoCloseable ownedWrite = write::close) {
                    write.withIOManager(borrowedIO == null ? ownedIO : borrowedIO);
                    write.withWriteRestore(pending);
                    write.compact(group.getKey().getLeft(), group.getKey().getRight(), full);
                    for (CommitMessage message : write.prepareCommit(true, identifier)) {
                        CommitMessageImpl prepared = (CommitMessageImpl) message;
                        refreshed.put(
                                Pair.of(prepared.partition(), prepared.bucket()),
                                prepared.compactIncrement());
                    }
                }
            }
            List<CommitMessage> result = new ArrayList<>();
            for (CommitMessage message : messages) {
                CommitMessageImpl original = (CommitMessageImpl) message;
                CompactIncrement compact =
                        refreshed.remove(Pair.of(original.partition(), original.bucket()));
                result.add(
                        new CommitMessageImpl(
                                original.partition(),
                                original.bucket(),
                                original.totalBuckets(),
                                original.newFilesIncrement(),
                                compact == null ? CompactIncrement.emptyIncrement() : compact));
            }
            return result;
        } catch (Exception error) {
            throw new RuntimeException("Failed to refresh stale full compaction", error);
        }
    }
}
