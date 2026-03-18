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

package org.apache.paimon.flink.compact.changelog;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Coordinator operator for compacting changelog files.
 *
 * <p>{@link ChangelogCompactCoordinateOperator} calculates the file size of changelog files
 * contained in all buckets within each partition from {@link Committable} message emitted from
 * writer operator. And emit {@link ChangelogCompactTask} to {@link ChangelogCompactWorkerOperator}.
 */
public class ChangelogCompactCoordinateOperator
        extends AbstractStreamOperator<Either<Committable, ChangelogCompactTask>>
        implements OneInputStreamOperator<Committable, Either<Committable, ChangelogCompactTask>>,
                BoundedOneInput {

    private final CoreOptions options;

    private transient long checkpointId;
    private transient Map<BinaryRow, PartitionChangelog> partitionChangelogs;
    private transient Map<BinaryRow, Integer> numBuckets;

    public ChangelogCompactCoordinateOperator(CoreOptions options) {
        this.options = options;
    }

    @Override
    public void open() throws Exception {
        super.open();

        checkpointId = Long.MIN_VALUE;
        partitionChangelogs = new LinkedHashMap<>();
        numBuckets = new LinkedHashMap<>();
    }

    public void processElement(StreamRecord<Committable> record) {
        Committable committable = record.getValue();
        checkpointId = Math.max(checkpointId, committable.checkpointId());
        CommitMessageImpl message = (CommitMessageImpl) committable.commitMessage();
        if (message.newFilesIncrement().changelogFiles().isEmpty()
                && message.compactIncrement().changelogFiles().isEmpty()) {
            output.collect(new StreamRecord<>(Either.Left(record.getValue())));
            return;
        }

        numBuckets.put(message.partition(), message.totalBuckets());

        // Changelog files are not stored in an LSM tree,
        // so we can regard them as files without primary keys.
        long targetFileSize = options.targetFileSize(false);
        long compactionFileSize =
                Math.min(
                        options.compactionFileSize(false),
                        options.toConfiguration()
                                .get(FlinkConnectorOptions.CHANGELOG_PRECOMMIT_COMPACT_BUFFER_SIZE)
                                .getBytes());

        BinaryRow partition = message.partition();
        Integer bucket = message.bucket();
        List<DataFileMeta> skippedNewChangelogs = new ArrayList<>();
        List<DataFileMeta> skippedCompactChangelogs = new ArrayList<>();

        for (DataFileMeta meta : message.newFilesIncrement().changelogFiles()) {
            if (meta.fileSize() >= compactionFileSize) {
                skippedNewChangelogs.add(meta);
                continue;
            }
            partitionChangelogs
                    .computeIfAbsent(partition, k -> new PartitionChangelog())
                    .addNewChangelogFile(bucket, meta);
            PartitionChangelog partitionChangelog = partitionChangelogs.get(partition);
            if (partitionChangelog.totalFileSize >= targetFileSize) {
                emitPartitionChangelogCompactTask(partition);
            }
        }
        for (DataFileMeta meta : message.compactIncrement().changelogFiles()) {
            if (meta.fileSize() >= compactionFileSize) {
                skippedCompactChangelogs.add(meta);
                continue;
            }
            partitionChangelogs
                    .computeIfAbsent(partition, k -> new PartitionChangelog())
                    .addCompactChangelogFile(bucket, meta);
            PartitionChangelog partitionChangelog = partitionChangelogs.get(partition);
            if (partitionChangelog.totalFileSize >= targetFileSize) {
                emitPartitionChangelogCompactTask(partition);
            }
        }

        CommitMessageImpl newMessage =
                new CommitMessageImpl(
                        message.partition(),
                        message.bucket(),
                        message.totalBuckets(),
                        new DataIncrement(
                                message.newFilesIncrement().newFiles(),
                                message.newFilesIncrement().deletedFiles(),
                                skippedNewChangelogs,
                                message.newFilesIncrement().newIndexFiles(),
                                message.newFilesIncrement().deletedIndexFiles()),
                        new CompactIncrement(
                                message.compactIncrement().compactBefore(),
                                message.compactIncrement().compactAfter(),
                                skippedCompactChangelogs,
                                message.compactIncrement().newIndexFiles(),
                                message.compactIncrement().deletedIndexFiles()));
        Committable newCommittable = new Committable(committable.checkpointId(), newMessage);
        output.collect(new StreamRecord<>(Either.Left(newCommittable)));
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {
        emitAllPartitionsChangelogCompactTask();
    }

    @Override
    public void endInput() {
        emitAllPartitionsChangelogCompactTask();
    }

    private void emitPartitionChangelogCompactTask(BinaryRow partition) {
        PartitionChangelog partitionChangelog = partitionChangelogs.get(partition);
        int numNewChangelogFiles =
                partitionChangelog.newFileChangelogFiles.values().stream()
                        .mapToInt(List::size)
                        .sum();
        int numCompactChangelogFiles =
                partitionChangelog.compactChangelogFiles.values().stream()
                        .mapToInt(List::size)
                        .sum();
        if (numNewChangelogFiles + numCompactChangelogFiles == 1) {
            // there is only one changelog file in this partition, so we don't wrap it as a
            // compaction task
            CommitMessageImpl message;
            if (numNewChangelogFiles == 1) {
                Map.Entry<Integer, List<DataFileMeta>> entry =
                        partitionChangelog.newFileChangelogFiles.entrySet().iterator().next();
                message =
                        new CommitMessageImpl(
                                partition,
                                entry.getKey(),
                                numBuckets.get(partition),
                                new DataIncrement(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        entry.getValue()),
                                CompactIncrement.emptyIncrement());
            } else {
                Map.Entry<Integer, List<DataFileMeta>> entry =
                        partitionChangelog.compactChangelogFiles.entrySet().iterator().next();
                message =
                        new CommitMessageImpl(
                                partition,
                                entry.getKey(),
                                numBuckets.get(partition),
                                DataIncrement.emptyIncrement(),
                                new CompactIncrement(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        entry.getValue()));
            }
            Committable newCommittable = new Committable(checkpointId, message);
            output.collect(new StreamRecord<>(Either.Left(newCommittable)));
        } else {
            output.collect(
                    new StreamRecord<>(
                            Either.Right(
                                    new ChangelogCompactTask(
                                            checkpointId,
                                            partition,
                                            numBuckets.get(partition),
                                            partitionChangelog.newFileChangelogFiles,
                                            partitionChangelog.compactChangelogFiles))));
        }
        partitionChangelogs.remove(partition);
    }

    private void emitAllPartitionsChangelogCompactTask() {
        List<BinaryRow> partitions = new ArrayList<>(partitionChangelogs.keySet());
        for (BinaryRow partition : partitions) {
            emitPartitionChangelogCompactTask(partition);
        }
        numBuckets.clear();
    }

    private static class PartitionChangelog {
        private long totalFileSize;
        private final Map<Integer, List<DataFileMeta>> newFileChangelogFiles;
        private final Map<Integer, List<DataFileMeta>> compactChangelogFiles;

        public PartitionChangelog() {
            totalFileSize = 0;
            newFileChangelogFiles = new HashMap<>();
            compactChangelogFiles = new HashMap<>();
        }

        public void addNewChangelogFile(Integer bucket, DataFileMeta file) {
            totalFileSize += file.fileSize();
            newFileChangelogFiles.computeIfAbsent(bucket, k -> new ArrayList<>()).add(file);
        }

        public void addCompactChangelogFile(Integer bucket, DataFileMeta file) {
            totalFileSize += file.fileSize();
            compactChangelogFiles.computeIfAbsent(bucket, k -> new ArrayList<>()).add(file);
        }
    }
}
