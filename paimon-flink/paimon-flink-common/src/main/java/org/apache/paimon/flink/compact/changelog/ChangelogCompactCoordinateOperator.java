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

import org.apache.paimon.append.UnawareAppendTableCompactionCoordinator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Coordinator operator for compacting changelog files.
 *
 * <p>{@link UnawareAppendTableCompactionCoordinator} calculates the file size of changelog files
 * contained in all buckets within each partition from {@link Committable} message emitted from
 * writer operator. And emit {@link ChangelogCompactTask} to {@link ChangelogCompactWorkerOperator}.
 */
public class ChangelogCompactCoordinateOperator extends AbstractStreamOperator<ChangelogCompactTask>
        implements OneInputStreamOperator<Committable, ChangelogCompactTask>, BoundedOneInput {
    private final FileStoreTable table;
    private final OutputTag<Committable> committableOutputTag;

    private transient long checkpointId;
    private transient Map<BinaryRow, PartitionChangelog> partitionChangelogs;

    public ChangelogCompactCoordinateOperator(
            FileStoreTable table, OutputTag<Committable> committableOutputTag) {
        this.table = table;
        this.committableOutputTag = committableOutputTag;
    }

    @Override
    public void open() throws Exception {
        super.open();

        checkpointId = Long.MIN_VALUE;
        partitionChangelogs = new HashMap<>();
    }

    public void processElement(StreamRecord<Committable> record) {
        Committable committable = record.getValue();
        checkpointId = Math.max(checkpointId, committable.checkpointId());
        if (committable.kind() != Committable.Kind.FILE) {
            output.collect(committableOutputTag, record);
            return;
        }

        CommitMessageImpl message = (CommitMessageImpl) committable.wrappedCommittable();
        if (message.newFilesIncrement().changelogFiles().isEmpty()
                && message.compactIncrement().changelogFiles().isEmpty()) {
            output.collect(committableOutputTag, record);
            return;
        }

        BinaryRow partition = message.partition();
        Integer bucket = message.bucket();
        for (DataFileMeta meta : message.newFilesIncrement().changelogFiles()) {
            partitionChangelogs
                    .computeIfAbsent(partition, k -> new PartitionChangelog())
                    .addNewChangelogFile(bucket, meta);
            PartitionChangelog partitionChangelog = partitionChangelogs.get(partition);
            if (partitionChangelog.totalFileSize >= table.coreOptions().targetFileSize(false)) {
                emitPartitionChangelogCompactTask(partition);
            }
        }
        for (DataFileMeta meta : message.compactIncrement().changelogFiles()) {
            partitionChangelogs
                    .computeIfAbsent(partition, k -> new PartitionChangelog())
                    .addCompactChangelogFile(bucket, meta);
            PartitionChangelog partitionChangelog = partitionChangelogs.get(partition);
            if (partitionChangelog.totalFileSize >= table.coreOptions().targetFileSize(false)) {
                emitPartitionChangelogCompactTask(partition);
            }
        }

        CommitMessageImpl newMessage =
                new CommitMessageImpl(
                        message.partition(),
                        message.bucket(),
                        new DataIncrement(
                                message.newFilesIncrement().newFiles(),
                                message.newFilesIncrement().deletedFiles(),
                                Collections.emptyList()),
                        new CompactIncrement(
                                message.compactIncrement().compactBefore(),
                                message.compactIncrement().compactAfter(),
                                Collections.emptyList()),
                        message.indexIncrement());
        Committable newCommittable =
                new Committable(committable.checkpointId(), Committable.Kind.FILE, newMessage);
        output.collect(committableOutputTag, new StreamRecord<>(newCommittable));
    }

    public void prepareSnapshotPreBarrier(long checkpointId) {
        emitAllPartitionsChanglogCompactTask();
    }

    public void endInput() {
        emitAllPartitionsChanglogCompactTask();
    }

    private void emitPartitionChangelogCompactTask(BinaryRow partition) {
        PartitionChangelog partitionChangelog = partitionChangelogs.get(partition);
        output.collect(
                new StreamRecord<>(
                        new ChangelogCompactTask(
                                checkpointId,
                                partition,
                                partitionChangelog.newFileChangelogFiles,
                                partitionChangelog.compactChangelogFiles)));
        partitionChangelogs.remove(partition);
    }

    private void emitAllPartitionsChanglogCompactTask() {
        List<BinaryRow> partitions = new ArrayList<>(partitionChangelogs.keySet());
        for (BinaryRow partition : partitions) {
            emitPartitionChangelogCompactTask(partition);
        }
    }

    private static class PartitionChangelog {
        private long totalFileSize;
        private final Map<Integer, List<DataFileMeta>> newFileChangelogFiles;
        private final Map<Integer, List<DataFileMeta>> compactChangelogFiles;

        public long totalFileSize() {
            return totalFileSize;
        }

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
