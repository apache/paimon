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

package org.apache.paimon.flink.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.UnawareAppendCompactionTask;
import org.apache.paimon.append.UnawareBucketNewFilesCompactionCoordinator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Coordinator operator for compacting newly created files for unaware bucket tables.
 *
 * <p>{@link UnawareBucketNewFilesCompactionCoordinatorOperator} calculates the file size of newly
 * created files contained in all buckets within each partition from {@link Committable} message
 * emitted from writer operator. And emit {@link UnawareAppendCompactionTask} to {@link
 * UnawareBucketNewFilesCompactionWorkerOperator}.
 */
public class UnawareBucketNewFilesCompactionCoordinatorOperator
        extends AbstractStreamOperator<
                Either<Committable, Tuple2<Long, UnawareAppendCompactionTask>>>
        implements OneInputStreamOperator<
                        Committable,
                        Either<Committable, Tuple2<Long, UnawareAppendCompactionTask>>>,
                BoundedOneInput {

    private final long targetFileSize;
    private final long compactionFileSize;

    private transient UnawareBucketNewFilesCompactionCoordinator coordinator;
    private transient long checkpointId;

    public UnawareBucketNewFilesCompactionCoordinatorOperator(CoreOptions options) {
        this.targetFileSize = options.targetFileSize(false);
        this.compactionFileSize = options.compactionFileSize(false);
    }

    @Override
    public void open() throws Exception {
        super.open();

        coordinator = new UnawareBucketNewFilesCompactionCoordinator(targetFileSize);
        checkpointId = Long.MIN_VALUE;
    }

    @Override
    public void processElement(StreamRecord<Committable> record) throws Exception {
        Committable committable = record.getValue();
        checkpointId = Math.max(checkpointId, committable.checkpointId());
        if (committable.kind() != Committable.Kind.FILE) {
            output.collect(new StreamRecord<>(Either.Left(committable)));
            return;
        }

        CommitMessageImpl message = (CommitMessageImpl) committable.wrappedCommittable();
        if (message.newFilesIncrement().newFiles().isEmpty()) {
            output.collect(new StreamRecord<>(Either.Left(committable)));
            return;
        }

        BinaryRow partition = message.partition();
        List<DataFileMeta> skippedFiles = new ArrayList<>();
        for (DataFileMeta meta : message.newFilesIncrement().newFiles()) {
            if (meta.fileSize() >= compactionFileSize) {
                skippedFiles.add(meta);
                continue;
            }

            Optional<Pair<BinaryRow, List<DataFileMeta>>> optionalPair =
                    coordinator.addFile(partition, meta);
            if (optionalPair.isPresent()) {
                Pair<BinaryRow, List<DataFileMeta>> p = optionalPair.get();
                Preconditions.checkArgument(!p.getValue().isEmpty());
                if (p.getValue().size() > 1) {
                    output.collect(
                            new StreamRecord<>(
                                    Either.Right(
                                            Tuple2.of(
                                                    checkpointId,
                                                    new UnawareAppendCompactionTask(
                                                            p.getKey(), p.getValue())))));
                } else {
                    skippedFiles.add(p.getValue().get(0));
                }
            }
        }

        CommitMessageImpl newMessage =
                new CommitMessageImpl(
                        message.partition(),
                        message.bucket(),
                        new DataIncrement(
                                skippedFiles,
                                message.newFilesIncrement().deletedFiles(),
                                message.newFilesIncrement().changelogFiles()),
                        message.compactIncrement(),
                        message.indexIncrement());
        if (!newMessage.isEmpty()) {
            Committable newCommittable =
                    new Committable(committable.checkpointId(), Committable.Kind.FILE, newMessage);
            output.collect(new StreamRecord<>(Either.Left(newCommittable)));
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        emitAll();
    }

    @Override
    public void endInput() throws Exception {
        emitAll();
    }

    private void emitAll() {
        for (Pair<BinaryRow, List<DataFileMeta>> p : coordinator.emitAll()) {
            Preconditions.checkArgument(!p.getValue().isEmpty());
            if (p.getValue().size() > 1) {
                output.collect(
                        new StreamRecord<>(
                                Either.Right(
                                        Tuple2.of(
                                                checkpointId,
                                                new UnawareAppendCompactionTask(
                                                        p.getKey(), p.getValue())))));
            } else {
                CommitMessageImpl message =
                        new CommitMessageImpl(
                                p.getKey(),
                                BucketMode.UNAWARE_BUCKET,
                                new DataIncrement(
                                        Collections.singletonList(p.getValue().get(0)),
                                        Collections.emptyList(),
                                        Collections.emptyList()),
                                CompactIncrement.emptyIncrement());
                output.collect(
                        new StreamRecord<>(
                                Either.Left(
                                        new Committable(
                                                checkpointId, Committable.Kind.FILE, message))));
            }
        }
    }
}
