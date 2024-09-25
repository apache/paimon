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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.compact.changelog.format.CompactedChangelogReadOnlyFormat;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Operator to compact several changelog files from the same partition into one file, in order to
 * reduce the number of small files.
 */
public class ChangelogCompactOperator extends AbstractStreamOperator<Committable>
        implements OneInputStreamOperator<Committable, Committable>, BoundedOneInput {

    private final FileStoreTable table;

    private transient FileStorePathFactory pathFactory;
    private transient long checkpointId;
    private transient Map<BinaryRow, OutputStream> outputStreams;
    private transient Map<BinaryRow, List<Result>> results;

    public ChangelogCompactOperator(FileStoreTable table) {
        this.table = table;
    }

    @Override
    public void open() throws Exception {
        super.open();

        pathFactory = table.store().pathFactory();
        checkpointId = Long.MIN_VALUE;
        outputStreams = new HashMap<>();
        results = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<Committable> record) throws Exception {
        Committable committable = record.getValue();
        checkpointId = Math.max(checkpointId, committable.checkpointId());
        if (committable.kind() != Committable.Kind.FILE) {
            output.collect(record);
            return;
        }

        CommitMessageImpl message = (CommitMessageImpl) committable.wrappedCommittable();
        if (message.newFilesIncrement().changelogFiles().isEmpty()
                && message.compactIncrement().changelogFiles().isEmpty()) {
            output.collect(record);
            return;
        }

        // copy changelogs from the same partition into one file
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(message.partition(), message.bucket());
        for (DataFileMeta meta : message.newFilesIncrement().changelogFiles()) {
            copyFile(
                    dataFilePathFactory.toPath(meta.fileName()),
                    message.partition(),
                    message.bucket(),
                    false,
                    meta);
        }
        for (DataFileMeta meta : message.compactIncrement().changelogFiles()) {
            copyFile(
                    dataFilePathFactory.toPath(meta.fileName()),
                    message.partition(),
                    message.bucket(),
                    true,
                    meta);
        }

        // send commit message without changelog files
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
        if (record.hasTimestamp()) {
            output.collect(new StreamRecord<>(newCommittable, record.getTimestamp()));
        } else {
            output.collect(new StreamRecord<>(newCommittable));
        }
    }

    private void copyFile(
            Path path, BinaryRow partition, int bucket, boolean isCompactResult, DataFileMeta meta)
            throws Exception {
        if (!outputStreams.containsKey(partition)) {
            Path outputPath =
                    new Path(path.getParent(), "tmp-compacted-changelog-" + UUID.randomUUID());
            outputStreams.put(
                    partition,
                    new OutputStream(
                            outputPath, table.fileIO().newOutputStream(outputPath, false)));
        }

        OutputStream outputStream = outputStreams.get(partition);
        long offset = outputStream.out.getPos();
        try (SeekableInputStream in = table.fileIO().newInputStream(path)) {
            IOUtils.copyBytes(in, outputStream.out, IOUtils.BLOCKSIZE, false);
        }
        table.fileIO().deleteQuietly(path);
        results.computeIfAbsent(partition, p -> new ArrayList<>())
                .add(
                        new Result(
                                bucket,
                                isCompactResult,
                                meta,
                                offset,
                                outputStream.out.getPos() - offset));

        if (outputStream.out.getPos() >= table.coreOptions().targetFileSize(false)) {
            flushPartition(partition);
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        flushAllPartitions();
    }

    @Override
    public void endInput() throws Exception {
        flushAllPartitions();
    }

    private void flushAllPartitions() throws Exception {
        List<BinaryRow> partitions = new ArrayList<>(outputStreams.keySet());
        for (BinaryRow partition : partitions) {
            flushPartition(partition);
        }
    }

    private void flushPartition(BinaryRow partition) throws Exception {
        OutputStream outputStream = outputStreams.get(partition);
        outputStream.out.close();

        Result baseResult = results.get(partition).get(0);
        Preconditions.checkArgument(baseResult.offset == 0);
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(partition, baseResult.bucket);
        // see Java docs of `CompactedChangelogFormatReaderFactory`
        String realName =
                "compacted-changelog-"
                        + UUID.randomUUID()
                        + "$"
                        + baseResult.bucket
                        + "-"
                        + baseResult.length;
        table.fileIO()
                .rename(
                        outputStream.path,
                        dataFilePathFactory.toPath(
                                realName
                                        + "."
                                        + CompactedChangelogReadOnlyFormat.getIdentifier(
                                                baseResult.meta.fileFormat())));

        Map<Integer, List<Result>> grouped = new HashMap<>();
        for (Result result : results.get(partition)) {
            grouped.computeIfAbsent(result.bucket, b -> new ArrayList<>()).add(result);
        }

        for (Map.Entry<Integer, List<Result>> entry : grouped.entrySet()) {
            List<DataFileMeta> newFilesChangelog = new ArrayList<>();
            List<DataFileMeta> compactChangelog = new ArrayList<>();
            for (Result result : entry.getValue()) {
                // see Java docs of `CompactedChangelogFormatReaderFactory`
                String name =
                        (result.offset == 0
                                        ? realName
                                        : realName + "-" + result.offset + "-" + result.length)
                                + "."
                                + CompactedChangelogReadOnlyFormat.getIdentifier(
                                        result.meta.fileFormat());
                if (result.isCompactResult) {
                    compactChangelog.add(result.meta.rename(name));
                } else {
                    newFilesChangelog.add(result.meta.rename(name));
                }
            }

            CommitMessageImpl newMessage =
                    new CommitMessageImpl(
                            partition,
                            entry.getKey(),
                            new DataIncrement(
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    newFilesChangelog),
                            new CompactIncrement(
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    compactChangelog));
            Committable newCommittable =
                    new Committable(checkpointId, Committable.Kind.FILE, newMessage);
            output.collect(new StreamRecord<>(newCommittable));
        }

        outputStreams.remove(partition);
        results.remove(partition);
    }

    @Override
    public void close() throws Exception {
        for (Map.Entry<BinaryRow, OutputStream> entry : outputStreams.entrySet()) {
            OutputStream outputStream = entry.getValue();
            try {
                outputStream.out.close();
            } catch (Exception e) {
                LOG.warn("Failed to close output stream for file " + outputStream.path, e);
            }
            table.fileIO().deleteQuietly(outputStream.path);
        }

        outputStreams.clear();
        results.clear();
    }

    private static class OutputStream {

        private final Path path;
        private final PositionOutputStream out;

        private OutputStream(Path path, PositionOutputStream out) {
            this.path = path;
            this.out = out;
        }
    }

    private static class Result {

        private final int bucket;
        private final boolean isCompactResult;
        private final DataFileMeta meta;
        private final long offset;
        private final long length;

        private Result(
                int bucket, boolean isCompactResult, DataFileMeta meta, long offset, long length) {
            this.bucket = bucket;
            this.isCompactResult = isCompactResult;
            this.meta = meta;
            this.offset = offset;
            this.length = length;
        }
    }
}
