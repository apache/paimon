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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * {@link ChangelogCompactTask} to compact several changelog files from the same partition into one
 * file, in order to reduce the number of small files.
 */
public class ChangelogCompactTask implements Serializable {
    private final long checkpointId;
    private final BinaryRow partition;
    private final Map<Integer, List<DataFileMeta>> newFileChangelogFiles;
    private final Map<Integer, List<DataFileMeta>> compactChangelogFiles;

    public ChangelogCompactTask(
            long checkpointId,
            BinaryRow partition,
            Map<Integer, List<DataFileMeta>> newFileChangelogFiles,
            Map<Integer, List<DataFileMeta>> compactChangelogFiles) {
        this.checkpointId = checkpointId;
        this.partition = partition;
        this.newFileChangelogFiles = newFileChangelogFiles;
        this.compactChangelogFiles = compactChangelogFiles;
    }

    public long checkpointId() {
        return checkpointId;
    }

    public BinaryRow partition() {
        return partition;
    }

    public Map<Integer, List<DataFileMeta>> newFileChangelogFiles() {
        return newFileChangelogFiles;
    }

    public Map<Integer, List<DataFileMeta>> compactChangelogFiles() {
        return compactChangelogFiles;
    }

    public List<Committable> doCompact(FileStoreTable table) throws Exception {
        FileStorePathFactory pathFactory = table.store().pathFactory();
        OutputStream outputStream = new OutputStream();
        List<Result> results = new ArrayList<>();

        // copy all changelog files to a new big file
        for (Map.Entry<Integer, List<DataFileMeta>> entry : newFileChangelogFiles.entrySet()) {
            int bucket = entry.getKey();
            DataFilePathFactory dataFilePathFactory =
                    pathFactory.createDataFilePathFactory(partition, bucket);
            for (DataFileMeta meta : entry.getValue()) {
                copyFile(
                        outputStream,
                        results,
                        table,
                        dataFilePathFactory.toPath(meta.fileName()),
                        bucket,
                        false,
                        meta);
            }
        }
        for (Map.Entry<Integer, List<DataFileMeta>> entry : compactChangelogFiles.entrySet()) {
            Integer bucket = entry.getKey();
            DataFilePathFactory dataFilePathFactory =
                    pathFactory.createDataFilePathFactory(partition, bucket);
            for (DataFileMeta meta : entry.getValue()) {
                copyFile(
                        outputStream,
                        results,
                        table,
                        dataFilePathFactory.toPath(meta.fileName()),
                        bucket,
                        true,
                        meta);
            }
        }
        outputStream.out.close();

        return produceNewCommittables(results, table, pathFactory, outputStream.path);
    }

    private void copyFile(
            OutputStream outputStream,
            List<Result> results,
            FileStoreTable table,
            Path path,
            int bucket,
            boolean isCompactResult,
            DataFileMeta meta)
            throws Exception {
        if (!outputStream.isInitialized) {
            Path outputPath =
                    new Path(path.getParent(), "tmp-compacted-changelog-" + UUID.randomUUID());
            outputStream.init(outputPath, table.fileIO().newOutputStream(outputPath, false));
        }
        long offset = outputStream.out.getPos();
        try (SeekableInputStream in = table.fileIO().newInputStream(path)) {
            IOUtils.copyBytes(in, outputStream.out, IOUtils.BLOCKSIZE, false);
        }
        table.fileIO().deleteQuietly(path);
        results.add(
                new Result(
                        bucket, isCompactResult, meta, offset, outputStream.out.getPos() - offset));
    }

    private List<Committable> produceNewCommittables(
            List<Result> results,
            FileStoreTable table,
            FileStorePathFactory pathFactory,
            Path changelogTempPath)
            throws IOException {
        Result baseResult = results.get(0);
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
                        changelogTempPath,
                        dataFilePathFactory.toPath(
                                realName
                                        + "."
                                        + CompactedChangelogReadOnlyFormat.getIdentifier(
                                                baseResult.meta.fileFormat())));

        List<Committable> newCommittables = new ArrayList<>();

        Map<Integer, List<Result>> bucketedResults = new HashMap<>();
        for (Result result : results) {
            bucketedResults.computeIfAbsent(result.bucket, b -> new ArrayList<>()).add(result);
        }

        for (Map.Entry<Integer, List<Result>> entry : bucketedResults.entrySet()) {
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
            newCommittables.add(new Committable(checkpointId, Committable.Kind.FILE, newMessage));
        }
        return newCommittables;
    }

    public int hashCode() {
        return Objects.hash(checkpointId, partition, newFileChangelogFiles, compactChangelogFiles);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ChangelogCompactTask that = (ChangelogCompactTask) o;
        return checkpointId == that.checkpointId
                && Objects.equals(partition, that.partition)
                && Objects.equals(newFileChangelogFiles, that.newFileChangelogFiles)
                && Objects.equals(compactChangelogFiles, that.compactChangelogFiles);
    }

    @Override
    public String toString() {
        return String.format(
                "ChangelogCompactionTask {"
                        + "partition = %s, "
                        + "newFileChangelogFiles = %s, "
                        + "compactChangelogFiles = %s}",
                partition, newFileChangelogFiles, compactChangelogFiles);
    }

    private static class OutputStream {

        private Path path;
        private PositionOutputStream out;
        private boolean isInitialized;

        private OutputStream() {
            this.isInitialized = false;
        }

        private void init(Path path, PositionOutputStream out) {
            this.path = path;
            this.out = out;
            this.isInitialized = true;
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
