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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Base {@link FileStoreWrite} implementation.
 *
 * @param <T> type of record to write.
 */
public abstract class AbstractFileStoreWrite<T> implements FileStoreWrite<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFileStoreWrite.class);

    private final String commitUser;
    protected final SnapshotManager snapshotManager;
    private final FileStoreScan scan;

    @Nullable protected IOManager ioManager;

    protected final Map<BinaryRowData, Map<Integer, WriterContainer<T>>> writers;
    private final ExecutorService compactExecutor;

    private boolean overwrite = false;

    protected AbstractFileStoreWrite(
            String commitUser, SnapshotManager snapshotManager, FileStoreScan scan) {
        this.commitUser = commitUser;
        this.snapshotManager = snapshotManager;
        this.scan = scan;

        this.writers = new HashMap<>();
        this.compactExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory(
                                Thread.currentThread().getName() + "-compaction"));
    }

    @Override
    public FileStoreWrite<T> withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    protected List<DataFileMeta> scanExistingFileMetas(
            Long snapshotId, BinaryRowData partition, int bucket) {
        List<DataFileMeta> existingFileMetas = new ArrayList<>();
        if (snapshotId != null) {
            // Concat all the DataFileMeta of existing files into existingFileMetas.
            scan.withSnapshot(snapshotId).withPartitionFilter(Collections.singletonList(partition))
                    .withBucket(bucket).plan().files().stream()
                    .map(ManifestEntry::file)
                    .forEach(existingFileMetas::add);
        }
        return existingFileMetas;
    }

    public void withOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    @Override
    public void write(BinaryRowData partition, int bucket, T data) throws Exception {
        RecordWriter<T> writer = getWriterWrapper(partition, bucket).writer;
        writer.write(data);
    }

    @Override
    public void compact(BinaryRowData partition, int bucket, boolean fullCompaction)
            throws Exception {
        getWriterWrapper(partition, bucket).writer.compact(fullCompaction);
    }

    @Override
    public void notifyNewFiles(
            long snapshotId, BinaryRowData partition, int bucket, List<DataFileMeta> files) {
        WriterContainer<T> writerContainer = getWriterWrapper(partition, bucket);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Get extra compact files for partition {}, bucket {}. Extra snapshot {}, base snapshot {}.\nFiles: {}",
                    partition,
                    bucket,
                    snapshotId,
                    writerContainer.baseSnapshotId,
                    files);
        }
        if (snapshotId > writerContainer.baseSnapshotId) {
            writerContainer.writer.addNewFiles(files);
        }
    }

    @Override
    public List<FileCommittable> prepareCommit(boolean blocking, long commitIdentifier)
            throws Exception {
        long latestCommittedIdentifier;
        if (writers.values().stream()
                        .map(Map::values)
                        .flatMap(Collection::stream)
                        .mapToLong(w -> w.lastModifiedCommitIdentifier)
                        .max()
                        .orElse(Long.MIN_VALUE)
                == Long.MIN_VALUE) {
            // Optimization for the first commit.
            //
            // If this is the first commit, no writer has previous modified commit, so the value of
            // `latestCommittedIdentifier` does not matter.
            //
            // Without this optimization, we may need to scan through all snapshots only to find
            // that there is no previous snapshot by this user, which is very inefficient.
            latestCommittedIdentifier = Long.MIN_VALUE;
        } else {
            latestCommittedIdentifier =
                    snapshotManager
                            .latestSnapshotOfUser(commitUser)
                            .map(Snapshot::commitIdentifier)
                            .orElse(Long.MIN_VALUE);
        }

        List<FileCommittable> result = new ArrayList<>();

        Iterator<Map.Entry<BinaryRowData, Map<Integer, WriterContainer<T>>>> partIter =
                writers.entrySet().iterator();
        while (partIter.hasNext()) {
            Map.Entry<BinaryRowData, Map<Integer, WriterContainer<T>>> partEntry = partIter.next();
            BinaryRowData partition = partEntry.getKey();
            Iterator<Map.Entry<Integer, WriterContainer<T>>> bucketIter =
                    partEntry.getValue().entrySet().iterator();
            while (bucketIter.hasNext()) {
                Map.Entry<Integer, WriterContainer<T>> entry = bucketIter.next();
                int bucket = entry.getKey();
                WriterContainer<T> writerContainer = entry.getValue();

                RecordWriter.CommitIncrement increment =
                        writerContainer.writer.prepareCommit(blocking);
                FileCommittable committable =
                        new FileCommittable(
                                partition,
                                bucket,
                                increment.newFilesIncrement(),
                                increment.compactIncrement());
                result.add(committable);

                if (committable.isEmpty()) {
                    if (writerContainer.lastModifiedCommitIdentifier <= latestCommittedIdentifier) {
                        // Clear writer if no update, and if its latest modification has committed.
                        //
                        // We need a mechanism to clear writers, otherwise there will be more and
                        // more such as yesterday's partition that no longer needs to be written.
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Closing writer for partition {}, bucket {}. "
                                            + "Writer's last modified identifier is {}, "
                                            + "while latest committed identifier is {}",
                                    partition,
                                    bucket,
                                    writerContainer.lastModifiedCommitIdentifier,
                                    latestCommittedIdentifier);
                        }
                        writerContainer.writer.close();
                        bucketIter.remove();
                    }
                } else {
                    writerContainer.lastModifiedCommitIdentifier = commitIdentifier;
                }
            }

            if (partEntry.getValue().isEmpty()) {
                partIter.remove();
            }
        }

        return result;
    }

    @Override
    public void close() throws Exception {
        for (Map<Integer, WriterContainer<T>> bucketWriters : writers.values()) {
            for (WriterContainer<T> writerContainer : bucketWriters.values()) {
                writerContainer.writer.close();
            }
        }
        writers.clear();
        compactExecutor.shutdownNow();
    }

    private WriterContainer<T> getWriterWrapper(BinaryRowData partition, int bucket) {
        Map<Integer, WriterContainer<T>> buckets = writers.get(partition);
        if (buckets == null) {
            buckets = new HashMap<>();
            writers.put(partition.copy(), buckets);
        }
        return buckets.computeIfAbsent(
                bucket, k -> createWriterContainer(partition.copy(), bucket));
    }

    private WriterContainer<T> createWriterContainer(BinaryRowData partition, int bucket) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating writer for partition {}, bucket {}", partition, bucket);
        }
        WriterContainer<T> writerContainer =
                overwrite
                        ? createEmptyWriterContainer(partition.copy(), bucket, compactExecutor)
                        : createWriterContainer(partition.copy(), bucket, compactExecutor);
        notifyNewWriter(writerContainer.writer);
        return writerContainer;
    }

    protected void notifyNewWriter(RecordWriter<T> writer) {}

    /** Create a {@link RecordWriter} from partition and bucket. */
    @VisibleForTesting
    public abstract WriterContainer<T> createWriterContainer(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor);

    /** Create an empty {@link RecordWriter} from partition and bucket. */
    @VisibleForTesting
    public abstract WriterContainer<T> createEmptyWriterContainer(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor);

    /**
     * {@link RecordWriter} with the snapshot id it is created upon and the identifier of its last
     * modified commit.
     */
    @VisibleForTesting
    public static class WriterContainer<T> {

        public final RecordWriter<T> writer;
        private final long baseSnapshotId;
        private long lastModifiedCommitIdentifier;

        protected WriterContainer(RecordWriter<T> writer, Long baseSnapshotId) {
            this.writer = writer;
            this.baseSnapshotId =
                    baseSnapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID - 1 : baseSnapshotId;
            this.lastModifiedCommitIdentifier = Long.MIN_VALUE;
        }
    }
}
