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

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.Restorable;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
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
public abstract class AbstractFileStoreWrite<T>
        implements FileStoreWrite<T>, Restorable<List<AbstractFileStoreWrite.State<T>>> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFileStoreWrite.class);

    private final String commitUser;
    protected final SnapshotManager snapshotManager;
    private final FileStoreScan scan;
    @Nullable private final IndexMaintainer.Factory<T> indexFactory;

    @Nullable protected IOManager ioManager;

    protected final Map<BinaryRow, Map<Integer, WriterContainer<T>>> writers;

    private ExecutorService lazyCompactExecutor;
    private boolean ignorePreviousFiles = false;

    protected AbstractFileStoreWrite(
            String commitUser,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            @Nullable IndexMaintainer.Factory<T> indexFactory) {
        this.commitUser = commitUser;
        this.snapshotManager = snapshotManager;
        this.scan = scan;
        this.indexFactory = indexFactory;

        this.writers = new HashMap<>();
    }

    @Override
    public FileStoreWrite<T> withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public FileStoreWrite<T> withMemoryPool(MemorySegmentPool memoryPool) {
        return this;
    }

    @Override
    public void withIgnorePreviousFiles(boolean ignorePreviousFiles) {
        this.ignorePreviousFiles = ignorePreviousFiles;
    }

    @Override
    public void write(BinaryRow partition, int bucket, T data) throws Exception {
        WriterContainer<T> container = getWriterWrapper(partition, bucket);
        container.writer.write(data);
        if (container.indexMaintainer != null) {
            container.indexMaintainer.notifyNewRecord(data);
        }
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        getWriterWrapper(partition, bucket).writer.compact(fullCompaction);
    }

    @Override
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
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
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
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

        List<CommitMessage> result = new ArrayList<>();

        Iterator<Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>>> partIter =
                writers.entrySet().iterator();
        while (partIter.hasNext()) {
            Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>> partEntry = partIter.next();
            BinaryRow partition = partEntry.getKey();
            Iterator<Map.Entry<Integer, WriterContainer<T>>> bucketIter =
                    partEntry.getValue().entrySet().iterator();
            while (bucketIter.hasNext()) {
                Map.Entry<Integer, WriterContainer<T>> entry = bucketIter.next();
                int bucket = entry.getKey();
                WriterContainer<T> writerContainer = entry.getValue();

                CommitIncrement increment = writerContainer.writer.prepareCommit(waitCompaction);
                List<IndexFileMeta> newIndexFiles = new ArrayList<>();
                if (writerContainer.indexMaintainer != null) {
                    newIndexFiles = writerContainer.indexMaintainer.prepareCommit();
                }
                CommitMessageImpl committable =
                        new CommitMessageImpl(
                                partition,
                                bucket,
                                increment.newFilesIncrement(),
                                increment.compactIncrement(),
                                new IndexIncrement(newIndexFiles));
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
        if (lazyCompactExecutor != null) {
            lazyCompactExecutor.shutdownNow();
        }
    }

    @Override
    public List<State<T>> checkpoint() {
        List<State<T>> result = new ArrayList<>();

        for (Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>> partitionEntry :
                writers.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            for (Map.Entry<Integer, WriterContainer<T>> bucketEntry :
                    partitionEntry.getValue().entrySet()) {
                int bucket = bucketEntry.getKey();
                WriterContainer<T> writerContainer = bucketEntry.getValue();

                CommitIncrement increment;
                try {
                    increment = writerContainer.writer.prepareCommit(false);
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to extract state from writer of partition "
                                    + partition
                                    + " bucket "
                                    + bucket,
                            e);
                }
                // writer.allFiles() must be fetched after writer.prepareCommit(), because
                // compaction result might be updated during prepareCommit
                Collection<DataFileMeta> dataFiles = writerContainer.writer.dataFiles();
                result.add(
                        new State<>(
                                partition,
                                bucket,
                                writerContainer.baseSnapshotId,
                                writerContainer.lastModifiedCommitIdentifier,
                                dataFiles,
                                writerContainer.indexMaintainer,
                                increment));
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Extracted state " + result);
        }
        return result;
    }

    @Override
    public void restore(List<State<T>> states) {
        for (State<T> state : states) {
            RecordWriter<T> writer =
                    createWriter(
                            state.partition,
                            state.bucket,
                            state.dataFiles,
                            state.commitIncrement,
                            compactExecutor());
            notifyNewWriter(writer);
            WriterContainer<T> writerContainer =
                    new WriterContainer<>(writer, state.indexMaintainer, state.baseSnapshotId);
            writerContainer.lastModifiedCommitIdentifier = state.lastModifiedCommitIdentifier;
            writers.computeIfAbsent(state.partition, k -> new HashMap<>())
                    .put(state.bucket, writerContainer);
        }
    }

    private WriterContainer<T> getWriterWrapper(BinaryRow partition, int bucket) {
        Map<Integer, WriterContainer<T>> buckets = writers.get(partition);
        if (buckets == null) {
            buckets = new HashMap<>();
            writers.put(partition.copy(), buckets);
        }
        return buckets.computeIfAbsent(
                bucket, k -> createWriterContainer(partition.copy(), bucket, emptyWriter));
    }

    @VisibleForTesting
    public WriterContainer<T> createWriterContainer(
            BinaryRow partition, int bucket, boolean emptyWriter) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating writer for partition {}, bucket {}", partition, bucket);
        }

        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        List<DataFileMeta> restoreFiles = new ArrayList<>();
        if (!emptyWriter && latestSnapshotId != null) {
            restoreFiles = scanExistingFileMetas(latestSnapshotId, partition, bucket);
        }
        IndexMaintainer<T> indexMaintainer =
                indexFactory == null
                        ? null
                        : indexFactory.createOrRestore(latestSnapshotId, partition, bucket);
        RecordWriter<T> writer =
                createWriter(partition.copy(), bucket, restoreFiles, null, compactExecutor());
        notifyNewWriter(writer);
        return new WriterContainer<>(writer, indexMaintainer, latestSnapshotId);
    }

    private List<DataFileMeta> scanExistingFileMetas(
            long snapshotId, BinaryRow partition, int bucket) {
        List<DataFileMeta> existingFileMetas = new ArrayList<>();
        // Concat all the DataFileMeta of existing files into existingFileMetas.
        scan.withSnapshot(snapshotId).withPartitionBucket(partition, bucket).plan().files().stream()
                .map(ManifestEntry::file)
                .forEach(existingFileMetas::add);
        return existingFileMetas;
    }

    private ExecutorService compactExecutor() {
        if (lazyCompactExecutor == null) {
            lazyCompactExecutor =
                    Executors.newSingleThreadScheduledExecutor(
                            new ExecutorThreadFactory(
                                    Thread.currentThread().getName() + "-compaction"));
        }
        return lazyCompactExecutor;
    }

    protected void notifyNewWriter(RecordWriter<T> writer) {}

    protected abstract RecordWriter<T> createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor);

    /**
     * {@link RecordWriter} with the snapshot id it is created upon and the identifier of its last
     * modified commit.
     */
    @VisibleForTesting
    public static class WriterContainer<T> {
        public final RecordWriter<T> writer;
        @Nullable public final IndexMaintainer<T> indexMaintainer;
        protected final long baseSnapshotId;
        protected long lastModifiedCommitIdentifier;

        protected WriterContainer(
                RecordWriter<T> writer,
                @Nullable IndexMaintainer<T> indexMaintainer,
                Long baseSnapshotId) {
            this.writer = writer;
            this.indexMaintainer = indexMaintainer;
            this.baseSnapshotId =
                    baseSnapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID - 1 : baseSnapshotId;
            this.lastModifiedCommitIdentifier = Long.MIN_VALUE;
        }
    }

    /** Recoverable state of {@link AbstractFileStoreWrite}. */
    public static class State<T> {
        protected final BinaryRow partition;
        protected final int bucket;

        protected final long baseSnapshotId;
        protected final long lastModifiedCommitIdentifier;
        protected final List<DataFileMeta> dataFiles;
        @Nullable protected final IndexMaintainer<T> indexMaintainer;
        protected final CommitIncrement commitIncrement;

        protected State(
                BinaryRow partition,
                int bucket,
                long baseSnapshotId,
                long lastModifiedCommitIdentifier,
                Collection<DataFileMeta> dataFiles,
                @Nullable IndexMaintainer<T> indexMaintainer,
                CommitIncrement commitIncrement) {
            this.partition = partition;
            this.bucket = bucket;
            this.baseSnapshotId = baseSnapshotId;
            this.lastModifiedCommitIdentifier = lastModifiedCommitIdentifier;
            this.dataFiles = new ArrayList<>(dataFiles);
            this.indexMaintainer = indexMaintainer;
            this.commitIncrement = commitIncrement;
        }

        @Override
        public String toString() {
            return String.format(
                    "{%s, %d, %d, %d, %s, %s, %s}",
                    partition,
                    bucket,
                    baseSnapshotId,
                    lastModifiedCommitIdentifier,
                    dataFiles,
                    indexMaintainer,
                    commitIncrement);
        }
    }
}
