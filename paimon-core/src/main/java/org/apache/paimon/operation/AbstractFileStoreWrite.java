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
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.RecordWriter;
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

import static org.apache.paimon.io.DataFileMeta.getMaxSequenceNumber;

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
    private final int writerNumberMax;
    @Nullable private final IndexMaintainer.Factory<T> indexFactory;
    @Nullable private final DeletionVectorsMaintainer.Factory deletionVectorsMaintainerFactory;

    @Nullable protected IOManager ioManager;

    protected final Map<BinaryRow, Map<Integer, WriterContainer<T>>> writers;

    private ExecutorService lazyCompactExecutor;
    private boolean closeCompactExecutorWhenLeaving = true;
    private boolean ignorePreviousFiles = false;
    protected boolean isStreamingMode = false;

    protected CompactionMetrics compactionMetrics = null;
    protected final String tableName;

    protected AbstractFileStoreWrite(
            String commitUser,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            @Nullable IndexMaintainer.Factory<T> indexFactory,
            @Nullable DeletionVectorsMaintainer.Factory deletionVectorsMaintainerFactory,
            String tableName,
            int writerNumberMax) {
        this.commitUser = commitUser;
        this.snapshotManager = snapshotManager;
        this.scan = scan;
        this.indexFactory = indexFactory;
        this.deletionVectorsMaintainerFactory = deletionVectorsMaintainerFactory;
        this.writers = new HashMap<>();
        this.tableName = tableName;
        this.writerNumberMax = writerNumberMax;
    }

    @Override
    public FileStoreWrite<T> withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public FileStoreWrite<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        return this;
    }

    @Override
    public void withIgnorePreviousFiles(boolean ignorePreviousFiles) {
        this.ignorePreviousFiles = ignorePreviousFiles;
    }

    @Override
    public void withCompactExecutor(ExecutorService compactExecutor) {
        this.lazyCompactExecutor = compactExecutor;
        this.closeCompactExecutorWhenLeaving = false;
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
                    newIndexFiles.addAll(writerContainer.indexMaintainer.prepareCommit());
                }
                if (writerContainer.deletionVectorsMaintainer != null) {
                    newIndexFiles.addAll(writerContainer.deletionVectorsMaintainer.prepareCommit());
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
                    // Condition 1: There is no more record waiting to be committed.
                    // Condition 2: No compaction is in progress. That is, no more changelog will be
                    // produced.
                    if (writerContainer.lastModifiedCommitIdentifier <= latestCommittedIdentifier
                            && !writerContainer.writer.isCompacting()) {
                        // Clear writer if no update, and if its latest modification has committed.
                        //
                        // We need a mechanism to clear writers, otherwise there will be more and
                        // more such as yesterday's partition that no longer needs to be written.
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Closing writer for partition {}, bucket {}. "
                                            + "Writer's last modified identifier is {}, "
                                            + "while latest committed identifier is {}, "
                                            + "current commit identifier is {}.",
                                    partition,
                                    bucket,
                                    writerContainer.lastModifiedCommitIdentifier,
                                    latestCommittedIdentifier,
                                    commitIdentifier);
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
        if (lazyCompactExecutor != null && closeCompactExecutorWhenLeaving) {
            lazyCompactExecutor.shutdownNow();
        }
        if (compactionMetrics != null) {
            compactionMetrics.close();
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
                                writerContainer.writer.maxSequenceNumber(),
                                writerContainer.indexMaintainer,
                                writerContainer.deletionVectorsMaintainer,
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
                            state.maxSequenceNumber,
                            state.commitIncrement,
                            compactExecutor(),
                            state.deletionVectorsMaintainer);
            notifyNewWriter(writer);
            WriterContainer<T> writerContainer =
                    new WriterContainer<>(
                            writer,
                            state.indexMaintainer,
                            state.deletionVectorsMaintainer,
                            state.baseSnapshotId);
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
                bucket, k -> createWriterContainer(partition.copy(), bucket, ignorePreviousFiles));
    }

    private long writerNumber() {
        return writers.values().stream().mapToLong(e -> e.values().size()).sum();
    }

    @VisibleForTesting
    public WriterContainer<T> createWriterContainer(
            BinaryRow partition, int bucket, boolean ignorePreviousFiles) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating writer for partition {}, bucket {}", partition, bucket);
        }

        if (!isStreamingMode && writerNumber() >= writerNumberMax) {
            try {
                forceBufferSpill();
            } catch (Exception e) {
                throw new RuntimeException("Error happens while force buffer spill", e);
            }
        }

        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        List<DataFileMeta> restoreFiles = new ArrayList<>();
        if (!ignorePreviousFiles && latestSnapshotId != null) {
            restoreFiles = scanExistingFileMetas(latestSnapshotId, partition, bucket);
        }
        IndexMaintainer<T> indexMaintainer =
                indexFactory == null
                        ? null
                        : indexFactory.createOrRestore(
                                ignorePreviousFiles ? null : latestSnapshotId, partition, bucket);
        DeletionVectorsMaintainer deletionVectorsMaintainer =
                deletionVectorsMaintainerFactory == null
                        ? null
                        : deletionVectorsMaintainerFactory.createOrRestore(
                                ignorePreviousFiles ? null : latestSnapshotId, partition, bucket);
        RecordWriter<T> writer =
                createWriter(
                        partition.copy(),
                        bucket,
                        restoreFiles,
                        getMaxSequenceNumber(restoreFiles),
                        null,
                        compactExecutor(),
                        deletionVectorsMaintainer);
        notifyNewWriter(writer);
        return new WriterContainer<>(
                writer, indexMaintainer, deletionVectorsMaintainer, latestSnapshotId);
    }

    @Override
    public void withExecutionMode(boolean isStreamingMode) {
        this.isStreamingMode = isStreamingMode;
    }

    @Override
    public FileStoreWrite<T> withMetricRegistry(MetricRegistry metricRegistry) {
        this.compactionMetrics = new CompactionMetrics(metricRegistry, tableName);
        return this;
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

    @VisibleForTesting
    public ExecutorService getCompactExecutor() {
        return lazyCompactExecutor;
    }

    protected void notifyNewWriter(RecordWriter<T> writer) {}

    protected abstract RecordWriter<T> createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            long restoredMaxSeqNumber,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor,
            @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer);

    // force buffer spill to avoid out of memory in batch mode
    protected void forceBufferSpill() throws Exception {}

    /**
     * {@link RecordWriter} with the snapshot id it is created upon and the identifier of its last
     * modified commit.
     */
    @VisibleForTesting
    public static class WriterContainer<T> {
        public final RecordWriter<T> writer;
        @Nullable public final IndexMaintainer<T> indexMaintainer;
        @Nullable public final DeletionVectorsMaintainer deletionVectorsMaintainer;
        protected final long baseSnapshotId;
        protected long lastModifiedCommitIdentifier;

        protected WriterContainer(
                RecordWriter<T> writer,
                @Nullable IndexMaintainer<T> indexMaintainer,
                @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer,
                Long baseSnapshotId) {
            this.writer = writer;
            this.indexMaintainer = indexMaintainer;
            this.deletionVectorsMaintainer = deletionVectorsMaintainer;
            this.baseSnapshotId =
                    baseSnapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID - 1 : baseSnapshotId;
            this.lastModifiedCommitIdentifier = Long.MIN_VALUE;
        }
    }

    @VisibleForTesting
    Map<BinaryRow, Map<Integer, WriterContainer<T>>> writers() {
        return writers;
    }
}
