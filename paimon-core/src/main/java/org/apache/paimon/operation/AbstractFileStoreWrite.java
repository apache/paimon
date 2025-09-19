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
import org.apache.paimon.KeyValue;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.DynamicBucketIndexMaintainer;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
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
import java.util.function.Function;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.apache.paimon.io.DataFileMeta.getMaxSequenceNumber;
import static org.apache.paimon.shade.guava30.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.paimon.utils.FileStorePathFactory.getPartitionComputer;

/**
 * Base {@link FileStoreWrite} implementation.
 *
 * @param <T> type of record to write.
 */
public abstract class AbstractFileStoreWrite<T> implements FileStoreWrite<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFileStoreWrite.class);

    private final int writerNumberMax;
    @Nullable private final DynamicBucketIndexMaintainer.Factory dbMaintainerFactory;
    @Nullable private final BucketedDvMaintainer.Factory dvMaintainerFactory;
    private final int numBuckets;
    private final RowType partitionType;

    @Nullable protected IOManager ioManager;

    protected final Map<BinaryRow, Map<Integer, WriterContainer<T>>> writers;

    protected WriteRestore restore;
    private ExecutorService lazyCompactExecutor;
    private boolean closeCompactExecutorWhenLeaving = true;
    private boolean ignorePreviousFiles = false;
    private boolean ignoreNumBucketCheck = false;

    protected CompactionMetrics compactionMetrics = null;
    protected final String tableName;
    private final boolean legacyPartitionName;

    protected AbstractFileStoreWrite(
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            @Nullable DynamicBucketIndexMaintainer.Factory dbMaintainerFactory,
            @Nullable BucketedDvMaintainer.Factory dvMaintainerFactory,
            String tableName,
            CoreOptions options,
            RowType partitionType) {
        IndexFileHandler indexFileHandler = null;
        if (dbMaintainerFactory != null) {
            indexFileHandler = dbMaintainerFactory.indexFileHandler();
        } else if (dvMaintainerFactory != null) {
            indexFileHandler = dvMaintainerFactory.indexFileHandler();
        }
        this.restore = new FileSystemWriteRestore(options, snapshotManager, scan, indexFileHandler);
        this.dbMaintainerFactory = dbMaintainerFactory;
        this.dvMaintainerFactory = dvMaintainerFactory;
        this.numBuckets = options.bucket();
        this.partitionType = partitionType;
        this.writers = new HashMap<>();
        this.tableName = tableName;
        this.writerNumberMax = options.writeMaxWritersToSpill();
        this.legacyPartitionName = options.legacyPartitionName();
    }

    @Override
    public FileStoreWrite<T> withWriteRestore(WriteRestore writeRestore) {
        this.restore = writeRestore;
        return this;
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
    public void withIgnoreNumBucketCheck(boolean ignoreNumBucketCheck) {
        this.ignoreNumBucketCheck = ignoreNumBucketCheck;
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
        if (container.dynamicBucketMaintainer != null) {
            container.dynamicBucketMaintainer.notifyNewRecord((KeyValue) data);
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
        Function<WriterContainer<T>, Boolean> writerCleanChecker;
        if (writers.values().stream()
                        .map(Map::values)
                        .flatMap(Collection::stream)
                        .mapToLong(w -> w.lastModifiedCommitIdentifier)
                        .max()
                        .orElse(Long.MIN_VALUE)
                == Long.MIN_VALUE) {
            // If this is the first commit, no writer should be cleaned.
            writerCleanChecker = writerContainer -> false;
        } else {
            writerCleanChecker = createWriterCleanChecker();
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
                DataIncrement newFilesIncrement = increment.newFilesIncrement();
                CompactIncrement compactIncrement = increment.compactIncrement();
                if (writerContainer.dynamicBucketMaintainer != null) {
                    newFilesIncrement
                            .newIndexFiles()
                            .addAll(writerContainer.dynamicBucketMaintainer.prepareCommit());
                }
                CompactDeletionFile compactDeletionFile = increment.compactDeletionFile();
                if (compactDeletionFile != null) {
                    compactDeletionFile
                            .getOrCompute()
                            .ifPresent(compactIncrement.newIndexFiles()::add);
                }
                CommitMessageImpl committable =
                        new CommitMessageImpl(
                                partition,
                                bucket,
                                writerContainer.totalBuckets,
                                newFilesIncrement,
                                compactIncrement);
                result.add(committable);

                if (committable.isEmpty()) {
                    if (writerCleanChecker.apply(writerContainer)) {
                        // Clear writer if no update, and if its latest modification has committed.
                        //
                        // We need a mechanism to clear writers, otherwise there will be more and
                        // more such as yesterday's partition that no longer needs to be written.
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Closing writer for partition {}, bucket {}. "
                                            + "Writer's last modified identifier is {}, "
                                            + "while current commit identifier is {}.",
                                    partition,
                                    bucket,
                                    writerContainer.lastModifiedCommitIdentifier,
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

    // This abstract function returns a whole function (instead of just a boolean value),
    // because we do not want to introduce `commitUser` into this base class.
    //
    // For writers with no conflicts, `commitUser` might be some random value.
    protected abstract Function<WriterContainer<T>, Boolean> createWriterCleanChecker();

    protected static <T>
            Function<WriterContainer<T>, Boolean> createConflictAwareWriterCleanChecker(
                    String commitUser, WriteRestore restore) {
        long latestCommittedIdentifier = restore.latestCommittedIdentifier(commitUser);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Latest committed identifier is {}", latestCommittedIdentifier);
        }

        // Condition 1: There is no more record waiting to be committed. Note that the
        // condition is < (instead of <=), because each commit identifier may have
        // multiple snapshots. We must make sure all snapshots of this identifier are
        // committed.
        //
        // Condition 2: No compaction is in progress. That is, no more changelog will be
        // produced.
        //
        // Condition 3: The writer has no postponed compaction like gentle lookup compaction.
        return writerContainer ->
                writerContainer.lastModifiedCommitIdentifier < latestCommittedIdentifier
                        && !writerContainer.writer.compactNotCompleted();
    }

    protected static <T>
            Function<WriterContainer<T>, Boolean> createNoConflictAwareWriterCleanChecker() {
        return writerContainer -> true;
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
                                writerContainer.totalBuckets,
                                writerContainer.baseSnapshotId,
                                writerContainer.lastModifiedCommitIdentifier,
                                dataFiles,
                                writerContainer.writer.maxSequenceNumber(),
                                writerContainer.dynamicBucketMaintainer,
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
                            state.totalBuckets,
                            state.indexMaintainer,
                            state.deletionVectorsMaintainer,
                            state.baseSnapshotId);
            writerContainer.lastModifiedCommitIdentifier = state.lastModifiedCommitIdentifier;
            writers.computeIfAbsent(state.partition, k -> new HashMap<>())
                    .put(state.bucket, writerContainer);
        }
    }

    public Map<BinaryRow, List<Integer>> getActiveBuckets() {
        Map<BinaryRow, List<Integer>> result = new HashMap<>();
        for (Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>> partitions :
                writers.entrySet()) {
            result.put(partitions.getKey(), new ArrayList<>(partitions.getValue().keySet()));
        }
        return result;
    }

    protected WriterContainer<T> getWriterWrapper(BinaryRow partition, int bucket) {
        Map<Integer, WriterContainer<T>> buckets = writers.get(partition);
        if (buckets == null) {
            buckets = new HashMap<>();
            writers.put(partition.copy(), buckets);
        }
        return buckets.computeIfAbsent(
                bucket, k -> createWriterContainer(partition.copy(), bucket));
    }

    public RecordWriter<T> createWriter(BinaryRow partition, int bucket) {
        return createWriterContainer(partition, bucket).writer;
    }

    public WriterContainer<T> createWriterContainer(BinaryRow partition, int bucket) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating writer for partition {}, bucket {}", partition, bucket);
        }

        if (writerNumber() >= writerNumberMax) {
            try {
                forceBufferSpill();
            } catch (Exception e) {
                throw new RuntimeException("Error happens while force buffer spill", e);
            }
        }

        RestoreFiles restored = RestoreFiles.empty();
        if (!ignorePreviousFiles) {
            restored = scanExistingFileMetas(partition, bucket);
        }

        DynamicBucketIndexMaintainer indexMaintainer =
                dbMaintainerFactory == null
                        ? null
                        : dbMaintainerFactory.create(
                                partition, bucket, restored.dynamicBucketIndex());
        BucketedDvMaintainer dvMaintainer =
                dvMaintainerFactory == null
                        ? null
                        : dvMaintainerFactory.create(
                                partition, bucket, restored.deleteVectorsIndex());

        List<DataFileMeta> restoreFiles = restored.dataFiles();
        if (restoreFiles == null) {
            restoreFiles = new ArrayList<>();
        }
        RecordWriter<T> writer =
                createWriter(
                        partition.copy(),
                        bucket,
                        restoreFiles,
                        getMaxSequenceNumber(restoreFiles),
                        null,
                        compactExecutor(),
                        dvMaintainer);
        notifyNewWriter(writer);

        Snapshot previousSnapshot = restored.snapshot();
        return new WriterContainer<>(
                writer,
                firstNonNull(restored.totalBuckets(), numBuckets),
                indexMaintainer,
                dvMaintainer,
                previousSnapshot == null ? null : previousSnapshot.id());
    }

    private long writerNumber() {
        return writers.values().stream().mapToLong(Map::size).sum();
    }

    @Override
    public FileStoreWrite<T> withMetricRegistry(MetricRegistry metricRegistry) {
        this.compactionMetrics = new CompactionMetrics(metricRegistry, tableName);
        return this;
    }

    private RestoreFiles scanExistingFileMetas(BinaryRow partition, int bucket) {
        RestoreFiles restored =
                restore.restoreFiles(
                        partition,
                        bucket,
                        dbMaintainerFactory != null,
                        dvMaintainerFactory != null);
        Integer restoredTotalBuckets = restored.totalBuckets();
        int totalBuckets = numBuckets;
        if (restoredTotalBuckets != null) {
            totalBuckets = restoredTotalBuckets;
        }
        if (!ignoreNumBucketCheck && totalBuckets != numBuckets) {
            String partInfo =
                    partitionType.getFieldCount() > 0
                            ? "partition "
                                    + getPartitionComputer(
                                                    partitionType,
                                                    PARTITION_DEFAULT_NAME.defaultValue(),
                                                    legacyPartitionName)
                                            .generatePartValues(partition)
                            : "table";
            throw new RuntimeException(
                    String.format(
                            "Try to write %s with a new bucket num %d, but the previous bucket num is %d. "
                                    + "Please switch to batch mode, and perform INSERT OVERWRITE to rescale current data layout first.",
                            partInfo, numBuckets, totalBuckets));
        }
        return restored;
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
            @Nullable BucketedDvMaintainer deletionVectorsMaintainer);

    // force buffer spill to avoid out of memory in batch mode
    protected void forceBufferSpill() throws Exception {}

    /**
     * {@link RecordWriter} with the snapshot id it is created upon and the identifier of its last
     * modified commit.
     */
    @VisibleForTesting
    public static class WriterContainer<T> {
        public final RecordWriter<T> writer;
        public final int totalBuckets;
        @Nullable public final DynamicBucketIndexMaintainer dynamicBucketMaintainer;
        @Nullable public final BucketedDvMaintainer deletionVectorsMaintainer;
        protected final long baseSnapshotId;
        protected long lastModifiedCommitIdentifier;

        protected WriterContainer(
                RecordWriter<T> writer,
                int totalBuckets,
                @Nullable DynamicBucketIndexMaintainer dynamicBucketMaintainer,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer,
                Long baseSnapshotId) {
            this.writer = writer;
            this.totalBuckets = totalBuckets;
            this.dynamicBucketMaintainer = dynamicBucketMaintainer;
            this.deletionVectorsMaintainer = deletionVectorsMaintainer;
            this.baseSnapshotId =
                    baseSnapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID - 1 : baseSnapshotId;
            this.lastModifiedCommitIdentifier = Long.MIN_VALUE;
        }
    }

    @VisibleForTesting
    public Map<BinaryRow, Map<Integer, WriterContainer<T>>> writers() {
        return writers;
    }

    @VisibleForTesting
    public CompactionMetrics compactionMetrics() {
        return compactionMetrics;
    }
}
