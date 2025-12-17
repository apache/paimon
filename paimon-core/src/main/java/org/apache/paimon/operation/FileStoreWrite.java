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

import org.apache.paimon.FileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.DynamicBucketIndexMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.Restorable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Write operation which provides {@link RecordWriter} creation and writes {@link SinkRecord} to
 * {@link FileStore}.
 *
 * @param <T> type of record to write.
 */
public interface FileStoreWrite<T> extends Restorable<List<FileStoreWrite.State<T>>> {

    FileStoreWrite<T> withWriteRestore(WriteRestore writeRestore);

    FileStoreWrite<T> withIOManager(IOManager ioManager);

    /** Specified the write rowType. */
    default void withWriteType(RowType writeType) {
        throw new UnsupportedOperationException();
    }

    /**
     * With memory pool factory for the current file store write.
     *
     * @param memoryPoolFactory the given memory pool factory.
     */
    FileStoreWrite<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory);

    /**
     * Set whether the write operation should ignore previously stored files.
     *
     * @param ignorePreviousFiles whether the write operation should ignore previously stored files.
     */
    void withIgnorePreviousFiles(boolean ignorePreviousFiles);

    /**
     * Ignores the check that the written partition must have the same number of buckets with the
     * table option.
     */
    void withIgnoreNumBucketCheck(boolean ignoreNumBucketCheck);

    /** With metrics to measure compaction. */
    FileStoreWrite<T> withMetricRegistry(MetricRegistry metricRegistry);

    void withCompactExecutor(ExecutorService compactExecutor);

    default void specifyDataFileFormat(String fileFormat) {
        throw new UnsupportedOperationException();
    }

    /**
     * Write the data to the store according to the partition and bucket.
     *
     * @param partition the partition of the data
     * @param bucket the bucket id of the data
     * @param data the given data
     * @throws Exception the thrown exception when writing the record
     */
    void write(BinaryRow partition, int bucket, T data) throws Exception;

    /**
     * Compact data stored in given partition and bucket. Note that compaction process is only
     * submitted and may not be completed when the method returns.
     *
     * @param partition the partition to compact
     * @param bucket the bucket to compact
     * @param fullCompaction whether to trigger full compaction or just normal compaction
     * @throws Exception the thrown exception when compacting the records
     */
    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    /**
     * Notify that some new files are created at given snapshot in given bucket.
     *
     * <p>Most probably, these files are created by another job. Currently this method is only used
     * by the dedicated compact job to see files created by writer jobs.
     *
     * @param snapshotId the snapshot id where new files are created
     * @param partition the partition where new files are created
     * @param bucket the bucket where new files are created
     * @param files the new files themselves
     */
    void notifyNewFiles(long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files);

    /**
     * Prepare commit in the write.
     *
     * @param waitCompaction if this method need to wait for current compaction to complete
     * @param commitIdentifier identifier of the commit being prepared
     * @return the file committable list
     * @throws Exception the thrown exception
     */
    List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception;

    /**
     * Close the writer.
     *
     * @throws Exception the thrown exception
     */
    void close() throws Exception;

    /** Recoverable state of {@link FileStoreWrite}. */
    class State<T> {

        protected final BinaryRow partition;
        protected final int bucket;
        protected final int totalBuckets;

        protected final long baseSnapshotId;
        protected final long lastModifiedCommitIdentifier;
        protected final List<DataFileMeta> dataFiles;
        protected final long maxSequenceNumber;
        @Nullable protected final DynamicBucketIndexMaintainer indexMaintainer;
        @Nullable protected final BucketedDvMaintainer deletionVectorsMaintainer;
        protected final CommitIncrement commitIncrement;

        protected State(
                BinaryRow partition,
                int bucket,
                int totalBuckets,
                long baseSnapshotId,
                long lastModifiedCommitIdentifier,
                Collection<DataFileMeta> dataFiles,
                long maxSequenceNumber,
                @Nullable DynamicBucketIndexMaintainer indexMaintainer,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer,
                CommitIncrement commitIncrement) {
            this.partition = partition;
            this.bucket = bucket;
            this.totalBuckets = totalBuckets;
            this.baseSnapshotId = baseSnapshotId;
            this.lastModifiedCommitIdentifier = lastModifiedCommitIdentifier;
            this.dataFiles = new ArrayList<>(dataFiles);
            this.maxSequenceNumber = maxSequenceNumber;
            this.indexMaintainer = indexMaintainer;
            this.deletionVectorsMaintainer = deletionVectorsMaintainer;
            this.commitIncrement = commitIncrement;
        }

        @Override
        public String toString() {
            return String.format(
                    "{%s, %d, %d, %d, %d, %s, %d, %s, %s, %s}",
                    partition,
                    bucket,
                    totalBuckets,
                    baseSnapshotId,
                    lastModifiedCommitIdentifier,
                    dataFiles,
                    maxSequenceNumber,
                    indexMaintainer,
                    deletionVectorsMaintainer,
                    commitIncrement);
        }
    }
}
