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

package org.apache.paimon.table.sink;

import org.apache.paimon.FileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.casting.DefaultValueRow;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.BundleFileStoreWriter;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.operation.FileStoreWrite.State;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Restorable;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * {@link TableWrite} implementation.
 *
 * @param <T> type of record to write into {@link FileStore}.
 */
public class TableWriteImpl<T> implements InnerTableWrite, Restorable<List<State<T>>> {

    private final RowType rowType;
    private final FileStoreWrite<T> write;
    private final KeyAndBucketExtractor<InternalRow> keyAndBucketExtractor;
    private final RecordExtractor<T> recordExtractor;
    @Nullable private final RowKindGenerator rowKindGenerator;
    private final boolean ignoreDelete;

    private boolean batchCommitted = false;
    private BucketMode bucketMode;

    private final int[] notNullFieldIndex;
    private final @Nullable DefaultValueRow defaultValueRow;

    public TableWriteImpl(
            RowType rowType,
            FileStoreWrite<T> write,
            KeyAndBucketExtractor<InternalRow> keyAndBucketExtractor,
            RecordExtractor<T> recordExtractor,
            @Nullable RowKindGenerator rowKindGenerator,
            boolean ignoreDelete) {
        this.rowType = rowType;
        this.write = write;
        this.keyAndBucketExtractor = keyAndBucketExtractor;
        this.recordExtractor = recordExtractor;
        this.rowKindGenerator = rowKindGenerator;
        this.ignoreDelete = ignoreDelete;

        List<String> notNullColumnNames =
                rowType.getFields().stream()
                        .filter(field -> !field.type().isNullable())
                        .map(DataField::name)
                        .collect(Collectors.toList());
        this.notNullFieldIndex = rowType.getFieldIndices(notNullColumnNames);
        this.defaultValueRow = DefaultValueRow.create(rowType);
    }

    @Override
    public InnerTableWrite withWriteRestore(WriteRestore writeRestore) {
        this.write.withWriteRestore(writeRestore);
        return this;
    }

    @Override
    public TableWriteImpl<T> withIgnorePreviousFiles(boolean ignorePreviousFiles) {
        write.withIgnorePreviousFiles(ignorePreviousFiles);
        return this;
    }

    @Override
    public TableWriteImpl<T> withIOManager(IOManager ioManager) {
        write.withIOManager(ioManager);
        return this;
    }

    @Override
    public TableWriteImpl<T> withMemoryPool(MemorySegmentPool memoryPool) {
        write.withMemoryPool(memoryPool);
        return this;
    }

    public TableWriteImpl<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        write.withMemoryPoolFactory(memoryPoolFactory);
        return this;
    }

    public TableWriteImpl<T> withCompactExecutor(ExecutorService compactExecutor) {
        write.withCompactExecutor(compactExecutor);
        return this;
    }

    public TableWriteImpl<T> withBucketMode(BucketMode bucketMode) {
        this.bucketMode = bucketMode;
        return this;
    }

    @Override
    public BinaryRow getPartition(InternalRow row) {
        keyAndBucketExtractor.setRecord(row);
        return keyAndBucketExtractor.partition();
    }

    @Override
    public int getBucket(InternalRow row) {
        keyAndBucketExtractor.setRecord(row);
        return keyAndBucketExtractor.bucket();
    }

    @Override
    public void write(InternalRow row) throws Exception {
        writeAndReturn(row);
    }

    @Override
    public void write(InternalRow row, int bucket) throws Exception {
        writeAndReturn(row, bucket);
    }

    @Override
    public void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle)
            throws Exception {
        if (write instanceof BundleFileStoreWriter) {
            ((BundleFileStoreWriter) write).writeBundle(partition, bucket, bundle);
        } else {
            for (InternalRow row : bundle) {
                write(row, bucket);
            }
        }
    }

    @Nullable
    public SinkRecord writeAndReturn(InternalRow row) throws Exception {
        checkNullability(row);
        row = wrapDefaultValue(row);
        RowKind rowKind = RowKindGenerator.getRowKind(rowKindGenerator, row);
        if (ignoreDelete && rowKind.isRetract()) {
            return null;
        }
        SinkRecord record = toSinkRecord(row);
        write.write(record.partition(), record.bucket(), recordExtractor.extract(record, rowKind));
        return record;
    }

    @Nullable
    public SinkRecord writeAndReturn(InternalRow row, int bucket) throws Exception {
        checkNullability(row);
        RowKind rowKind = RowKindGenerator.getRowKind(rowKindGenerator, row);
        if (ignoreDelete && rowKind.isRetract()) {
            return null;
        }
        SinkRecord record = toSinkRecord(row, bucket);
        write.write(record.partition(), bucket, recordExtractor.extract(record, rowKind));
        return record;
    }

    private void checkNullability(InternalRow row) {
        for (int idx : notNullFieldIndex) {
            if (row.isNullAt(idx)) {
                String columnName = rowType.getFields().get(idx).name();
                throw new RuntimeException(
                        String.format("Cannot write null to non-null column(%s)", columnName));
            }
        }
    }

    private InternalRow wrapDefaultValue(InternalRow row) {
        return defaultValueRow == null ? row : defaultValueRow.replaceRow(row);
    }

    private SinkRecord toSinkRecord(InternalRow row) {
        keyAndBucketExtractor.setRecord(row);
        return new SinkRecord(
                keyAndBucketExtractor.partition(),
                keyAndBucketExtractor.bucket(),
                keyAndBucketExtractor.trimmedPrimaryKey(),
                row);
    }

    private SinkRecord toSinkRecord(InternalRow row, int bucket) {
        keyAndBucketExtractor.setRecord(row);
        return new SinkRecord(
                keyAndBucketExtractor.partition(),
                bucket,
                keyAndBucketExtractor.trimmedPrimaryKey(),
                row);
    }

    public SinkRecord toLogRecord(SinkRecord record) {
        keyAndBucketExtractor.setRecord(record.row());
        return new SinkRecord(
                record.partition(),
                bucketMode == BucketMode.BUCKET_UNAWARE ? -1 : record.bucket(),
                keyAndBucketExtractor.logPrimaryKey(),
                record.row());
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        write.compact(partition, bucket, fullCompaction);
    }

    @Override
    public TableWriteImpl<T> withMetricRegistry(MetricRegistry metricRegistry) {
        write.withMetricRegistry(metricRegistry);
        return this;
    }

    /**
     * Notify that some new files are created at given snapshot in given bucket.
     *
     * <p>Most probably, these files are created by another job. Currently this method is only used
     * by the dedicated compact job to see files created by writer jobs.
     */
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        write.notifyNewFiles(snapshotId, partition, bucket, files);
    }

    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception {
        return write.prepareCommit(waitCompaction, commitIdentifier);
    }

    @Override
    public List<CommitMessage> prepareCommit() throws Exception {
        checkState(!batchCommitted, "BatchTableWrite only support one-time committing.");
        batchCommitted = true;
        return prepareCommit(true, BatchWriteBuilder.COMMIT_IDENTIFIER);
    }

    @Override
    public void close() throws Exception {
        write.close();
    }

    @Override
    public List<State<T>> checkpoint() {
        return write.checkpoint();
    }

    @Override
    public void restore(List<State<T>> state) {
        write.restore(state);
    }

    @VisibleForTesting
    public FileStoreWrite<T> getWrite() {
        return write;
    }

    /** Extractor to extract {@link T} from the {@link SinkRecord}. */
    public interface RecordExtractor<T> {

        T extract(SinkRecord record, RowKind rowKind);
    }
}
