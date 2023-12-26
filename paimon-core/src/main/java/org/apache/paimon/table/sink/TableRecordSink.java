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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.NextIterator;
import org.apache.paimon.utils.OffsetRow;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.schema.SystemColumns.SEQUENCE_NUMBER;
import static org.apache.paimon.schema.SystemColumns.VALUE_KIND;

/**
 * Sort the given {@link InternalRow} and bucket with specified field. Order with the sort is, given
 * fields, bucket, orderNumber.
 *
 * <p>For example, table (f0 String, f1 int, f2 int), we sort rows by f2 and bucket field is f1 :
 *
 * <pre>
 *    a, 3, 1
 *    a, 2, 1
 *    a, 3, 100
 *    a, 2, 0
 * </pre>
 *
 * <p>We get:
 *
 * <pre>
 *     a, 2, 0
 *     a, 2, 1
 *     a, 3, 1
 *     a, 3, 100
 * </pre>
 *
 * <p>The sorter will spill the data to disk.
 */
public class TableRecordSink<T> implements FileStoreWrite<T> {

    private final FileStoreWrite<T> write;
    private final KeyAndBucketExtractor<InternalRow> keyAndBucketExtractor;
    private final TableWriteImpl.RecordExtractor<T> recordExtractor;
    private final TableSchema schema;
    private final int maxWritersInBatch;

    @Nullable private BatchBucketSorter sorter;

    private boolean streamingMode = false;

    public TableRecordSink(
            FileStoreWrite<T> write,
            TableWriteImpl.RecordExtractor<T> recordExtractor,
            KeyAndBucketExtractor<InternalRow> keyAndBucketExtractor,
            TableSchema schema) {
        this.write = write;
        this.keyAndBucketExtractor = keyAndBucketExtractor;
        this.recordExtractor = recordExtractor;
        this.schema = schema;

        this.maxWritersInBatch =
                Integer.parseInt(
                        schema.options()
                                .getOrDefault(
                                        CoreOptions.WRITE_BATCH_MAX_WRITERS.key(),
                                        CoreOptions.WRITE_BATCH_MAX_WRITERS
                                                .defaultValue()
                                                .toString()));
    }

    private boolean inBatch() {
        return !streamingMode;
    }

    @Override
    public FileStoreWrite<T> withIOManager(IOManager ioManager) {
        sorter = new BatchBucketSorter(ioManager, keyAndBucketExtractor, schema);
        return write.withIOManager(ioManager);
    }

    @Override
    public FileStoreWrite<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        write.withMemoryPoolFactory(memoryPoolFactory);
        return this;
    }

    @Override
    public void withIgnorePreviousFiles(boolean ignorePreviousFiles) {
        write.withIgnorePreviousFiles(ignorePreviousFiles);
    }

    @Override
    public void withExecutionMode(boolean isStreamingMode) {
        this.streamingMode = isStreamingMode;
        write.withExecutionMode(isStreamingMode);
    }

    @Override
    public FileStoreWrite<T> withMetricRegistry(MetricRegistry metricRegistry) {
        write.withMetricRegistry(metricRegistry);
        return this;
    }

    @Override
    public void withCompactExecutor(ExecutorService compactExecutor) {
        write.withCompactExecutor(compactExecutor);
    }

    public void write(SinkRecord sinkRecord) throws Exception {
        BinaryRow partition = sinkRecord.partition();
        int bucket = sinkRecord.bucket();
        InternalRow data = sinkRecord.row();

        if (inBatch() && sorter != null && write.aliveWriters() >= maxWritersInBatch) {
            sorter.write(partition, bucket, data);
        } else {
            write.write(partition, bucket, recordExtractor.extract(sinkRecord));
        }
    }

    @Override
    public void write(BinaryRow partition, int bucket, T data) throws Exception {
        throw new RuntimeException("Can't invoke method from here");
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        if (fullCompaction) {
            // we need to write all the datas before triggering a full compaction, otherwise, the
            // changelog producer of full-compaction will not be able to generate changelog in time.
            flushAllToWriter();
        }
        write.compact(partition, bucket, fullCompaction);
    }

    @Override
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        write.notifyNewFiles(snapshotId, partition, bucket, files);
    }

    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception {
        List<CommitMessage> commitMessages = new ArrayList<>();
        if (sorter != null && sorter.size() != 0) {
            // flush external row sort buffer.
            NextIterator<SinkRecord> iterator = sorter.sortedIterator();
            BinaryRow lastPartition = BinaryRow.EMPTY_ROW;
            SinkRecord sinkRecord;
            int lastBucket = -1;
            while ((sinkRecord = iterator.nextOrNull()) != null) {
                if (!lastPartition.equals(sinkRecord.partition())
                        || lastBucket != sinkRecord.bucket()) {
                    commitMessages.addAll(write.prepareCommit(waitCompaction, commitIdentifier));
                    // close at once to avoid out-of-memory
                    write.closeWriter(lastPartition, lastBucket);
                    sinkRecord.partition().copy(lastPartition);
                    lastBucket = sinkRecord.bucket();
                }
                write.write(
                        sinkRecord.partition(),
                        sinkRecord.bucket(),
                        recordExtractor.extract(sinkRecord));
            }
            sorter.clear();
        }
        commitMessages.addAll(write.prepareCommit(waitCompaction, commitIdentifier));
        return commitMessages;
    }

    @Override
    public void closeWriter(BinaryRow partition, int bucket) throws Exception {
        write.closeWriter(partition, bucket);
    }

    @Override
    public int aliveWriters() {
        return write.aliveWriters();
    }

    @Override
    public void close() throws Exception {
        write.close();
    }

    @Override
    public List<State<T>> checkpoint() {
        try {
            flushAllToWriter();
        } catch (Exception e) {
            throw new RuntimeException("Error happens while flushing to writer.", e);
        }
        return write.checkpoint();
    }

    @Override
    public void restore(List<State<T>> state) {
        write.restore(state);
    }

    private void flushAllToWriter() throws Exception {
        if (sorter != null && sorter.size() != 0) {
            NextIterator<SinkRecord> iterator = sorter.sortedIterator();
            SinkRecord record;
            while ((record = iterator.nextOrNull()) != null) {
                write.write(record.partition(), record.bucket(), recordExtractor.extract(record));
            }
            sorter.clear();
        }
    }

    static class BatchBucketSorter {
        private static final String KEY_PREFIX = "_SORT_KEY_";

        private final BinaryExternalSortBuffer binaryExternalSortBuffer;
        private final KeyAndBucketExtractor<InternalRow> keyAndBucketExtractor;
        private final int bucketIndex;
        private final int rowKindIndex;
        private final int fullSize;

        private final JoinedRow inJoinedRowAll;
        private final JoinedRow inJoinedRowPart;
        private final GenericRow inUnionRow;
        private final OffsetRow outRow;

        private long orderNumber = 0;

        public BatchBucketSorter(
                IOManager ioManager,
                KeyAndBucketExtractor<InternalRow> keyAndBucketExtractor,
                TableSchema schema) {
            this.keyAndBucketExtractor = keyAndBucketExtractor;

            RowType rowType = schema.logicalRowType();
            int[] fieldsIndex = schema.projection(schema.partitionKeys());
            CoreOptions coreOptions = new CoreOptions(schema.options());

            List<DataField> keyFields = new ArrayList<>();
            // construct the value projection to extract origin row from extended key_value row.
            for (int i : fieldsIndex) {
                // we put the KEY_PREFIX ahead of origin field name to avoid conflict
                keyFields.add(
                        rowType.getFields()
                                .get(i)
                                .newName(KEY_PREFIX + rowType.getFields().get(i).name()));
            }
            keyFields.add(new DataField(0, KEY_PREFIX + "_BUCKET_", DataTypes.INT()));
            keyFields.add(new DataField(keyFields.size(), SEQUENCE_NUMBER, new BigIntType(false)));

            List<DataField> dataFields = new ArrayList<>(keyFields);
            dataFields.add(new DataField(dataFields.size(), VALUE_KIND, new TinyIntType(false)));
            dataFields.addAll(rowType.getFields());

            // construct the binary sorter to sort the extended key_value row
            binaryExternalSortBuffer =
                    BinaryExternalSortBuffer.create(
                            ioManager,
                            new RowType(keyFields),
                            new RowType(dataFields),
                            coreOptions.writeBufferSize(),
                            coreOptions.pageSize(),
                            coreOptions.localSortMaxNumFileHandles());

            this.bucketIndex = fieldsIndex.length;
            this.rowKindIndex = keyFields.size();

            this.fullSize = dataFields.size();
            this.inJoinedRowAll = new JoinedRow();
            this.inJoinedRowPart = new JoinedRow();
            this.inUnionRow = new GenericRow(3);
            this.outRow = new OffsetRow(rowType.getFieldCount(), keyFields.size() + 1);
        }

        public int size() {
            return binaryExternalSortBuffer.size();
        }

        public void clear() {
            orderNumber = 0;
            binaryExternalSortBuffer.clear();
        }

        @VisibleForTesting
        public boolean flushMemory() throws IOException {
            return binaryExternalSortBuffer.flushMemory();
        }

        public void write(BinaryRow partition, int bucket, InternalRow payload) throws IOException {
            binaryExternalSortBuffer.write(
                    toSortRow(partition, bucket, orderNumber++, payload.getRowKind(), payload));
        }

        private InternalRow toSortRow(
                BinaryRow partition,
                int bucket,
                long orderNumber,
                RowKind rowKind,
                InternalRow payload) {
            inUnionRow.setField(0, bucket);
            inUnionRow.setField(1, orderNumber);
            inUnionRow.setField(2, rowKind.toByteValue());
            inJoinedRowPart.replace(partition, inUnionRow);
            inJoinedRowAll.replace(inJoinedRowPart, payload);
            return inJoinedRowAll;
        }

        private int bucket(InternalRow sortedRow) {
            return sortedRow.getInt(bucketIndex);
        }

        private RowKind rowKind(InternalRow sortedRow) {
            return RowKind.fromByteValue(sortedRow.getByte(rowKindIndex));
        }

        private InternalRow toOriginRow(InternalRow row) {
            outRow.replace(row);
            outRow.setRowKind(rowKind(row));
            return outRow;
        }

        public NextIterator<SinkRecord> sortedIterator() throws IOException {
            MutableObjectIterator<BinaryRow> iterator = binaryExternalSortBuffer.sortedIterator();
            return new NextIterator<SinkRecord>() {
                private BinaryRow binaryRow = new BinaryRow(fullSize);

                @Override
                public SinkRecord nextOrNull() {
                    try {
                        binaryRow = iterator.next(binaryRow);
                        if (binaryRow != null) {
                            InternalRow originRow = toOriginRow(binaryRow);
                            int bucket = bucket(binaryRow);
                            keyAndBucketExtractor.setRecord(originRow);
                            BinaryRow partition = keyAndBucketExtractor.partition();
                            BinaryRow primaryKey = keyAndBucketExtractor.trimmedPrimaryKey();
                            return new SinkRecord(partition, bucket, primaryKey, originRow);
                        }
                        return null;
                    } catch (IOException e) {
                        throw new RuntimeException("Errors while sorted bucket batch", e);
                    }
                }
            };
        }
    }
}
