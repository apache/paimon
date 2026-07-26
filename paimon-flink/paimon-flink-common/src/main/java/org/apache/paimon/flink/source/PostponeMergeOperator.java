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

package org.apache.paimon.flink.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.FlinkRowDataWithBlob;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.NestedProjectedRowData;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.PostponeMergeRead;
import org.apache.paimon.table.source.PostponeMergeReadBuilder;
import org.apache.paimon.table.source.SplitSerializer;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.paimon.flink.source.PostponeMergeOnRead.BUCKET;
import static org.apache.paimon.flink.source.PostponeMergeOnRead.CARRIER_TYPE;
import static org.apache.paimon.flink.source.PostponeMergeOnRead.INPUT_KIND;
import static org.apache.paimon.flink.source.PostponeMergeOnRead.KEY;
import static org.apache.paimon.flink.source.PostponeMergeOnRead.PARTITION;
import static org.apache.paimon.flink.source.PostponeMergeOnRead.POSTPONE_RECORD_KIND;
import static org.apache.paimon.flink.source.PostponeMergeOnRead.REAL_SPLIT;
import static org.apache.paimon.flink.source.PostponeMergeOnRead.REAL_SPLIT_KIND;
import static org.apache.paimon.flink.source.PostponeMergeOnRead.ROW_KIND;
import static org.apache.paimon.flink.source.PostponeMergeOnRead.VALUE;
import static org.apache.paimon.flink.source.PostponeMergeOnRead.WRITER_LOCAL_ORDER;

/** Spillably sorts routed carriers and merges each target bucket through Paimon Core. */
final class PostponeMergeOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<InternalRow, RowData>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final PostponeMergeReadBuilder readBuilder;
    private final RowType resultReadType;
    private final Map<String, String> tableOptions;
    @Nullable private final NestedProjectedRowData outerProject;
    @Nullable private final Long limit;
    private final boolean blobAsDescriptor;

    private transient IOManager ioManager;
    private transient BinaryExternalSortBuffer buffer;
    private transient PostponeMergeRead read;
    private transient FlinkRowData flinkRow;
    @Nullable private transient NestedProjectedRowData projectedRow;
    @Nullable private transient RecordLimiter recordLimiter;

    PostponeMergeOperator(
            PostponeMergeReadBuilder readBuilder,
            RowType resultReadType,
            Map<String, String> tableOptions,
            @Nullable NestedProjectedRowData outerProject,
            @Nullable Long limit,
            boolean blobAsDescriptor) {
        this.readBuilder = readBuilder;
        this.resultReadType = resultReadType;
        this.tableOptions = tableOptions;
        this.outerProject = outerProject;
        this.limit = limit;
        this.blobAsDescriptor = blobAsDescriptor;
    }

    @Override
    public void open() throws Exception {
        super.open();
        ioManager =
                IOManager.create(
                        getContainingTask()
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectoriesPaths());
        CoreOptions options = CoreOptions.fromMap(tableOptions);
        buffer =
                BinaryExternalSortBuffer.create(
                        ioManager,
                        CARRIER_TYPE,
                        new int[] {PARTITION, BUCKET, INPUT_KIND, WRITER_LOCAL_ORDER},
                        options.sortSpillBufferSize(),
                        options.pageSize(),
                        options.localSortMaxNumFileHandles(),
                        options.spillCompressOptions(),
                        options.writeBufferSpillDiskSize());
        read = readBuilder.newRead().withIOManager(ioManager);

        Set<Integer> blobFields = FlinkRowWrapper.blobFieldIndexes(resultReadType);
        flinkRow =
                blobFields.isEmpty()
                        ? new FlinkRowData(null)
                        : new FlinkRowDataWithBlob(null, blobFields, blobAsDescriptor);
        projectedRow = NestedProjectedRowData.copy(outerProject);
        recordLimiter = RecordLimiter.create(limit);
    }

    @Override
    public void processElement(StreamRecord<InternalRow> element) throws Exception {
        buffer.write(element.getValue());
    }

    @Override
    public void endInput() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }

        SortedCarriers carriers = new SortedCarriers(buffer.sortedIterator());
        while (carriers.hasNext() && !reachLimit()) {
            mergeBucket(carriers);
        }
    }

    private void mergeBucket(SortedCarriers carriers) throws Exception {
        InternalRow first = carriers.peek();
        BucketKey bucketKey = new BucketKey(first.getBinary(PARTITION), first.getInt(BUCKET));

        DataSplit realSplit = null;
        if (sameBucket(first, bucketKey) && first.getByte(INPUT_KIND) == REAL_SPLIT_KIND) {
            realSplit =
                    (DataSplit) SplitSerializer.deserialize(carriers.next().getBinary(REAL_SPLIT));
        }

        Iterator<KeyValue> postponeRecords =
                new Iterator<KeyValue>() {
                    @Override
                    public boolean hasNext() {
                        return carriers.hasNext()
                                && sameBucket(carriers.peek(), bucketKey)
                                && carriers.peek().getByte(INPUT_KIND) == POSTPONE_RECORD_KIND;
                    }

                    @Override
                    public KeyValue next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        InternalRow carrier = carriers.next();
                        return new KeyValue()
                                .replace(
                                        SerializationUtils.deserializeBinaryRow(
                                                carrier.getBinary(KEY)),
                                        RowKind.fromByteValue(carrier.getByte(ROW_KIND)),
                                        SerializationUtils.deserializeBinaryRow(
                                                carrier.getBinary(VALUE)));
                    }
                };

        try (RecordReaderIterator<InternalRow> rows =
                new RecordReaderIterator<>(
                        read.createBucketMergeReader(
                                realSplit, new IteratorRecordReader<>(postponeRecords)))) {
            if (carriers.hasNext() && sameBucket(carriers.peek(), bucketKey)) {
                throw new IllegalStateException(
                        "Unexpected postpone merge carrier kind "
                                + carriers.peek().getByte(INPUT_KIND)
                                + " for one bucket.");
            }
            while (rows.hasNext() && !reachLimit()) {
                flinkRow.replace(rows.next());
                RowData result =
                        projectedRow == null ? flinkRow : projectedRow.replaceRow(flinkRow);
                output.collect(new StreamRecord<>(result));
                if (recordLimiter != null) {
                    recordLimiter.increment();
                }
            }
        }
    }

    private boolean reachLimit() {
        return recordLimiter != null && recordLimiter.reachLimit();
    }

    private static boolean sameBucket(InternalRow carrier, BucketKey bucketKey) {
        return carrier.getInt(BUCKET) == bucketKey.bucket
                && Arrays.equals(carrier.getBinary(PARTITION), bucketKey.partition);
    }

    @Override
    public void close() throws Exception {
        try {
            if (buffer != null) {
                buffer.clear();
            }
        } finally {
            try {
                if (ioManager != null) {
                    ioManager.close();
                }
            } finally {
                super.close();
            }
        }
    }

    private static class BucketKey {

        private final byte[] partition;
        private final int bucket;

        private BucketKey(byte[] partition, int bucket) {
            this.partition = partition;
            this.bucket = bucket;
        }
    }

    private static class SortedCarriers {

        private final MutableObjectIterator<BinaryRow> iterator;
        private final BinaryRow reuse = new BinaryRow(CARRIER_TYPE.getFieldCount());
        @Nullable private BinaryRow next;

        private SortedCarriers(MutableObjectIterator<BinaryRow> iterator) {
            this.iterator = iterator;
        }

        private boolean hasNext() {
            if (next == null) {
                try {
                    next = iterator.next(reuse);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to read sorted postpone carriers.", e);
                }
            }
            return next != null;
        }

        private BinaryRow peek() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return next;
        }

        private BinaryRow next() {
            BinaryRow result = peek();
            next = null;
            return result;
        }
    }
}
