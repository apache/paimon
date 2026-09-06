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

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.PostponeUtils.PostponeBucketRouter;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.PostponeMergeRead;
import org.apache.paimon.table.source.PostponeMergeReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitSerializer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.apache.paimon.flink.source.PostponeMergeOnRead.POSTPONE_RECORD_KIND;
import static org.apache.paimon.flink.source.PostponeMergeOnRead.REAL_SPLIT_KIND;

/** Reads postpone splits and emits serializable records carrying their target buckets. */
final class PostponeMergeInputOperator extends AbstractStreamOperator<InternalRow>
        implements OneInputStreamOperator<Split, InternalRow> {

    private static final long serialVersionUID = 1L;

    private final PostponeMergeReadBuilder readBuilder;
    private final RowType keyType;
    private final RowType mergeReadType;
    private final PostponeBucketRouter bucketRouter;

    private transient IOManager ioManager;
    private transient PostponeMergeRead read;
    private transient InternalRowSerializer keySerializer;
    private transient InternalRowSerializer valueSerializer;

    PostponeMergeInputOperator(
            PostponeMergeReadBuilder readBuilder,
            RowType keyType,
            RowType mergeReadType,
            PostponeBucketRouter bucketRouter) {
        this.readBuilder = readBuilder;
        this.keyType = keyType;
        this.mergeReadType = mergeReadType;
        this.bucketRouter = bucketRouter;
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
        read = readBuilder.newRead().withIOManager(ioManager);
        keySerializer = new InternalRowSerializer(keyType);
        valueSerializer = new InternalRowSerializer(mergeReadType);
    }

    @Override
    public void processElement(StreamRecord<Split> element) throws Exception {
        DataSplit split = (DataSplit) element.getValue();
        if (split.bucket() == BucketMode.POSTPONE_BUCKET) {
            emitPostponeRecords(split);
        } else {
            emitRealSplit(split);
        }
    }

    private void emitRealSplit(DataSplit split) throws Exception {
        GenericRow carrier =
                GenericRow.of(
                        SerializationUtils.serializeBinaryRow(split.partition()),
                        split.bucket(),
                        REAL_SPLIT_KIND,
                        SplitSerializer.serialize(split),
                        null,
                        0L,
                        (byte) 0,
                        null);
        output.collect(new StreamRecord<>(carrier));
    }

    private void emitPostponeRecords(DataSplit split) throws Exception {
        byte[] partition = SerializationUtils.serializeBinaryRow(split.partition());
        long writerLocalOrder = 0L;
        try (RecordReaderIterator<KeyValue> records =
                new RecordReaderIterator<>(read.createPostponeReader(split))) {
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                BinaryRow key = keySerializer.toBinaryRow(keyValue.key());
                BinaryRow value = valueSerializer.toBinaryRow(keyValue.value());
                GenericRow carrier =
                        GenericRow.of(
                                partition,
                                bucketRouter.bucket(split.partition(), key),
                                POSTPONE_RECORD_KIND,
                                null,
                                SerializationUtils.serializeBinaryRow(key),
                                writerLocalOrder,
                                keyValue.valueKind().toByteValue(),
                                SerializationUtils.serializeBinaryRow(value));
                output.collect(new StreamRecord<>(carrier));
                writerLocalOrder = Math.addExact(writerLocalOrder, 1L);
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (ioManager != null) {
                ioManager.close();
            }
        } finally {
            super.close();
        }
    }
}
