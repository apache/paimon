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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.mergetree.SortBufferWriteBuffer;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.table.sink.RowKindGenerator;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

/**
 * {@link AbstractStreamOperator} which buffer input record and apply merge function when the buffer
 * is full. Mainly to resolve data skew on primary keys.
 */
public class LocalMergeOperator extends AbstractStreamOperator<InternalRow>
        implements OneInputStreamOperator<InternalRow, InternalRow>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final TableSchema schema;
    private final boolean ignoreDelete;

    private transient Projection keyProjection;
    private transient RecordComparator keyComparator;

    private transient long recordCount;
    private transient RowKindGenerator rowKindGenerator;
    private transient MergeFunction<KeyValue> mergeFunction;

    private transient SortBufferWriteBuffer buffer;
    private transient long currentWatermark;

    private transient boolean endOfInput;

    public LocalMergeOperator(TableSchema schema) {
        Preconditions.checkArgument(
                schema.primaryKeys().size() > 0,
                "LocalMergeOperator currently only support tables with primary keys");
        this.schema = schema;
        this.ignoreDelete = CoreOptions.fromMap(schema.options()).ignoreDelete();
        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void open() throws Exception {
        super.open();

        RowType keyType = PrimaryKeyTableUtils.addKeyNamePrefix(schema.logicalPrimaryKeysType());
        RowType valueType = schema.logicalRowType();
        CoreOptions options = new CoreOptions(schema.options());

        keyProjection =
                CodeGenUtils.newProjection(valueType, schema.projection(schema.primaryKeys()));
        keyComparator = new KeyComparatorSupplier(keyType).get();

        recordCount = 0;
        rowKindGenerator = RowKindGenerator.create(schema, options);
        mergeFunction =
                PrimaryKeyTableUtils.createMergeFunctionFactory(
                                schema,
                                new KeyValueFieldsExtractor() {
                                    private static final long serialVersionUID = 1L;

                                    // At local merge operator, the key extractor should include
                                    // partition fields.
                                    @Override
                                    public List<DataField> keyFields(TableSchema schema) {
                                        return schema.primaryKeysFields();
                                    }

                                    @Override
                                    public List<DataField> valueFields(TableSchema schema) {
                                        return schema.fields();
                                    }
                                })
                        .create();

        buffer =
                new SortBufferWriteBuffer(
                        keyType,
                        valueType,
                        UserDefinedSeqComparator.create(valueType, options),
                        new HeapMemorySegmentPool(
                                options.localMergeBufferSize(), options.pageSize()),
                        false,
                        MemorySize.MAX_VALUE,
                        options.localSortMaxNumFileHandles(),
                        options.spillCompression(),
                        null);
        currentWatermark = Long.MIN_VALUE;

        endOfInput = false;
    }

    @Override
    public void processElement(StreamRecord<InternalRow> record) throws Exception {
        recordCount++;
        InternalRow row = record.getValue();

        RowKind rowKind = RowKindGenerator.getRowKind(rowKindGenerator, row);
        if (ignoreDelete && rowKind.isRetract()) {
            return;
        }

        // row kind must be INSERT when it is divided into key and value
        row.setRowKind(RowKind.INSERT);

        InternalRow key = keyProjection.apply(row);
        if (!buffer.put(recordCount, rowKind, key, row)) {
            flushBuffer();
            if (!buffer.put(recordCount, rowKind, key, row)) {
                // change row kind back
                row.setRowKind(rowKind);
                output.collect(record);
            }
        }
    }

    @Override
    public void processWatermark(Watermark mark) {
        // don't emit watermark immediately, emit them after flushing buffer
        currentWatermark = mark.getTimestamp();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        if (!endOfInput) {
            flushBuffer();
        }
        // no records are expected to emit after endOfInput
    }

    @Override
    public void endInput() throws Exception {
        endOfInput = true;
        flushBuffer();
    }

    @Override
    public void close() throws Exception {
        if (buffer != null) {
            buffer.clear();
        }

        super.close();
    }

    private void flushBuffer() throws Exception {
        if (buffer.size() == 0) {
            return;
        }

        buffer.forEach(
                keyComparator,
                mergeFunction,
                null,
                kv -> {
                    InternalRow row = kv.value();
                    row.setRowKind(kv.valueKind());
                    output.collect(new StreamRecord<>(row));
                });
        buffer.clear();

        if (currentWatermark != Long.MIN_VALUE) {
            super.processWatermark(new Watermark(currentWatermark));
            // each watermark should only be emitted once
            currentWatermark = Long.MIN_VALUE;
        }
    }
}
