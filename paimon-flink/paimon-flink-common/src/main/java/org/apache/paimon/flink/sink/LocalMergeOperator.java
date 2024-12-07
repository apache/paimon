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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.mergetree.SortBufferWriteBuffer;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.localmerge.HashMapLocalMerger;
import org.apache.paimon.mergetree.localmerge.LocalMerger;
import org.apache.paimon.mergetree.localmerge.SortBufferLocalMerger;
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
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

import static org.apache.paimon.table.PrimaryKeyTableUtils.addKeyNamePrefix;

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

    private transient RowKindGenerator rowKindGenerator;

    private transient LocalMerger merger;
    private transient long currentWatermark;

    private transient boolean endOfInput;

    private LocalMergeOperator(
            StreamOperatorParameters<InternalRow> parameters, TableSchema schema) {
        Preconditions.checkArgument(
                schema.primaryKeys().size() > 0,
                "LocalMergeOperator currently only support tables with primary keys");
        this.schema = schema;
        this.ignoreDelete = CoreOptions.fromMap(schema.options()).ignoreDelete();
        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    }

    @Override
    public void open() throws Exception {
        super.open();

        List<String> primaryKeys = schema.primaryKeys();
        RowType valueType = schema.logicalRowType();
        CoreOptions options = new CoreOptions(schema.options());

        keyProjection = CodeGenUtils.newProjection(valueType, schema.projection(primaryKeys));

        rowKindGenerator = RowKindGenerator.create(schema, options);
        MergeFunction<KeyValue> mergeFunction =
                PrimaryKeyTableUtils.createMergeFunctionFactory(
                                schema,
                                new KeyValueFieldsExtractor() {
                                    private static final long serialVersionUID = 1L;

                                    // At local merge operator, the key extractor should include
                                    // partition fields.
                                    @Override
                                    public List<DataField> keyFields(TableSchema schema) {
                                        return addKeyNamePrefix(schema.primaryKeysFields());
                                    }

                                    @Override
                                    public List<DataField> valueFields(TableSchema schema) {
                                        return schema.fields();
                                    }
                                })
                        .create();

        boolean canHashMerger = true;
        for (DataField field : valueType.getFields()) {
            if (primaryKeys.contains(field.name())) {
                continue;
            }

            if (!BinaryRow.isInFixedLengthPart(field.type())) {
                canHashMerger = false;
                break;
            }
        }

        HeapMemorySegmentPool pool =
                new HeapMemorySegmentPool(options.localMergeBufferSize(), options.pageSize());
        UserDefinedSeqComparator udsComparator =
                UserDefinedSeqComparator.create(valueType, options);
        if (canHashMerger) {
            merger =
                    new HashMapLocalMerger(
                            valueType, primaryKeys, pool, mergeFunction, udsComparator);
        } else {
            RowType keyType =
                    PrimaryKeyTableUtils.addKeyNamePrefix(schema.logicalPrimaryKeysType());
            SortBufferWriteBuffer sortBuffer =
                    new SortBufferWriteBuffer(
                            keyType,
                            valueType,
                            udsComparator,
                            pool,
                            false,
                            MemorySize.MAX_VALUE,
                            options.localSortMaxNumFileHandles(),
                            options.spillCompressOptions(),
                            null);
            merger =
                    new SortBufferLocalMerger(
                            sortBuffer, new KeyComparatorSupplier(keyType).get(), mergeFunction);
        }

        currentWatermark = Long.MIN_VALUE;
        endOfInput = false;
    }

    @Override
    public void processElement(StreamRecord<InternalRow> record) throws Exception {
        InternalRow row = record.getValue();

        RowKind rowKind = RowKindGenerator.getRowKind(rowKindGenerator, row);
        if (ignoreDelete && rowKind.isRetract()) {
            return;
        }

        // row kind must be INSERT when it is divided into key and value
        row.setRowKind(RowKind.INSERT);

        BinaryRow key = keyProjection.apply(row);
        if (!merger.put(rowKind, key, row)) {
            flushBuffer();
            if (!merger.put(rowKind, key, row)) {
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
        if (merger != null) {
            merger.clear();
        }

        super.close();
    }

    private void flushBuffer() throws Exception {
        if (merger.size() == 0) {
            return;
        }

        merger.forEach(row -> output.collect(new StreamRecord<>(row)));
        merger.clear();

        if (currentWatermark != Long.MIN_VALUE) {
            super.processWatermark(new Watermark(currentWatermark));
            // each watermark should only be emitted once
            currentWatermark = Long.MIN_VALUE;
        }
    }

    @VisibleForTesting
    LocalMerger merger() {
        return merger;
    }

    @VisibleForTesting
    void setOutput(Output<StreamRecord<InternalRow>> output) {
        this.output = output;
    }

    /** {@link StreamOperatorFactory} of {@link LocalMergeOperator}. */
    public static class Factory extends AbstractStreamOperatorFactory<InternalRow>
            implements OneInputStreamOperatorFactory<InternalRow, InternalRow> {
        private final TableSchema schema;

        public Factory(TableSchema schema) {
            this.chainingStrategy = ChainingStrategy.ALWAYS;
            this.schema = schema;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<InternalRow>> T createStreamOperator(
                StreamOperatorParameters<InternalRow> parameters) {
            return (T) new LocalMergeOperator(parameters, schema);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return LocalMergeOperator.class;
        }
    }
}
