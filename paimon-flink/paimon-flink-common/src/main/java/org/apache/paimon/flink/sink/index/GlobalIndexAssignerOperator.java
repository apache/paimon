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

package org.apache.paimon.flink.sink.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.RowBuffer;
import org.apache.paimon.flink.utils.ProjectToRowDataFunction;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/** A {@link OneInputStreamOperator} for {@link GlobalIndexAssigner}. */
public class GlobalIndexAssignerOperator<T> extends AbstractStreamOperator<Tuple2<T, Integer>>
        implements OneInputStreamOperator<Tuple2<KeyPartOrRow, T>, Tuple2<T, Integer>>,
                BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final Table table;
    private final GlobalIndexAssigner<T> assigner;
    private final SerializableFunction<T, InternalRow> toRow;
    private final SerializableFunction<InternalRow, T> fromRow;

    private transient RowBuffer bootstrapBuffer;

    public GlobalIndexAssignerOperator(
            Table table,
            GlobalIndexAssigner<T> assigner,
            SerializableFunction<T, InternalRow> toRow,
            SerializableFunction<InternalRow, T> fromRow) {
        this.table = table;
        this.assigner = assigner;
        this.toRow = toRow;
        this.fromRow = fromRow;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        org.apache.flink.runtime.io.disk.iomanager.IOManager ioManager =
                getContainingTask().getEnvironment().getIOManager();
        File[] tmpDirs = ioManager.getSpillingDirectories();
        File tmpDir = tmpDirs[ThreadLocalRandom.current().nextInt(tmpDirs.length)];
        assigner.open(
                tmpDir,
                getRuntimeContext().getNumberOfParallelSubtasks(),
                getRuntimeContext().getIndexOfThisSubtask(),
                this::collect);
        Options options = Options.fromMap(table.options());
        long bufferSize = options.get(CoreOptions.WRITE_BUFFER_SIZE).getBytes();
        long pageSize = options.get(CoreOptions.PAGE_SIZE).getBytes();
        bootstrapBuffer =
                RowBuffer.getBuffer(
                        IOManager.create(ioManager.getSpillingDirectoriesPaths()),
                        new HeapMemorySegmentPool(bufferSize, (int) pageSize),
                        new InternalRowSerializer(table.rowType()),
                        true);
    }

    @Override
    public void processElement(StreamRecord<Tuple2<KeyPartOrRow, T>> streamRecord)
            throws Exception {
        Tuple2<KeyPartOrRow, T> tuple2 = streamRecord.getValue();
        T value = tuple2.f1;
        switch (tuple2.f0) {
            case KEY_PART:
                assigner.bootstrap(value);
                break;
            case ROW:
                if (bootstrapBuffer != null) {
                    // ignore return value, we must enable spillable for bootstrapBuffer, so return
                    // is always true
                    bootstrapBuffer.put(toRow.apply(value));
                } else {
                    assigner.process(value);
                }
                break;
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        endBootstrap();
    }

    @Override
    public void endInput() throws Exception {
        endBootstrap();
    }

    private void endBootstrap() throws Exception {
        if (bootstrapBuffer != null) {
            bootstrapBuffer.complete();
            try (RowBuffer.RowBufferIterator iterator = bootstrapBuffer.newIterator()) {
                while (iterator.advanceNext()) {
                    assigner.process(fromRow.apply(iterator.getRow()));
                }
            }
            bootstrapBuffer.reset();
            bootstrapBuffer = null;
        }
    }

    private void collect(T value, int bucket) {
        output.collect(new StreamRecord<>(new Tuple2<>(value, bucket)));
    }

    @Override
    public void close() throws IOException {
        this.assigner.close();
    }

    public static GlobalIndexAssignerOperator<InternalRow> forRowData(Table table) {
        return new GlobalIndexAssignerOperator<>(
                table, createRowDataAssigner(table), r -> r, r -> r);
    }

    public static GlobalIndexAssigner<InternalRow> createRowDataAssigner(Table t) {
        RowType bootstrapType = IndexBootstrap.bootstrapType(((AbstractFileStoreTable) t).schema());
        int bucketIndex = bootstrapType.getFieldCount() - 1;
        return new GlobalIndexAssigner<>(
                t,
                RowPartitionKeyExtractor::new,
                KeyPartPartitionKeyExtractor::new,
                row -> row.getInt(bucketIndex),
                new ProjectToRowDataFunction(t.rowType(), t.partitionKeys()),
                (rowData, rowKind) -> {
                    rowData.setRowKind(rowKind);
                    return rowData;
                });
    }
}
