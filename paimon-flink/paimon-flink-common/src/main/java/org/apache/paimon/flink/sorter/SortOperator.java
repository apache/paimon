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

package org.apache.paimon.flink.sorter;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MutableObjectIterator;

import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.operators.TableStreamOperator;

import java.util.stream.IntStream;

/** SortOperator to sort the `InternalRow`s by the `KeyType`. */
public class SortOperator extends TableStreamOperator<InternalRow>
        implements OneInputStreamOperator<InternalRow, InternalRow>, BoundedOneInput {

    private final RowType keyType;
    private final RowType rowType;
    private final long maxMemory;
    private final int pageSize;
    private final int arity;
    private final int spillSortMaxNumFiles;
    private final CompressOptions spillCompression;
    private final int sinkParallelism;
    private final MemorySize maxDiskSize;

    private transient BinaryExternalSortBuffer buffer;
    private transient IOManager ioManager;

    public SortOperator(
            RowType keyType,
            RowType rowType,
            long maxMemory,
            int pageSize,
            int spillSortMaxNumFiles,
            CompressOptions spillCompression,
            int sinkParallelism,
            MemorySize maxDiskSize) {
        this.keyType = keyType;
        this.rowType = rowType;
        this.maxMemory = maxMemory;
        this.pageSize = pageSize;
        this.arity = rowType.getFieldCount();
        this.spillSortMaxNumFiles = spillSortMaxNumFiles;
        this.spillCompression = spillCompression;
        this.sinkParallelism = sinkParallelism;
        this.maxDiskSize = maxDiskSize;
    }

    @Override
    public void open() throws Exception {
        super.open();
        initBuffer();
        if (sinkParallelism != getRuntimeContext().getNumberOfParallelSubtasks()) {
            throw new IllegalArgumentException(
                    "Please ensure that the runtime parallelism of the sink matches the initial configuration "
                            + "to avoid potential issues with skewed range partitioning.");
        }
    }

    @VisibleForTesting
    void initBuffer() {
        this.ioManager =
                IOManager.create(
                        getContainingTask()
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectoriesPaths());
        buffer =
                BinaryExternalSortBuffer.create(
                        ioManager,
                        rowType,
                        IntStream.range(0, keyType.getFieldCount()).toArray(),
                        maxMemory,
                        pageSize,
                        spillSortMaxNumFiles,
                        spillCompression,
                        maxDiskSize);
    }

    @Override
    public void endInput() throws Exception {
        if (buffer.size() > 0) {
            MutableObjectIterator<BinaryRow> iterator = buffer.sortedIterator();
            BinaryRow binaryRow = new BinaryRow(arity);
            while ((binaryRow = iterator.next(binaryRow)) != null) {
                output.collect(new StreamRecord<>(binaryRow));
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (buffer != null) {
            buffer.clear();
        }
        if (ioManager != null) {
            ioManager.close();
        }
    }

    @Override
    public void processElement(StreamRecord<InternalRow> element) throws Exception {
        buffer.write(element.getValue());
    }

    @VisibleForTesting
    BinaryExternalSortBuffer getBuffer() {
        return buffer;
    }
}
