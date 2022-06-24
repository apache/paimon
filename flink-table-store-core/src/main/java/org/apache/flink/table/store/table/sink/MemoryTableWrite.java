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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.operation.FileStoreWrite;
import org.apache.flink.table.store.file.utils.HeapMemorySegmentPool;
import org.apache.flink.table.store.file.utils.MemoryPoolFactory;
import org.apache.flink.table.store.file.writer.MemoryRecordWriter;
import org.apache.flink.table.store.file.writer.RecordWriter;

import java.util.Map;

/**
 * A {@link TableWrite} which supports using shared memory and preempting memory from other writers.
 */
public abstract class MemoryTableWrite<T> extends AbstractTableWrite<T>
        implements MemoryPoolFactory.PreemptRunner<MemoryRecordWriter<T>> {

    private final MemoryPoolFactory<MemoryRecordWriter<T>> memoryPoolFactory;

    protected MemoryTableWrite(
            FileStoreWrite<T> write,
            SinkRecordConverter recordConverter,
            FileStoreOptions options) {
        super(write, recordConverter);

        MergeTreeOptions mergeTreeOptions = options.mergeTreeOptions();
        HeapMemorySegmentPool memoryPool =
                new HeapMemorySegmentPool(
                        mergeTreeOptions.writeBufferSize, mergeTreeOptions.pageSize);
        this.memoryPoolFactory = new MemoryPoolFactory<>(memoryPool, this);
    }

    @Override
    public void preemptMemory(MemoryRecordWriter<T> owner) {
        long maxMemory = -1;
        MemoryRecordWriter<T> max = null;
        for (Map<Integer, RecordWriter<T>> bucket : writers.values()) {
            for (RecordWriter<T> recordWriter : bucket.values()) {
                MemoryRecordWriter<T> writer = (MemoryRecordWriter<T>) recordWriter;
                // Don't preempt yourself! Write and flush at the same time, which may lead to
                // inconsistent state
                if (writer != owner && writer.memoryOccupancy() > maxMemory) {
                    maxMemory = writer.memoryOccupancy();
                    max = writer;
                }
            }
        }

        if (max != null) {
            try {
                max.flushMemory();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void notifyNewWriter(RecordWriter<T> writer) {
        if (!(writer instanceof MemoryRecordWriter)) {
            throw new RuntimeException(
                    "Should create a MemoryRecordWriter for MemoryTableWrite,"
                            + " but this is: "
                            + writer.getClass());
        }
        MemoryRecordWriter<T> memoryRecordWriter = (MemoryRecordWriter<T>) writer;
        MemorySegmentPool memoryPool = memoryPoolFactory.create(memoryRecordWriter);
        memoryRecordWriter.setMemoryPool(memoryPool);
    }
}
