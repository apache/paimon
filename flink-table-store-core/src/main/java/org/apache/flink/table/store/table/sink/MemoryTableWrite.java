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

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.memory.HeapMemorySegmentPool;
import org.apache.flink.table.store.file.memory.MemoryOwner;
import org.apache.flink.table.store.file.memory.MemoryPoolFactory;
import org.apache.flink.table.store.file.operation.FileStoreWrite;
import org.apache.flink.table.store.file.writer.RecordWriter;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.Map;

/**
 * A {@link TableWrite} which supports using shared memory and preempting memory from other writers.
 */
public abstract class MemoryTableWrite<T> extends AbstractTableWrite<T> {

    private final MemoryPoolFactory memoryPoolFactory;

    protected MemoryTableWrite(
            FileStoreWrite<T> write, SinkRecordConverter recordConverter, CoreOptions options) {
        super(write, recordConverter);

        HeapMemorySegmentPool memoryPool =
                new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize());
        this.memoryPoolFactory = new MemoryPoolFactory(memoryPool, this::memoryOwners);
    }

    private Iterator<MemoryOwner> memoryOwners() {
        Iterator<Map<Integer, RecordWriter<T>>> iterator = writers.values().iterator();
        return Iterators.concat(
                new Iterator<Iterator<MemoryOwner>>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Iterator<MemoryOwner> next() {
                        return Iterators.transform(
                                iterator.next().values().iterator(),
                                writer -> (MemoryOwner) writer);
                    }
                });
    }

    @Override
    protected void notifyNewWriter(RecordWriter<T> writer) {
        if (!(writer instanceof MemoryOwner)) {
            throw new RuntimeException(
                    "Should create a MemoryOwner for MemoryTableWrite,"
                            + " but this is: "
                            + writer.getClass());
        }
        memoryPoolFactory.notifyNewOwner((MemoryOwner) writer);
    }
}
