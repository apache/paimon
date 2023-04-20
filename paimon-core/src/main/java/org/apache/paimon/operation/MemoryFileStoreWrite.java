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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.Map;

import static org.apache.paimon.CoreOptions.LOOKUP_CACHE_MAX_MEMORY_SIZE;

/**
 * Base {@link FileStoreWrite} implementation which supports using shared memory and preempting
 * memory from other writers.
 *
 * @param <T> type of record to write.
 */
public abstract class MemoryFileStoreWrite<T> extends AbstractFileStoreWrite<T> {

    private final MemoryPoolFactory writeBufferPool;
    protected final CacheManager cacheManager;

    public MemoryFileStoreWrite(
            String commitUser,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options) {
        super(commitUser, snapshotManager, scan);
        HeapMemorySegmentPool memoryPool =
                new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize());
        this.writeBufferPool = new MemoryPoolFactory(memoryPool, this::memoryOwners);
        this.cacheManager =
                new CacheManager(
                        options.pageSize(),
                        options.toConfiguration().get(LOOKUP_CACHE_MAX_MEMORY_SIZE));
    }

    private Iterator<MemoryOwner> memoryOwners() {
        Iterator<Map<Integer, WriterContainer<T>>> iterator = writers.values().iterator();
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
                                writerContainer ->
                                        writerContainer == null
                                                ? null
                                                : (MemoryOwner) (writerContainer.writer));
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
        writeBufferPool.notifyNewOwner((MemoryOwner) writer);
    }
}
