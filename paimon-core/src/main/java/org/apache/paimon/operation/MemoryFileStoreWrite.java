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
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.index.IndexMaintainer;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.metrics.WriterBufferMetric;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Base {@link FileStoreWrite} implementation which supports using shared memory and preempting
 * memory from other writers.
 *
 * @param <T> type of record to write.
 */
public abstract class MemoryFileStoreWrite<T> extends AbstractFileStoreWrite<T> {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryFileStoreWrite.class);

    protected final CoreOptions options;
    protected final CacheManager cacheManager;
    private MemoryPoolFactory writeBufferPool;

    private WriterBufferMetric writerBufferMetric;

    public MemoryFileStoreWrite(
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options,
            RowType partitionType,
            @Nullable IndexMaintainer.Factory<T> indexFactory,
            @Nullable DeletionVectorsMaintainer.Factory dvMaintainerFactory,
            String tableName) {
        super(
                snapshotManager,
                scan,
                indexFactory,
                dvMaintainerFactory,
                tableName,
                options,
                options.bucket(),
                partitionType,
                options.writeMaxWritersToSpill(),
                options.legacyPartitionName());
        this.options = options;
        this.cacheManager =
                new CacheManager(
                        options.lookupCacheMaxMemory(), options.lookupCacheHighPrioPoolRatio());
    }

    @Override
    public FileStoreWrite<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        this.writeBufferPool = memoryPoolFactory.addOwners(this::memoryOwners);
        return this;
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

        if (writeBufferPool == null) {
            LOG.debug("Use default heap memory segment pool for write buffer.");
            writeBufferPool =
                    new MemoryPoolFactory(
                                    new HeapMemorySegmentPool(
                                            options.writeBufferSize(), options.pageSize()))
                            .addOwners(this::memoryOwners);
        }
        writeBufferPool.notifyNewOwner((MemoryOwner) writer);

        if (writerBufferMetric != null) {
            writerBufferMetric.increaseNumWriters();
        }
    }

    @Override
    public FileStoreWrite<T> withMetricRegistry(MetricRegistry metricRegistry) {
        super.withMetricRegistry(metricRegistry);
        registerWriterBufferMetric(metricRegistry);
        return this;
    }

    private void registerWriterBufferMetric(MetricRegistry metricRegistry) {
        if (metricRegistry != null) {
            writerBufferMetric =
                    new WriterBufferMetric(() -> writeBufferPool, metricRegistry, tableName);
        }
    }

    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception {
        List<CommitMessage> result = super.prepareCommit(waitCompaction, commitIdentifier);
        if (writerBufferMetric != null) {
            writerBufferMetric.setNumWriters(writers.values().stream().mapToInt(Map::size).sum());
        }
        return result;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (this.writerBufferMetric != null) {
            this.writerBufferMetric.close();
        }
    }
}
