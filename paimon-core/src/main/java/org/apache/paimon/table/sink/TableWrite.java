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

package org.apache.paimon.table.sink;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.mergetree.compact.CompactRewriterFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

/**
 * Write of {@link Table} to provide {@link InternalRow} writing.
 *
 * @since 0.4.0
 */
@Public
public interface TableWrite extends AutoCloseable {

    /** With {@link IOManager}, this is needed if 'write-buffer-spillable' is set to true. */
    TableWrite withIOManager(IOManager ioManager);

    /** Specified the write rowType. */
    TableWrite withWriteType(RowType writeType);

    /** With a shared {@link MemoryPoolFactory} for the current table write. */
    TableWrite withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory);

    /**
     * With a blob consumer for the current table write, all newly written Blobs will be called back
     * through this interface. Note that when this method is invoked, temporary files will not be
     * deleted in case of exceptions. Please delete them yourself.
     */
    TableWrite withBlobConsumer(BlobConsumer blobConsumer);

    /**
     * Installs a rewriter factory for primary-key merge-tree compaction. Configure this before
     * writing, restoring, or compacting any bucket, and configure it again on each recovered
     * writer. Paimon retains compaction scheduling and commit coordination. Append and clustering
     * writers do not support this hook; write-only writers never invoke it.
     */
    default TableWrite withCompactRewriterFactory(CompactRewriterFactory factory) {
        throw new UnsupportedOperationException("Custom compaction rewriters are not supported.");
    }

    /** Calculate which partition {@code row} belongs to. */
    BinaryRow getPartition(InternalRow row);

    /** Calculate which bucket {@code row} belongs to. */
    int getBucket(InternalRow row);

    /** Write a row to the writer. */
    void write(InternalRow row) throws Exception;

    /** Write a row with bucket. */
    void write(InternalRow row, int bucket) throws Exception;

    /** Write a bundle records directly, not per row. */
    void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle) throws Exception;

    /**
     * Compact a bucket of a partition. By default, it will determine whether to perform the
     * compaction according to the 'num-sorted-run.compaction-trigger' option. If fullCompaction is
     * true, it will force a full compaction, which is expensive.
     *
     * <p>NOTE: In Java API, full compaction is not automatically executed. If you set
     * 'changelog-producer' to 'full-compaction', please execute this method regularly to produce
     * changelog.
     */
    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    /** Set {@link MetricRegistry} to table write. */
    TableWrite withMetricRegistry(MetricRegistry registry);
}
