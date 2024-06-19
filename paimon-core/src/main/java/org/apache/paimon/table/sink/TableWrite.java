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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.Table;

/**
 * Write of {@link Table} to provide {@link InternalRow} writing.
 *
 * @since 0.4.0
 */
@Public
public interface TableWrite extends AutoCloseable {

    default boolean isTableWriteApi() {
        return false;
    }

    default TableWriteApi<?> asTableWriteApi(){
        throw new RuntimeException("not impl for asTableWriteApi");
    }

    default boolean isBatchTableWrite() {
        return false;
    }

    default BatchTableWrite asBatchTableWrite(){
        throw new RuntimeException("not impl for asBatchTableWrite");
    }

    default boolean isStreamTableWrite() {
        return false;
    }

    default StreamTableWrite asStreamTableWrite(){
        throw new RuntimeException("not impl for asStreamTableWrite");
    }

    default boolean isInnerTableWrite() {
        return false;
    }

    default InnerTableWrite asInnerTableWrite(){
        throw new RuntimeException("not impl for asInnerTableWrite");
    }

    /** With {@link IOManager}, this is needed if 'write-buffer-spillable' is set to true. */
    TableWrite withIOManager(IOManager ioManager);

    /** With {@link MemorySegmentPool} for the current table write. */
    TableWrite withMemoryPool(MemorySegmentPool memoryPool);

    /** Calculate which partition {@code row} belongs to. */
    BinaryRow getPartition(InternalRow row);

    /** Calculate which bucket {@code row} belongs to. */
    int getBucket(InternalRow row);

    /** Write a row to the writer. */
    void write(InternalRow row) throws Exception;

    /** Write a row with bucket. */
    void write(InternalRow row, int bucket) throws Exception;

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

    /** With metrics to measure compaction. */
    TableWrite withMetricRegistry(MetricRegistry registry);
}
