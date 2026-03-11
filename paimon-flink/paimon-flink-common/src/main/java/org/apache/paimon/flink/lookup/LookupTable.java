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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;

/** A lookup table which provides get and refresh. */
public interface LookupTable extends Closeable {

    void specifyPartitions(List<BinaryRow> scanPartitions, @Nullable Predicate partitionFilter);

    void open() throws Exception;

    List<InternalRow> get(InternalRow key) throws IOException;

    void refresh() throws Exception;

    void specifyCacheRowFilter(Filter<InternalRow> filter);

    Long nextSnapshotId();

    // ---- Partition refresh methods ----

    /**
     * Create a new LookupTable instance with the same configuration but a different temp path. The
     * new table is not opened yet.
     *
     * @throws UnsupportedOperationException if the implementation does not support this operation
     */
    default LookupTable copyWithNewPath(File newPath) {
        throw new UnsupportedOperationException(
                "copyWithNewPath is not supported by " + getClass().getSimpleName());
    }

    /**
     * Start refresh partition.
     *
     * @param newPartitions the new partitions to refresh to
     * @param partitionFilter the partition filter for the new partitions
     */
    default void startPartitionRefresh(
            List<BinaryRow> newPartitions, @Nullable Predicate partitionFilter) throws Exception {
        close();
        specifyPartitions(newPartitions, partitionFilter);
        open();
    }

    /**
     * Check if an async partition refresh has completed. If a new table is ready, this method
     * returns it and the caller should replace its current lookup table reference. Returns {@code
     * null} if no switch is needed.
     *
     * <p>For synchronous partition refresh, this always returns {@code null}.
     */
    @Nullable
    default LookupTable checkPartitionRefreshCompletion() throws Exception {
        return null;
    }

    /**
     * Return the partitions that the current lookup table was loaded with. During async refresh,
     * this may differ from the latest partitions detected by the partition loader.
     *
     * @return the active partitions, or {@code null} if partition refresh is not managed by this
     *     table
     */
    @Nullable
    default List<BinaryRow> scanPartitions() {
        return null;
    }
}
