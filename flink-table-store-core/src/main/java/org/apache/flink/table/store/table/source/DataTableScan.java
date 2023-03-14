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

package org.apache.flink.table.store.table.source;

import org.apache.flink.table.store.annotation.VisibleForTesting;
import org.apache.flink.table.store.file.operation.ScanKind;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.utils.Filter;

import javax.annotation.Nullable;

import java.util.List;

/** A {@link TableScan} for reading data. */
public interface DataTableScan extends InnerTableScan {

    DataTableScan withKind(ScanKind kind);

    DataTableScan withSnapshot(long snapshotId);

    DataTableScan withLevelFilter(Filter<Integer> levelFilter);

    DataTableScan withFilter(Predicate predicate);

    DataTableScan.DataFilePlan plan();

    /** Scanning plan containing snapshot ID and input splits. */
    class DataFilePlan implements Plan {

        @Nullable public final Long snapshotId;
        public final List<DataSplit> splits;

        @VisibleForTesting
        public DataFilePlan(@Nullable Long snapshotId, List<DataSplit> splits) {
            this.snapshotId = snapshotId;
            this.splits = splits;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public List<Split> splits() {
            return (List) splits;
        }
    }
}
