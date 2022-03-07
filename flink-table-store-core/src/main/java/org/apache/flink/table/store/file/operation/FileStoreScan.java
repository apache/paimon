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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.predicate.Predicate;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Scan operation which produces a plan. */
public interface FileStoreScan {

    boolean snapshotExists(long snapshotId);

    Snapshot snapshot(long snapshotId);

    FileStoreScan withPartitionFilter(Predicate predicate);

    FileStoreScan withPartitionFilter(List<BinaryRowData> partitions);

    FileStoreScan withKeyFilter(Predicate predicate);

    FileStoreScan withValueFilter(Predicate predicate);

    FileStoreScan withBucket(int bucket);

    FileStoreScan withSnapshot(long snapshotId);

    FileStoreScan withManifestList(List<ManifestFileMeta> manifests);

    FileStoreScan withIncremental(boolean isIncremental);

    /** Produce a {@link Plan}. */
    Plan plan();

    /** Result plan of this scan. */
    interface Plan {

        /** Snapshot id of this plan, return null if manifest list is specified. */
        @Nullable
        Long snapshotId();

        /** Result {@link ManifestEntry} files. */
        List<ManifestEntry> files();

        /** Return a map group by partition and bucket. */
        default Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> groupByPartFiles() {
            List<ManifestEntry> files = files();
            Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> groupBy = new HashMap<>();
            for (ManifestEntry entry : files) {
                checkArgument(entry.kind() == ValueKind.ADD);
                groupBy.computeIfAbsent(entry.partition(), k -> new HashMap<>())
                        .computeIfAbsent(entry.bucket(), k -> new ArrayList<>())
                        .add(entry.file());
            }
            return groupBy;
        }
    }
}
