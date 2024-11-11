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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** Read splits from specified {@link Snapshot} with given configuration. */
public interface SnapshotReader {

    @Nullable
    Integer parallelism();

    SnapshotManager snapshotManager();

    ManifestsReader manifestsReader();

    List<ManifestEntry> readManifest(ManifestFileMeta manifest);

    ConsumerManager consumerManager();

    SplitGenerator splitGenerator();

    FileStorePathFactory pathFactory();

    SnapshotReader withSnapshot(long snapshotId);

    SnapshotReader withSnapshot(Snapshot snapshot);

    SnapshotReader withFilter(Predicate predicate);

    SnapshotReader withPartitionFilter(Map<String, String> partitionSpec);

    SnapshotReader withPartitionFilter(Predicate predicate);

    SnapshotReader withPartitionFilter(List<BinaryRow> partitions);

    SnapshotReader withMode(ScanMode scanMode);

    SnapshotReader withLevelFilter(Filter<Integer> levelFilter);

    SnapshotReader withManifestEntryFilter(Filter<ManifestEntry> filter);

    SnapshotReader withBucket(int bucket);

    SnapshotReader withBuckets(List<Integer> buckets);

    SnapshotReader withBucketFilter(Filter<Integer> bucketFilter);

    SnapshotReader withDataFileNameFilter(Filter<String> fileNameFilter);

    SnapshotReader withShard(int indexOfThisSubtask, int numberOfParallelSubtasks);

    SnapshotReader withMetricRegistry(MetricRegistry registry);

    /** Get splits plan from snapshot. */
    Plan read();

    /** Get splits plan from file changes. */
    Plan readChanges();

    Plan readIncrementalDiff(Snapshot before);

    /** List partitions. */
    List<BinaryRow> partitions();

    List<PartitionEntry> partitionEntries();

    List<BucketEntry> bucketEntries();

    /** Result plan of this scan. */
    interface Plan extends TableScan.Plan {

        @Nullable
        Long watermark();

        /**
         * Snapshot id of this plan, return null if the table is empty or the manifest list is
         * specified.
         */
        @Nullable
        Long snapshotId();

        /** Result splits. */
        List<Split> splits();

        default List<DataSplit> dataSplits() {
            return (List) splits();
        }
    }
}
