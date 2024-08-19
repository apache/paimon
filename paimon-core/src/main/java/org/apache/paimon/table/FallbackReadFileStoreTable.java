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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SimpleFileReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link FileStoreTable} which mainly read from the current branch. However, if the current
 * branch does not have a partition, it will read that partition from the fallback branch.
 */
public class FallbackReadFileStoreTable extends DelegatedFileStoreTable {

    private final FileStoreTable fallback;

    public FallbackReadFileStoreTable(FileStoreTable main, FileStoreTable fallback) {
        super(main);
        this.fallback = fallback;

        Preconditions.checkArgument(!(main instanceof FallbackReadFileStoreTable));
        Preconditions.checkArgument(!(fallback instanceof FallbackReadFileStoreTable));
    }

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        return new FallbackReadFileStoreTable(
                wrapped.copy(dynamicOptions),
                fallback.copy(rewriteFallbackOptions(dynamicOptions)));
    }

    @Override
    public SimpleFileReader<ManifestFileMeta> manifestListReader() {
        return wrapped.manifestListReader();
    }

    @Override
    public SimpleFileReader<ManifestEntry> manifestFileReader() {
        return wrapped.manifestFileReader();
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        return new FallbackReadFileStoreTable(
                wrapped.copy(newTableSchema),
                fallback.copy(
                        newTableSchema.copy(rewriteFallbackOptions(newTableSchema.options()))));
    }

    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new FallbackReadFileStoreTable(
                wrapped.copyWithoutTimeTravel(dynamicOptions),
                fallback.copyWithoutTimeTravel(rewriteFallbackOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        return new FallbackReadFileStoreTable(
                wrapped.copyWithLatestSchema(), fallback.copyWithLatestSchema());
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        return new FallbackReadFileStoreTable(wrapped.switchToBranch(branchName), fallback);
    }

    private Map<String, String> rewriteFallbackOptions(Map<String, String> options) {
        Map<String, String> result = new HashMap<>(options);

        // branch of fallback table should never change
        String branchKey = CoreOptions.BRANCH.key();
        if (options.containsKey(branchKey)) {
            result.put(branchKey, fallback.options().get(branchKey));
        }

        // snapshot ids may be different between the main branch and the fallback branch,
        // so we need to convert main branch snapshot id to millisecond,
        // then convert millisecond to fallback branch snapshot id
        String scanSnapshotIdOptionKey = CoreOptions.SCAN_SNAPSHOT_ID.key();
        if (options.containsKey(scanSnapshotIdOptionKey)) {
            long id = Long.parseLong(options.get(scanSnapshotIdOptionKey));
            long millis = wrapped.snapshotManager().snapshot(id).timeMillis();
            Snapshot fallbackSnapshot = fallback.snapshotManager().earlierOrEqualTimeMills(millis);
            long fallbackId;
            if (fallbackSnapshot == null) {
                fallbackId = Snapshot.FIRST_SNAPSHOT_ID;
            } else {
                fallbackId = fallbackSnapshot.id();
            }
            result.put(scanSnapshotIdOptionKey, String.valueOf(fallbackId));
        }

        // bucket number of main branch and fallback branch are very likely different,
        // so we remove bucket in options to use fallback branch's bucket number
        result.remove(CoreOptions.BUCKET.key());

        return result;
    }

    @Override
    public DataTableScan newScan() {
        validateSchema();
        return new Scan();
    }

    private void validateSchema() {
        String mainBranch = wrapped.coreOptions().branch();
        String fallbackBranch = fallback.coreOptions().branch();
        RowType mainRowType = wrapped.schema().logicalRowType();
        RowType fallbackRowType = fallback.schema().logicalRowType();
        Preconditions.checkArgument(
                sameRowTypeIgnoreNullable(mainRowType, fallbackRowType),
                "Branch %s and %s does not have the same row type.\n"
                        + "Row type of branch %s is %s.\n"
                        + "Row type of branch %s is %s.",
                mainBranch,
                fallbackBranch,
                mainBranch,
                mainRowType,
                fallbackBranch,
                fallbackRowType);

        List<String> mainPrimaryKeys = wrapped.schema().primaryKeys();
        List<String> fallbackPrimaryKeys = fallback.schema().primaryKeys();
        if (!mainPrimaryKeys.isEmpty()) {
            if (fallbackPrimaryKeys.isEmpty()) {
                throw new IllegalArgumentException(
                        "Branch "
                                + mainBranch
                                + " has primary keys while fallback branch "
                                + fallbackBranch
                                + " does not. This is not allowed.");
            }
            Preconditions.checkArgument(
                    mainPrimaryKeys.equals(fallbackPrimaryKeys),
                    "Branch %s and %s both have primary keys but are not the same.\n"
                            + "Primary keys of %s are %s.\n"
                            + "Primary keys of %s are %s.",
                    mainBranch,
                    fallbackBranch,
                    mainBranch,
                    mainPrimaryKeys,
                    fallbackBranch,
                    fallbackPrimaryKeys);
        }
    }

    private boolean sameRowTypeIgnoreNullable(RowType mainRowType, RowType fallbackRowType) {
        if (mainRowType.getFieldCount() != fallbackRowType.getFieldCount()) {
            return false;
        }
        for (int i = 0; i < mainRowType.getFieldCount(); i++) {
            DataType mainType = mainRowType.getFields().get(i).type();
            DataType fallbackType = fallbackRowType.getFields().get(i).type();
            if (!mainType.equalsIgnoreNullable(fallbackType)) {
                return false;
            }
        }
        return true;
    }

    private class Scan implements DataTableScan {

        private final DataTableScan mainScan;
        private final DataTableScan fallbackScan;

        private Scan() {
            this.mainScan = wrapped.newScan();
            this.fallbackScan = fallback.newScan();
        }

        @Override
        public Scan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
            mainScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
            fallbackScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
            return this;
        }

        @Override
        public Scan withFilter(Predicate predicate) {
            mainScan.withFilter(predicate);
            fallbackScan.withFilter(predicate);
            return this;
        }

        @Override
        public Scan withLimit(int limit) {
            mainScan.withLimit(limit);
            fallbackScan.withLimit(limit);
            return this;
        }

        @Override
        public Scan withPartitionFilter(Map<String, String> partitionSpec) {
            mainScan.withPartitionFilter(partitionSpec);
            fallbackScan.withPartitionFilter(partitionSpec);
            return this;
        }

        @Override
        public Scan withPartitionFilter(List<BinaryRow> partitions) {
            mainScan.withPartitionFilter(partitions);
            fallbackScan.withPartitionFilter(partitions);
            return this;
        }

        @Override
        public Scan withBucketFilter(Filter<Integer> bucketFilter) {
            mainScan.withBucketFilter(bucketFilter);
            fallbackScan.withBucketFilter(bucketFilter);
            return this;
        }

        @Override
        public Scan withLevelFilter(Filter<Integer> levelFilter) {
            mainScan.withLevelFilter(levelFilter);
            fallbackScan.withLevelFilter(levelFilter);
            return this;
        }

        @Override
        public Scan withMetricsRegistry(MetricRegistry metricRegistry) {
            mainScan.withMetricsRegistry(metricRegistry);
            fallbackScan.withMetricsRegistry(metricRegistry);
            return this;
        }

        @Override
        public TableScan.Plan plan() {
            List<DataSplit> splits = new ArrayList<>();
            Set<BinaryRow> completePartitions = new HashSet<>();
            for (Split split : mainScan.plan().splits()) {
                DataSplit dataSplit = (DataSplit) split;
                splits.add(dataSplit);
                completePartitions.add(dataSplit.partition());
            }

            List<BinaryRow> remainingPartitions =
                    fallbackScan.listPartitions().stream()
                            .filter(p -> !completePartitions.contains(p))
                            .collect(Collectors.toList());
            if (!remainingPartitions.isEmpty()) {
                fallbackScan.withPartitionFilter(remainingPartitions);
                for (Split split : fallbackScan.plan().splits()) {
                    splits.add((DataSplit) split);
                }
            }
            return new DataFilePlan(splits);
        }

        @Override
        public List<BinaryRow> listPartitions() {
            Set<BinaryRow> partitions = new LinkedHashSet<>(mainScan.listPartitions());
            partitions.addAll(fallbackScan.listPartitions());
            return new ArrayList<>(partitions);
        }
    }

    @Override
    public InnerTableRead newRead() {
        return new Read();
    }

    private class Read implements InnerTableRead {

        private final InnerTableRead mainRead;
        private final InnerTableRead fallbackRead;

        private Read() {
            this.mainRead = wrapped.newRead();
            this.fallbackRead = fallback.newRead();
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            mainRead.withFilter(predicate);
            fallbackRead.withFilter(predicate);
            return this;
        }

        @Override
        public InnerTableRead withProjection(int[][] projection) {
            mainRead.withProjection(projection);
            fallbackRead.withProjection(projection);
            return this;
        }

        @Override
        public InnerTableRead forceKeepDelete() {
            mainRead.forceKeepDelete();
            fallbackRead.forceKeepDelete();
            return this;
        }

        @Override
        public TableRead executeFilter() {
            mainRead.executeFilter();
            fallbackRead.executeFilter();
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            mainRead.withIOManager(ioManager);
            fallbackRead.withIOManager(ioManager);
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            DataSplit dataSplit = (DataSplit) split;
            if (!dataSplit.dataFiles().isEmpty()
                    && dataSplit.dataFiles().get(0).minKey().getFieldCount() > 0) {
                return fallbackRead.createReader(split);
            } else {
                return mainRead.createReader(split);
            }
        }
    }
}
