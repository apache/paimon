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
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SegmentsCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link FileStoreTable} which mainly read from the current branch. However, if the current
 * branch does not have a partition, it will read that partition from the fallback branch.
 */
public class FallbackReadFileStoreTable extends DelegatedFileStoreTable {

    private static final Logger LOG = LoggerFactory.getLogger(FallbackReadFileStoreTable.class);

    private final FileStoreTable fallback;

    public FallbackReadFileStoreTable(FileStoreTable wrapped, FileStoreTable fallback) {
        super(wrapped);
        this.fallback = fallback;

        Preconditions.checkArgument(!(wrapped instanceof FallbackReadFileStoreTable));
        if (fallback instanceof FallbackReadFileStoreTable) {
            // ChainGroupReadTable need to be wrapped again
            if (!(fallback instanceof ChainGroupReadTable)) {
                throw new IllegalArgumentException(
                        "This is a bug, perhaps there is a recursive call.");
            }
        }
    }

    public FileStoreTable fallback() {
        return fallback;
    }

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        return new FallbackReadFileStoreTable(
                wrapped.copy(dynamicOptions),
                fallback.copy(rewriteFallbackOptions(dynamicOptions)));
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
        return new FallbackReadFileStoreTable(switchWrappedToBranch(branchName), fallback);
    }

    @Override
    public void setManifestCache(SegmentsCache<Path> manifestCache) {
        super.setManifestCache(manifestCache);
        fallback.setManifestCache(manifestCache);
    }

    protected FileStoreTable switchWrappedToBranch(String branchName) {
        Optional<TableSchema> optionalSchema =
                wrapped.schemaManager().copyWithBranch(branchName).latest();
        Preconditions.checkArgument(
                optionalSchema.isPresent(), "Branch " + branchName + " does not exist");

        TableSchema branchSchema = optionalSchema.get();
        Options branchOptions = new Options(branchSchema.options());
        branchOptions.set(CoreOptions.BRANCH, branchName);
        branchSchema = branchSchema.copy(branchOptions.toMap());
        return FileStoreTableFactory.createWithoutFallbackBranch(
                wrapped.fileIO(),
                wrapped.location(),
                branchSchema,
                new Options(),
                wrapped.catalogEnvironment());
    }

    protected Map<String, String> rewriteFallbackOptions(Map<String, String> options) {
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
        String scanSnapshotId = options.get(scanSnapshotIdOptionKey);
        if (scanSnapshotId != null) {
            long id = Long.parseLong(scanSnapshotId);
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
        return new FallbackReadScan(wrapped.newScan(), fallback.newScan());
    }

    protected void validateSchema() {
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

    /** Split for fallback read. */
    public interface FallbackSplit extends Split {
        boolean isFallback();

        Split wrapped();
    }

    /** Other split (except DataSplit) fallback implementation. */
    public static class FallbackSplitImpl implements FallbackSplit {

        private static final long serialVersionUID = 1L;

        private final Split split;
        private final boolean isFallback;

        public FallbackSplitImpl(Split split, boolean isFallback) {
            this.split = split;
            this.isFallback = isFallback;
        }

        @Override
        public boolean isFallback() {
            return isFallback;
        }

        @Override
        public Split wrapped() {
            return split;
        }

        @Override
        public long rowCount() {
            return split.rowCount();
        }
    }

    /** DataSplit fallback implementation. */
    public static class FallbackDataSplit extends DataSplit implements FallbackSplit {

        private static final long serialVersionUID = 1L;

        private boolean isFallback;

        private FallbackDataSplit(DataSplit dataSplit, boolean isFallback) {
            assign(dataSplit);
            this.isFallback = isFallback;
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o) && isFallback == ((FallbackDataSplit) o).isFallback;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), isFallback);
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            serialize(new DataOutputViewStreamWrapper(out));
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            FallbackDataSplit split = deserialize(new DataInputViewStreamWrapper(in));
            assign(split);
            this.isFallback = split.isFallback;
        }

        @Override
        public void serialize(DataOutputView out) throws IOException {
            super.serialize(out);
            out.writeBoolean(isFallback);
        }

        public static FallbackDataSplit deserialize(DataInputView in) throws IOException {
            DataSplit dataSplit = DataSplit.deserialize(in);
            return new FallbackDataSplit(dataSplit, in.readBoolean());
        }

        @Override
        public boolean isFallback() {
            return isFallback;
        }

        @Override
        public Split wrapped() {
            return this;
        }
    }

    public static FallbackSplit toFallbackSplit(Split split, boolean fallback) {
        if (split instanceof DataSplit) {
            return new FallbackDataSplit((DataSplit) split, fallback);
        }
        return new FallbackSplitImpl(split, fallback);
    }

    /** Scan implementation for {@link FallbackReadFileStoreTable}. */
    public static class FallbackReadScan implements DataTableScan {

        protected final DataTableScan mainScan;
        protected final DataTableScan fallbackScan;

        public FallbackReadScan(DataTableScan mainScan, DataTableScan fallbackScan) {
            this.mainScan = mainScan;
            this.fallbackScan = fallbackScan;
        }

        @Override
        public FallbackReadScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
            mainScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
            fallbackScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
            return this;
        }

        @Override
        public FallbackReadScan withFilter(Predicate predicate) {
            mainScan.withFilter(predicate);
            fallbackScan.withFilter(predicate);
            return this;
        }

        @Override
        public FallbackReadScan withLimit(int limit) {
            mainScan.withLimit(limit);
            fallbackScan.withLimit(limit);
            return this;
        }

        @Override
        public FallbackReadScan withPartitionFilter(Map<String, String> partitionSpec) {
            mainScan.withPartitionFilter(partitionSpec);
            fallbackScan.withPartitionFilter(partitionSpec);
            return this;
        }

        @Override
        public FallbackReadScan withPartitionFilter(List<BinaryRow> partitions) {
            mainScan.withPartitionFilter(partitions);
            fallbackScan.withPartitionFilter(partitions);
            return this;
        }

        @Override
        public InnerTableScan withPartitionsFilter(List<Map<String, String>> partitions) {
            mainScan.withPartitionsFilter(partitions);
            fallbackScan.withPartitionsFilter(partitions);
            return this;
        }

        @Override
        public InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
            mainScan.withPartitionFilter(partitionPredicate);
            fallbackScan.withPartitionFilter(partitionPredicate);
            return this;
        }

        @Override
        public FallbackReadScan withBucketFilter(Filter<Integer> bucketFilter) {
            mainScan.withBucketFilter(bucketFilter);
            fallbackScan.withBucketFilter(bucketFilter);
            return this;
        }

        @Override
        public FallbackReadScan withLevelFilter(Filter<Integer> levelFilter) {
            mainScan.withLevelFilter(levelFilter);
            fallbackScan.withLevelFilter(levelFilter);
            return this;
        }

        @Override
        public FallbackReadScan withMetricRegistry(MetricRegistry metricRegistry) {
            mainScan.withMetricRegistry(metricRegistry);
            fallbackScan.withMetricRegistry(metricRegistry);
            return this;
        }

        @Override
        public InnerTableScan withTopN(TopN topN) {
            mainScan.withTopN(topN);
            fallbackScan.withTopN(topN);
            return this;
        }

        @Override
        public InnerTableScan dropStats() {
            mainScan.dropStats();
            fallbackScan.dropStats();
            return this;
        }

        @Override
        public TableScan.Plan plan() {
            List<Split> splits = new ArrayList<>();
            Set<BinaryRow> completePartitions = new HashSet<>();
            for (Split split : mainScan.plan().splits()) {
                DataSplit dataSplit = (DataSplit) split;
                splits.add(toFallbackSplit(dataSplit, false));
                completePartitions.add(dataSplit.partition());
            }

            List<BinaryRow> remainingPartitions =
                    fallbackScan.listPartitions().stream()
                            .filter(p -> !completePartitions.contains(p))
                            .collect(Collectors.toList());
            if (!remainingPartitions.isEmpty()) {
                fallbackScan.withPartitionFilter(remainingPartitions);
                for (Split split : fallbackScan.plan().splits()) {
                    splits.add(toFallbackSplit(split, true));
                }
            }
            return new DataFilePlan<>(splits);
        }

        @Override
        public List<PartitionEntry> listPartitionEntries() {
            List<PartitionEntry> partitionEntries =
                    new ArrayList<>(mainScan.listPartitionEntries());
            Set<BinaryRow> partitions =
                    partitionEntries.stream()
                            .map(PartitionEntry::partition)
                            .collect(Collectors.toSet());
            List<PartitionEntry> fallBackPartitionEntries = fallbackScan.listPartitionEntries();
            fallBackPartitionEntries.stream()
                    .filter(e -> !partitions.contains(e.partition()))
                    .forEach(partitionEntries::add);
            return partitionEntries;
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
        public InnerTableRead withReadType(RowType readType) {
            mainRead.withReadType(readType);
            fallbackRead.withReadType(readType);
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
            if (split instanceof FallbackSplit) {
                FallbackSplit fallbackSplit = (FallbackSplit) split;
                if (fallbackSplit.isFallback()) {
                    try {
                        return fallbackRead.createReader(fallbackSplit.wrapped());
                    } catch (Exception ignored) {
                        LOG.error(
                                "Reading from fallback branch has problems: {}",
                                fallbackSplit.wrapped());
                    }
                }
            }
            return mainRead.createReader(split);
        }
    }
}
