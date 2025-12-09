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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.variant.VariantAccessInfo;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexScanBuilder;
import org.apache.paimon.globalindex.ScoreFunction;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.globalindex.GlobalIndexScanBuilder.parallelScan;

/** A {@link Table} for reading table with global index. */
public class GlobalIndexedTable implements DataTable, ReadonlyTable {

    private final FileStoreTable wrapped;

    public GlobalIndexedTable(FileStoreTable wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public Optional<Snapshot> latestSnapshot() {
        return wrapped.latestSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return wrapped.snapshot(snapshotId);
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
    public SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        return wrapped.indexManifestFileReader();
    }

    @Override
    public String name() {
        return wrapped.name();
    }

    @Override
    public RowType rowType() {
        return wrapped.rowType();
    }

    @Override
    public List<String> partitionKeys() {
        return wrapped.partitionKeys();
    }

    @Override
    public Map<String, String> options() {
        return wrapped.options();
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.emptyList();
    }

    @Override
    public SnapshotReader newSnapshotReader() {
        return wrapped.newSnapshotReader();
    }

    @Override
    public DataTableScan newScan() {
        return new GlobalIndexBatchScan(wrapped.newScan());
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        return wrapped.newStreamScan();
    }

    @Override
    public CoreOptions coreOptions() {
        return wrapped.coreOptions();
    }

    @Override
    public Path location() {
        return wrapped.location();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return wrapped.snapshotManager();
    }

    @Override
    public ChangelogManager changelogManager() {
        return wrapped.changelogManager();
    }

    @Override
    public ConsumerManager consumerManager() {
        return wrapped.consumerManager();
    }

    @Override
    public SchemaManager schemaManager() {
        return wrapped.schemaManager();
    }

    @Override
    public TagManager tagManager() {
        return wrapped.tagManager();
    }

    @Override
    public BranchManager branchManager() {
        return wrapped.branchManager();
    }

    @Override
    public DataTable switchToBranch(String branchName) {
        return new GlobalIndexedTable(wrapped.switchToBranch(branchName));
    }

    @Override
    public InnerTableRead newRead() {
        return new GlobalIndexTableRead(wrapped.rowType(), wrapped.newRead());
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new GlobalIndexedTable(wrapped.copy(dynamicOptions));
    }

    @Override
    public FileIO fileIO() {
        return wrapped.fileIO();
    }

    /** Scan for table with global index. */
    public class GlobalIndexBatchScan implements DataTableScan {

        private final DataTableScan batchScan;
        private GlobalIndexResult globalIndexResult;
        private Predicate filter;

        private GlobalIndexBatchScan(DataTableScan batchScan) {
            this.batchScan = batchScan;
        }

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            this.filter = predicate;
            batchScan.withFilter(predicate);
            return this;
        }

        @Override
        public InnerTableScan withMetricRegistry(MetricRegistry metricsRegistry) {
            batchScan.withMetricRegistry(metricsRegistry);
            return this;
        }

        @Override
        public InnerTableScan withLimit(int limit) {
            batchScan.withLimit(limit);
            return this;
        }

        @Override
        public InnerTableScan withPartitionFilter(Map<String, String> partitionSpec) {
            batchScan.withPartitionFilter(partitionSpec);
            return this;
        }

        @Override
        public InnerTableScan withPartitionFilter(List<BinaryRow> partitions) {
            batchScan.withPartitionFilter(partitions);
            return this;
        }

        @Override
        public InnerTableScan withPartitionsFilter(List<Map<String, String>> partitions) {
            batchScan.withPartitionsFilter(partitions);
            return this;
        }

        @Override
        public InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
            batchScan.withPartitionFilter(partitionPredicate);
            return this;
        }

        @Override
        public InnerTableScan withBucketFilter(Filter<Integer> bucketFilter) {
            batchScan.withBucketFilter(bucketFilter);
            return this;
        }

        @Override
        public InnerTableScan withLevelFilter(Filter<Integer> levelFilter) {
            batchScan.withLevelFilter(levelFilter);
            return this;
        }

        @Override
        public InnerTableScan withRowRanges(List<Range> rowRanges) {
            if (rowRanges != null) {
                this.globalIndexResult = GlobalIndexResult.fromRanges(rowRanges);
            }
            return this;
        }

        public InnerTableScan withGlobalIndexResult(GlobalIndexResult globalIndexResult) {
            this.globalIndexResult = globalIndexResult;
            return this;
        }

        private void configureGlobalIndex(InnerTableScan scan) {
            if (globalIndexResult == null && filter != null) {
                PartitionPredicate partitionPredicate = scan.partitionFilter();
                GlobalIndexScanBuilder globalIndexScanBuilder = wrapped.newIndexScanBuilder();
                Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(wrapped);
                globalIndexScanBuilder
                        .withPartitionPredicate(partitionPredicate)
                        .withSnapshot(snapshot);
                List<Range> indexedRowRanges = globalIndexScanBuilder.shardList();
                if (!indexedRowRanges.isEmpty()) {
                    List<Range> nonIndexedRowRanges =
                            new Range(0, snapshot.nextRowId() - 1).exclude(indexedRowRanges);
                    Optional<GlobalIndexResult> combined =
                            parallelScan(indexedRowRanges, globalIndexScanBuilder, filter);
                    if (combined.isPresent()) {
                        GlobalIndexResult globalIndexResultTemp = combined.get();
                        if (!nonIndexedRowRanges.isEmpty()) {
                            for (Range range : nonIndexedRowRanges) {
                                globalIndexResultTemp.or(GlobalIndexResult.fromRange(range));
                            }
                        }

                        this.globalIndexResult = globalIndexResultTemp;
                    }
                }
            }

            if (this.globalIndexResult != null) {
                scan.withRowRanges(this.globalIndexResult.results().toRangeList());
            }
        }

        @Override
        public Plan plan() {
            configureGlobalIndex(batchScan);
            List<Split> splits = batchScan.plan().splits();
            return wrap(splits);
        }

        private Plan wrap(List<Split> splits) {
            if (globalIndexResult == null) {
                return () -> splits;
            }
            List<Split> indexedSplits = new ArrayList<>();
            for (Split split : splits) {
                DataSplit dataSplit = (DataSplit) split;
                List<Range> fromDataFile =
                        Range.mergeSortedAsPossible(
                                dataSplit.dataFiles().stream()
                                        .map(
                                                d ->
                                                        new Range(
                                                                d.nonNullFirstRowId(),
                                                                d.nonNullFirstRowId()
                                                                        + d.rowCount()
                                                                        - 1))
                                        .collect(Collectors.toList()));

                List<Range> expected =
                        Range.and(fromDataFile, globalIndexResult.results().toRangeList());

                Float[] scores = null;
                ScoreFunction scoreFunction = globalIndexResult.scoreFunction();
                if (scoreFunction != null) {
                    int size = expected.stream().mapToInt(r -> (int) (r.to - r.from + 1)).sum();
                    scores = new Float[size];

                    int index = 0;
                    for (Range range : expected) {
                        for (long i = range.from; i <= range.to; i++) {
                            scores[index++] = scoreFunction.score(i);
                        }
                    }
                }

                indexedSplits.add(new IndexedSplit(dataSplit, expected, scores));
            }
            return () -> indexedSplits;
        }

        @Override
        public List<PartitionEntry> listPartitionEntries() {
            return batchScan.listPartitionEntries();
        }

        @Override
        public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
            batchScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
            return this;
        }
    }

    private static class IndexedSplit implements Split {
        private final DataSplit split;
        @NotNull private final List<Range> rowRanges;
        @Nullable private final Float[] scores;

        public IndexedSplit(
                DataSplit split, @NotNull List<Range> rowRanges, @Nullable Float[] scores) {
            this.split = split;
            this.rowRanges = rowRanges;
            this.scores = scores;
        }

        @Override
        public long rowCount() {
            return rowRanges.stream().mapToLong(r -> r.to - r.from + 1).sum();
        }
    }

    private static class GlobalIndexTableRead implements InnerTableRead {

        private final InnerTableRead dataRead;
        private RowType readType;
        private List<Range> rowRanges;

        protected GlobalIndexTableRead(RowType rowType, InnerTableRead dataRead) {
            this.readType = rowType;
            this.dataRead = dataRead.forceKeepDelete();
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            dataRead.withFilter(predicate);
            return this;
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            this.readType = readType;
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            this.dataRead.withIOManager(ioManager);
            return this;
        }

        @Override
        public InnerTableRead withRowRanges(List<Range> rowRanges) {
            this.rowRanges = rowRanges;
            return this;
        }

        @Override
        public TableRead executeFilter() {
            dataRead.executeFilter();
            return this;
        }

        @Override
        public InnerTableRead withFilter(List<Predicate> predicates) {
            dataRead.withFilter(predicates);
            return this;
        }

        @Override
        public InnerTableRead withTopN(TopN topN) {
            dataRead.withTopN(topN);
            return this;
        }

        @Override
        public InnerTableRead withVariantAccess(VariantAccessInfo[] variantAccessInfo) {
            dataRead.withVariantAccess(variantAccessInfo);
            return this;
        }

        @Override
        public InnerTableRead withLimit(int limit) {
            dataRead.withLimit(limit);
            return this;
        }

        @Override
        public InnerTableRead withMetricRegistry(MetricRegistry registry) {
            dataRead.withMetricRegistry(registry);
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (split instanceof IndexedSplit) {
                IndexedSplit indexedSplit = (IndexedSplit) split;
                List<Range> pushDownRanges = this.rowRanges;
                List<Range> splitRowRanges = indexedSplit.rowRanges;
                if (pushDownRanges != null) {
                    pushDownRanges = Range.and(this.rowRanges, splitRowRanges);
                } else {
                    pushDownRanges = splitRowRanges;
                }

                dataRead.withRowRanges(pushDownRanges);

                Map<Long, Float> rowIdToScore = null;
                if (indexedSplit.scores != null) {
                    rowIdToScore = new HashMap<>();
                    int index = 0;
                    for (Range range : indexedSplit.rowRanges) {
                        for (long i = range.from; i <= range.to; i++) {
                            rowIdToScore.put(i, indexedSplit.scores[index++]);
                        }
                    }
                }

                int rowIdIndex = readType.getFieldIndex(SpecialFields.ROW_ID.name());
                RowType actualReadType = readType;
                ProjectedRow projectedRow = null;

                if (rowIdToScore != null && rowIdIndex == -1) {
                    actualReadType = SpecialFields.rowTypeWithRowId(readType);
                    rowIdIndex = actualReadType.getFieldCount() - 1;
                    int[] mappings = new int[readType.getFieldCount()];
                    for (int i = 0; i < readType.getFieldCount(); i++) {
                        mappings[i] = i;
                    }
                    projectedRow = ProjectedRow.from(mappings);
                }

                dataRead.withReadType(actualReadType);
                return new ReaderWithScore(
                        dataRead.createReader(indexedSplit.split),
                        rowIdToScore,
                        rowIdIndex,
                        projectedRow);
            } else {
                if (readType != null) {
                    dataRead.withReadType(readType);
                }
                return dataRead.createReader(split);
            }
        }
    }

    private static class ReaderWithScore implements RecordReader<InternalRow> {
        private final RecordReader<InternalRow> reader;
        @Nullable private final Map<Long, Float> rowIdToScore;
        private final int rowIdIndex;
        private final ProjectedRow projectedRow;

        public ReaderWithScore(
                RecordReader<InternalRow> reader,
                @Nullable Map<Long, Float> rowIdToScore,
                int rowIdIndex,
                @Nullable ProjectedRow projectedRow) {
            this.reader = reader;
            this.rowIdToScore = rowIdToScore;
            this.rowIdIndex = rowIdIndex;
            this.projectedRow = projectedRow;
        }

        @Nullable
        @Override
        public ScoreRecordIterator<InternalRow> readBatch() throws IOException {
            RecordIterator<InternalRow> iterator = reader.readBatch();
            if (iterator == null) {
                return null;
            }
            return new ScoreRecordIterator<InternalRow>() {

                private Float score = null;

                @Override
                public Float returnedScore() {
                    return score;
                }

                @Override
                public InternalRow next() throws IOException {
                    InternalRow row = iterator.next();
                    if (row != null && rowIdToScore != null) {
                        Long rowId = row.getLong(rowIdIndex);
                        this.score = rowIdToScore.get(rowId);
                        if (projectedRow != null) {
                            projectedRow.replaceRow(row);
                            return projectedRow;
                        }
                    }
                    return row;
                }

                @Override
                public void releaseBatch() {
                    iterator.releaseBatch();
                }
            };
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
