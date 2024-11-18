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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateReplaceVisitor;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingContext;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for reading audit log of table. */
public class AuditLogTable implements DataTable, ReadonlyTable {

    public static final String AUDIT_LOG = "audit_log";

    public static final PredicateReplaceVisitor PREDICATE_CONVERTER =
            p -> {
                if (p.index() == 0) {
                    return Optional.empty();
                }
                return Optional.of(
                        new LeafPredicate(
                                p.function(),
                                p.type(),
                                p.index() - 1,
                                p.fieldName(),
                                p.literals()));
            };

    private final FileStoreTable wrapped;

    public AuditLogTable(FileStoreTable wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public OptionalLong latestSnapshotId() {
        return wrapped.latestSnapshotId();
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
        return wrapped.name() + SYSTEM_TABLE_SPLITTER + AUDIT_LOG;
    }

    @Override
    public RowType rowType() {
        List<DataField> fields = new ArrayList<>();
        fields.add(SpecialFields.ROW_KIND);
        fields.addAll(wrapped.rowType().getFields());
        return new RowType(fields);
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
        return new AuditLogDataReader(wrapped.newSnapshotReader());
    }

    @Override
    public DataTableScan newScan() {
        return new AuditLogBatchScan(wrapped.newScan());
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        return new AuditLogStreamScan(wrapped.newStreamScan());
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
    public TagManager tagManager() {
        return wrapped.tagManager();
    }

    @Override
    public BranchManager branchManager() {
        return wrapped.branchManager();
    }

    @Override
    public DataTable switchToBranch(String branchName) {
        return new AuditLogTable(wrapped.switchToBranch(branchName));
    }

    @Override
    public InnerTableRead newRead() {
        return new AuditLogRead(wrapped.newRead());
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new AuditLogTable(wrapped.copy(dynamicOptions));
    }

    @Override
    public FileIO fileIO() {
        return wrapped.fileIO();
    }

    /** Push down predicate to dataScan and dataRead. */
    private Optional<Predicate> convert(Predicate predicate) {
        List<Predicate> result =
                PredicateBuilder.splitAnd(predicate).stream()
                        .map(p -> p.visit(PREDICATE_CONVERTER))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());
        if (result.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(PredicateBuilder.and(result));
    }

    private class AuditLogDataReader implements SnapshotReader {

        private final SnapshotReader wrapped;

        private AuditLogDataReader(SnapshotReader wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public Integer parallelism() {
            return wrapped.parallelism();
        }

        @Override
        public SnapshotManager snapshotManager() {
            return wrapped.snapshotManager();
        }

        @Override
        public ManifestsReader manifestsReader() {
            return wrapped.manifestsReader();
        }

        @Override
        public List<ManifestEntry> readManifest(ManifestFileMeta manifest) {
            return wrapped.readManifest(manifest);
        }

        @Override
        public ConsumerManager consumerManager() {
            return wrapped.consumerManager();
        }

        @Override
        public SplitGenerator splitGenerator() {
            return wrapped.splitGenerator();
        }

        @Override
        public FileStorePathFactory pathFactory() {
            return wrapped.pathFactory();
        }

        public SnapshotReader withSnapshot(long snapshotId) {
            wrapped.withSnapshot(snapshotId);
            return this;
        }

        public SnapshotReader withSnapshot(Snapshot snapshot) {
            wrapped.withSnapshot(snapshot);
            return this;
        }

        public SnapshotReader withFilter(Predicate predicate) {
            convert(predicate).ifPresent(wrapped::withFilter);
            return this;
        }

        @Override
        public SnapshotReader withPartitionFilter(Map<String, String> partitionSpec) {
            wrapped.withPartitionFilter(partitionSpec);
            return this;
        }

        @Override
        public SnapshotReader withPartitionFilter(Predicate predicate) {
            wrapped.withPartitionFilter(predicate);
            return this;
        }

        @Override
        public SnapshotReader withPartitionFilter(List<BinaryRow> partitions) {
            wrapped.withPartitionFilter(partitions);
            return this;
        }

        @Override
        public SnapshotReader withMode(ScanMode scanMode) {
            wrapped.withMode(scanMode);
            return this;
        }

        @Override
        public SnapshotReader withLevelFilter(Filter<Integer> levelFilter) {
            wrapped.withLevelFilter(levelFilter);
            return this;
        }

        @Override
        public SnapshotReader withManifestEntryFilter(Filter<ManifestEntry> filter) {
            wrapped.withManifestEntryFilter(filter);
            return this;
        }

        public SnapshotReader withBucket(int bucket) {
            wrapped.withBucket(bucket);
            return this;
        }

        @Override
        public SnapshotReader withBucketFilter(Filter<Integer> bucketFilter) {
            wrapped.withBucketFilter(bucketFilter);
            return this;
        }

        @Override
        public SnapshotReader withDataFileNameFilter(Filter<String> fileNameFilter) {
            wrapped.withDataFileNameFilter(fileNameFilter);
            return this;
        }

        @Override
        public SnapshotReader dropStats() {
            wrapped.dropStats();
            return this;
        }

        @Override
        public SnapshotReader withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
            wrapped.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
            return this;
        }

        @Override
        public SnapshotReader withMetricRegistry(MetricRegistry registry) {
            wrapped.withMetricRegistry(registry);
            return this;
        }

        @Override
        public Plan read() {
            return wrapped.read();
        }

        @Override
        public Plan readChanges() {
            return wrapped.readChanges();
        }

        @Override
        public Plan readIncrementalDiff(Snapshot before) {
            return wrapped.readIncrementalDiff(before);
        }

        @Override
        public List<BinaryRow> partitions() {
            return wrapped.partitions();
        }

        @Override
        public List<PartitionEntry> partitionEntries() {
            return wrapped.partitionEntries();
        }

        @Override
        public List<BucketEntry> bucketEntries() {
            return wrapped.bucketEntries();
        }

        @Override
        public Iterator<ManifestEntry> readFileIterator() {
            return wrapped.readFileIterator();
        }
    }

    private class AuditLogBatchScan implements DataTableScan {

        private final DataTableScan batchScan;

        private AuditLogBatchScan(DataTableScan batchScan) {
            this.batchScan = batchScan;
        }

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            convert(predicate).ifPresent(batchScan::withFilter);
            return this;
        }

        @Override
        public InnerTableScan withMetricsRegistry(MetricRegistry metricsRegistry) {
            batchScan.withMetricsRegistry(metricsRegistry);
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
        public Plan plan() {
            return batchScan.plan();
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

    private class AuditLogStreamScan implements StreamDataTableScan {

        private final StreamDataTableScan streamScan;

        private AuditLogStreamScan(StreamDataTableScan streamScan) {
            this.streamScan = streamScan;
        }

        @Override
        public StreamDataTableScan withFilter(Predicate predicate) {
            convert(predicate).ifPresent(streamScan::withFilter);
            return this;
        }

        @Override
        public StartingContext startingContext() {
            return streamScan.startingContext();
        }

        @Override
        public Plan plan() {
            return streamScan.plan();
        }

        @Override
        public List<PartitionEntry> listPartitionEntries() {
            return streamScan.listPartitionEntries();
        }

        @Nullable
        @Override
        public Long checkpoint() {
            return streamScan.checkpoint();
        }

        @Nullable
        @Override
        public Long watermark() {
            return streamScan.watermark();
        }

        @Override
        public void restore(@Nullable Long nextSnapshotId) {
            streamScan.restore(nextSnapshotId);
        }

        @Override
        public void restore(@Nullable Long nextSnapshotId, boolean scanAllSnapshot) {
            streamScan.restore(nextSnapshotId, scanAllSnapshot);
        }

        @Override
        public void notifyCheckpointComplete(@Nullable Long nextSnapshot) {
            streamScan.notifyCheckpointComplete(nextSnapshot);
        }

        @Override
        public StreamDataTableScan withMetricsRegistry(MetricRegistry metricsRegistry) {
            streamScan.withMetricsRegistry(metricsRegistry);
            return this;
        }

        @Override
        public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
            streamScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
            return this;
        }
    }

    class AuditLogRead implements InnerTableRead {

        protected final InnerTableRead dataRead;

        protected int[] readProjection;

        protected AuditLogRead(InnerTableRead dataRead) {
            this.dataRead = dataRead.forceKeepDelete();
            this.readProjection = defaultProjection();
        }

        /** Default projection, just add row kind to the first. */
        private int[] defaultProjection() {
            int dataFieldCount = wrapped.rowType().getFieldCount();
            int[] projection = new int[dataFieldCount + 1];
            projection[0] = -1;
            for (int i = 0; i < dataFieldCount; i++) {
                projection[i + 1] = i;
            }
            return projection;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            convert(predicate).ifPresent(dataRead::withFilter);
            return this;
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            // data projection to push down to dataRead
            List<DataField> dataReadFields = new ArrayList<>();

            // read projection to handle record returned by dataRead
            List<DataField> fields = readType.getFields();
            int[] readProjection = new int[fields.size()];

            boolean rowKindAppeared = false;
            for (int i = 0; i < fields.size(); i++) {
                String fieldName = fields.get(i).name();
                if (fieldName.equals(SpecialFields.ROW_KIND.name())) {
                    rowKindAppeared = true;
                    readProjection[i] = -1;
                } else {
                    dataReadFields.add(fields.get(i));
                    // There is no row kind field. Keep it as it is
                    // Row kind field has occurred, and the following fields are offset by 1
                    // position
                    readProjection[i] = rowKindAppeared ? i - 1 : i;
                }
            }

            this.readProjection = readProjection;
            dataRead.withReadType(new RowType(readType.isNullable(), dataReadFields));
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            this.dataRead.withIOManager(ioManager);
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            return dataRead.createReader(split).transform(this::convertRow);
        }

        private InternalRow convertRow(InternalRow data) {
            return new AuditLogRow(readProjection, data);
        }
    }

    /** A {@link ProjectedRow} which returns row kind when mapping index is negative. */
    static class AuditLogRow extends ProjectedRow {

        AuditLogRow(int[] indexMapping, InternalRow row) {
            super(indexMapping);
            replaceRow(row);
        }

        @Override
        public RowKind getRowKind() {
            return RowKind.INSERT;
        }

        @Override
        public void setRowKind(RowKind kind) {
            throw new UnsupportedOperationException(
                    "Set row kind is not supported in AuditLogRowData.");
        }

        @Override
        public boolean isNullAt(int pos) {
            if (indexMapping[pos] < 0) {
                // row kind is always not null
                return false;
            }
            return super.isNullAt(pos);
        }

        @Override
        public BinaryString getString(int pos) {
            if (indexMapping[pos] < 0) {
                return BinaryString.fromString(row.getRowKind().shortString());
            }
            return super.getString(pos);
        }
    }
}
