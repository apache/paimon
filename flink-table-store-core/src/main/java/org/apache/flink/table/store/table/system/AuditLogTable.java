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

package org.apache.flink.table.store.table.system;

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.data.BinaryString;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.operation.ScanKind;
import org.apache.flink.table.store.file.predicate.LeafPredicate;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.predicate.PredicateReplaceVisitor;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.reader.RecordReader;
import org.apache.flink.table.store.table.DataTable;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.ReadonlyTable;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.source.BatchDataTableScan;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.table.source.InnerTableRead;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.StreamDataTableScan;
import org.apache.flink.table.store.table.source.snapshot.FollowUpScanner;
import org.apache.flink.table.store.table.source.snapshot.SnapshotSplitReader;
import org.apache.flink.table.store.table.source.snapshot.StartingScanner;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.RowKind;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.types.VarCharType;
import org.apache.flink.table.store.utils.Filter;
import org.apache.flink.table.store.utils.ProjectedRow;

import org.apache.flink.shaded.guava30.com.google.common.primitives.Ints;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for reading audit log of table. */
public class AuditLogTable implements DataTable, ReadonlyTable {

    public static final String AUDIT_LOG = "audit_log";

    public static final String ROW_KIND = "rowkind";

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

    private final FileStoreTable dataTable;

    public AuditLogTable(FileStoreTable dataTable) {
        this.dataTable = dataTable;
    }

    @Override
    public String name() {
        return dataTable.name() + SYSTEM_TABLE_SPLITTER + AUDIT_LOG;
    }

    @Override
    public RowType rowType() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, ROW_KIND, new VarCharType(VarCharType.MAX_LENGTH)));
        fields.addAll(dataTable.rowType().getFields());
        return new RowType(fields);
    }

    @Override
    public SnapshotSplitReader newSnapshotSplitReader() {
        return new AuditLogDataSplitReader(dataTable.newSnapshotSplitReader());
    }

    @Override
    public BatchDataTableScan newScan() {
        return new AuditLogBatchScan(dataTable.newScan());
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        return new AuditLogStreamScan(dataTable.newStreamScan());
    }

    @Override
    public CoreOptions options() {
        return dataTable.options();
    }

    @Override
    public Path location() {
        return dataTable.location();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return dataTable.snapshotManager();
    }

    @Override
    public InnerTableRead newRead() {
        return new AuditLogRead(dataTable.newRead());
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new AuditLogTable(dataTable.copy(dynamicOptions));
    }

    @Override
    public FileIO fileIO() {
        return dataTable.fileIO();
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

    private class AuditLogDataSplitReader implements SnapshotSplitReader {

        private final SnapshotSplitReader snapshotSplitReader;

        private AuditLogDataSplitReader(SnapshotSplitReader snapshotSplitReader) {
            this.snapshotSplitReader = snapshotSplitReader;
        }

        public SnapshotSplitReader withSnapshot(long snapshotId) {
            snapshotSplitReader.withSnapshot(snapshotId);
            return this;
        }

        public SnapshotSplitReader withFilter(Predicate predicate) {
            convert(predicate).ifPresent(snapshotSplitReader::withFilter);
            return this;
        }

        public SnapshotSplitReader withKind(ScanKind scanKind) {
            snapshotSplitReader.withKind(scanKind);
            return this;
        }

        public SnapshotSplitReader withLevelFilter(Filter<Integer> levelFilter) {
            snapshotSplitReader.withLevelFilter(levelFilter);
            return this;
        }

        public SnapshotSplitReader withBucket(int bucket) {
            snapshotSplitReader.withBucket(bucket);
            return this;
        }

        public List<DataSplit> splits() {
            return snapshotSplitReader.splits();
        }

        public List<DataSplit> overwriteSplits() {
            return snapshotSplitReader.overwriteSplits();
        }
    }

    private class AuditLogBatchScan implements BatchDataTableScan {

        private final BatchDataTableScan batchScan;

        private AuditLogBatchScan(BatchDataTableScan batchScan) {
            this.batchScan = batchScan;
        }

        @Override
        public DataTableScan withFilter(Predicate predicate) {
            convert(predicate).ifPresent(batchScan::withFilter);
            return this;
        }

        @Override
        public DataTableScan withKind(ScanKind kind) {
            batchScan.withKind(kind);
            return this;
        }

        @Override
        public DataTableScan withSnapshot(long snapshotId) {
            batchScan.withSnapshot(snapshotId);
            return this;
        }

        @Override
        public DataTableScan withLevelFilter(Filter<Integer> levelFilter) {
            batchScan.withLevelFilter(levelFilter);
            return this;
        }

        @Override
        public DataTableScan.DataFilePlan plan() {
            return batchScan.plan();
        }

        @Override
        public BatchDataTableScan withStartingScanner(StartingScanner startingScanner) {
            return batchScan.withStartingScanner(startingScanner);
        }
    }

    private class AuditLogStreamScan implements StreamDataTableScan {

        private final StreamDataTableScan streamScan;

        private AuditLogStreamScan(StreamDataTableScan streamScan) {
            this.streamScan = streamScan;
        }

        @Override
        public DataTableScan withFilter(Predicate predicate) {
            convert(predicate).ifPresent(streamScan::withFilter);
            return this;
        }

        @Override
        public DataTableScan withKind(ScanKind kind) {
            streamScan.withKind(kind);
            return this;
        }

        @Override
        public DataTableScan withSnapshot(long snapshotId) {
            streamScan.withSnapshot(snapshotId);
            return this;
        }

        @Override
        public DataTableScan withLevelFilter(Filter<Integer> levelFilter) {
            streamScan.withLevelFilter(levelFilter);
            return this;
        }

        @Override
        public DataTableScan.DataFilePlan plan() {
            return streamScan.plan();
        }

        @Override
        public boolean supportStreamingReadOverwrite() {
            return streamScan.supportStreamingReadOverwrite();
        }

        @Override
        public StreamDataTableScan withStartingScanner(StartingScanner startingScanner) {
            return streamScan.withStartingScanner(startingScanner);
        }

        @Override
        public StreamDataTableScan withFollowUpScanner(FollowUpScanner followUpScanner) {
            return streamScan.withFollowUpScanner(followUpScanner);
        }

        @Override
        public StreamDataTableScan withSnapshotStarting() {
            return streamScan.withSnapshotStarting();
        }

        @Nullable
        @Override
        public Long checkpoint() {
            return streamScan.checkpoint();
        }

        @Override
        public void restore(@Nullable Long nextSnapshotId) {
            streamScan.restore(nextSnapshotId);
        }
    }

    private class AuditLogRead implements InnerTableRead {

        private final InnerTableRead dataRead;

        private int[] readProjection;

        private AuditLogRead(InnerTableRead dataRead) {
            this.dataRead = dataRead;
            this.readProjection = defaultProjection();
        }

        /** Default projection, just add row kind to the first. */
        private int[] defaultProjection() {
            int dataFieldCount = dataTable.rowType().getFieldCount();
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
        public InnerTableRead withProjection(int[][] projection) {
            // data projection to push down to dataRead
            List<int[]> dataProjection = new ArrayList<>();
            // read projection to handle record returned by dataRead
            List<Integer> readProjection = new ArrayList<>();
            boolean rowKindAppeared = false;
            for (int i = 0; i < projection.length; i++) {
                int[] field = projection[i];
                int topField = field[0];
                if (topField == 0) {
                    rowKindAppeared = true;
                    readProjection.add(-1);
                } else {
                    int[] newField = Arrays.copyOf(field, field.length);
                    newField[0] = newField[0] - 1;
                    dataProjection.add(newField);

                    // There is no row kind field. Keep it as it is
                    // Row kind field has occurred, and the following fields are offset by 1
                    // position
                    readProjection.add(rowKindAppeared ? i - 1 : i);
                }
            }
            this.readProjection = Ints.toArray(readProjection);
            dataRead.withProjection(dataProjection.toArray(new int[0][]));
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
    private static class AuditLogRow extends ProjectedRow {

        private AuditLogRow(int[] indexMapping, InternalRow row) {
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
