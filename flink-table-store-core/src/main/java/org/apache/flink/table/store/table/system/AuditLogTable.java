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
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.table.DataTable;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.RowKind;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.types.VarCharType;
import org.apache.flink.table.store.utils.ProjectedRow;

import org.apache.flink.shaded.guava30.com.google.common.primitives.Ints;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.catalog.Catalog.SYSTEM_TABLE_SPLITTER;
import static org.apache.flink.table.store.file.utils.RecordReaderUtils.transform;

/** A {@link Table} for reading audit log of table. */
public class AuditLogTable implements DataTable {

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
    public DataTableScan newScan() {
        return new AuditLogScan(dataTable.newScan());
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
    public TableRead newRead() {
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

    private class AuditLogScan implements DataTableScan {

        private final DataTableScan dataScan;

        private AuditLogScan(DataTableScan dataScan) {
            this.dataScan = dataScan;
        }

        @Override
        public DataTableScan withFilter(Predicate predicate) {
            convert(predicate).ifPresent(dataScan::withFilter);
            return this;
        }

        @Override
        public DataTableScan withKind(ScanKind kind) {
            dataScan.withKind(kind);
            return this;
        }

        @Override
        public DataTableScan withSnapshot(long snapshotId) {
            dataScan.withSnapshot(snapshotId);
            return this;
        }

        @Override
        public DataTableScan withLevel(int level) {
            dataScan.withLevel(level);
            return this;
        }

        @Override
        public DataTableScan.DataFilePlan plan() {
            return dataScan.plan();
        }
    }

    private class AuditLogRead implements TableRead {

        private final TableRead dataRead;

        private int[] readProjection;

        private AuditLogRead(TableRead dataRead) {
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
        public TableRead withFilter(Predicate predicate) {
            convert(predicate).ifPresent(dataRead::withFilter);
            return this;
        }

        @Override
        public TableRead withProjection(int[][] projection) {
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
            return transform(dataRead.createReader(split), this::convertRow);
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
