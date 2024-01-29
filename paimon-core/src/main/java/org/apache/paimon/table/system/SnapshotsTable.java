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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing committing snapshots of table. */
public class SnapshotsTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String SNAPSHOTS = "snapshots";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "snapshot_id", DataTypes.BIGINT().notNull()),
                            new DataField(1, "schema_id", DataTypes.BIGINT().notNull()),
                            new DataField(2, "commit_user", DataTypes.STRING().notNull()),
                            new DataField(3, "commit_identifier", DataTypes.BIGINT().notNull()),
                            new DataField(4, "commit_kind", DataTypes.STRING().notNull()),
                            new DataField(5, "commit_time", new TimestampType(false, 3)),
                            new DataField(6, "base_manifest_list", DataTypes.STRING().notNull()),
                            new DataField(7, "delta_manifest_list", DataTypes.STRING().notNull()),
                            new DataField(8, "changelog_manifest_list", DataTypes.STRING()),
                            new DataField(9, "total_record_count", new BigIntType(true)),
                            new DataField(10, "delta_record_count", new BigIntType(true)),
                            new DataField(11, "changelog_record_count", new BigIntType(true)),
                            new DataField(12, "added_file_count", new IntType(true)),
                            new DataField(13, "delete_file_count", new IntType(true)),
                            new DataField(14, "watermark", new BigIntType(true))));

    private final FileIO fileIO;
    private final Path location;

    private final FileStoreTable dataTable;

    public SnapshotsTable(FileIO fileIO, Path location, FileStoreTable dataTable) {
        this.fileIO = fileIO;
        this.location = location;
        this.dataTable = dataTable;
    }

    @Override
    public String name() {
        return location.getName() + SYSTEM_TABLE_SPLITTER + SNAPSHOTS;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("snapshot_id");
    }

    @Override
    public InnerTableScan newScan() {
        return new SnapshotsScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new SnapshotsRead(fileIO, dataTable);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new SnapshotsTable(fileIO, location, dataTable.copy(dynamicOptions));
    }

    private class SnapshotsScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new SnapshotsSplit(fileIO, location));
        }
    }

    private static class SnapshotsSplit implements Split {

        private static final long serialVersionUID = 1L;

        private final FileIO fileIO;
        private final Path location;

        private SnapshotsSplit(FileIO fileIO, Path location) {
            this.fileIO = fileIO;
            this.location = location;
        }

        @Override
        public long rowCount() {
            try {
                return new SnapshotManager(fileIO, location).snapshotCount();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SnapshotsSplit that = (SnapshotsSplit) o;
            return Objects.equals(location, that.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location);
        }
    }

    private static class SnapshotsRead implements InnerTableRead {

        private final FileIO fileIO;
        private int[][] projection;

        private final FileStoreTable dataTable;

        public SnapshotsRead(FileIO fileIO, FileStoreTable dataTable) {
            this.fileIO = fileIO;
            this.dataTable = dataTable;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public InnerTableRead withProjection(int[][] projection) {
            this.projection = projection;
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (!(split instanceof SnapshotsSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            Path location = ((SnapshotsSplit) split).location;
            Iterator<Snapshot> snapshots = new SnapshotManager(fileIO, location).snapshots();
            Iterator<InternalRow> rows =
                    Iterators.transform(snapshots, snapshot -> toRow(snapshot, dataTable));
            if (projection != null) {
                rows =
                        Iterators.transform(
                                rows, row -> ProjectedRow.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private InternalRow toRow(Snapshot snapshot, FileStoreTable dataTable) {
            FileStoreScan.Plan plan = dataTable.store().newScan().withSnapshot(snapshot).plan();
            return GenericRow.of(
                    snapshot.id(),
                    snapshot.schemaId(),
                    BinaryString.fromString(snapshot.commitUser()),
                    snapshot.commitIdentifier(),
                    BinaryString.fromString(snapshot.commitKind().toString()),
                    Timestamp.fromLocalDateTime(
                            LocalDateTime.ofInstant(
                                    Instant.ofEpochMilli(snapshot.timeMillis()),
                                    ZoneId.systemDefault())),
                    BinaryString.fromString(snapshot.baseManifestList()),
                    BinaryString.fromString(snapshot.deltaManifestList()),
                    BinaryString.fromString(snapshot.changelogManifestList()),
                    snapshot.totalRecordCount(),
                    snapshot.deltaRecordCount(),
                    snapshot.changelogRecordCount(),
                    plan.files(FileKind.ADD).size(),
                    plan.files(FileKind.DELETE).size(),
                    snapshot.watermark());
        }
    }
}
