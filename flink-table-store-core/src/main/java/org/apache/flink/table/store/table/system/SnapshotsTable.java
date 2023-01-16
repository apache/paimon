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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.data.BinaryString;
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.data.Timestamp;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.utils.IteratorRecordReader;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.SerializationUtils;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.TableScan;
import org.apache.flink.table.store.types.BigIntType;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.types.TimestampType;
import org.apache.flink.table.store.utils.ProjectedRow;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterators;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.store.file.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing committing snapshots of table. */
public class SnapshotsTable implements Table {

    private static final long serialVersionUID = 1L;

    public static final String SNAPSHOTS = "snapshots";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "snapshot_id", new BigIntType(false)),
                            new DataField(1, "schema_id", new BigIntType(false)),
                            new DataField(
                                    2, "commit_user", SerializationUtils.newStringType(false)),
                            new DataField(3, "commit_identifier", new BigIntType(false)),
                            new DataField(
                                    4, "commit_kind", SerializationUtils.newStringType(false)),
                            new DataField(5, "commit_time", new TimestampType(false, 3))));

    private final Path location;

    public SnapshotsTable(Path location) {
        this.location = location;
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
    public Path location() {
        return location;
    }

    @Override
    public TableScan newScan() {
        return new SnapshotsScan();
    }

    @Override
    public TableRead newRead() {
        return new SnapshotsRead();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new SnapshotsTable(location);
    }

    private class SnapshotsScan implements TableScan {

        @Override
        public TableScan withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public Plan plan() {
            return () -> Collections.singletonList(new SnapshotsSplit(location));
        }
    }

    private static class SnapshotsSplit implements Split {

        private static final long serialVersionUID = 1L;

        private final Path location;

        private SnapshotsSplit(Path location) {
            this.location = location;
        }

        @Override
        public long rowCount() {
            try {
                return new SnapshotManager(location).snapshotCount();
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

    private static class SnapshotsRead implements TableRead {

        private int[][] projection;

        @Override
        public TableRead withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public TableRead withProjection(int[][] projection) {
            this.projection = projection;
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (!(split instanceof SnapshotsSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            Path location = ((SnapshotsSplit) split).location;
            Iterator<Snapshot> snapshots = new SnapshotManager(location).snapshots();
            Iterator<InternalRow> rows = Iterators.transform(snapshots, this::toRow);
            if (projection != null) {
                rows =
                        Iterators.transform(
                                rows, row -> ProjectedRow.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private InternalRow toRow(Snapshot snapshot) {
            return GenericRow.of(
                    snapshot.id(),
                    snapshot.schemaId(),
                    BinaryString.fromString(snapshot.commitUser()),
                    snapshot.commitIdentifier(),
                    BinaryString.fromString(snapshot.commitKind().toString()),
                    Timestamp.fromLocalDateTime(
                            LocalDateTime.ofInstant(
                                    Instant.ofEpochMilli(snapshot.timeMillis()),
                                    ZoneId.systemDefault())));
        }
    }
}
