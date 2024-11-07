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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LeafPredicateExtractor;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateUtils;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.SingletonSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing committing snapshots of table. */
public class SnapshotsTable implements ReadonlyTable {

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
                            new DataField(5, "commit_time", new TimestampType(false, 3)),
                            new DataField(
                                    6,
                                    "base_manifest_list",
                                    SerializationUtils.newStringType(false)),
                            new DataField(
                                    7,
                                    "delta_manifest_list",
                                    SerializationUtils.newStringType(false)),
                            new DataField(
                                    8,
                                    "changelog_manifest_list",
                                    SerializationUtils.newStringType(true)),
                            new DataField(9, "total_record_count", new BigIntType(true)),
                            new DataField(10, "delta_record_count", new BigIntType(true)),
                            new DataField(11, "changelog_record_count", new BigIntType(true)),
                            new DataField(12, "watermark", new BigIntType(true))));

    private final FileIO fileIO;
    private final Path location;
    private final String branch;

    private final FileStoreTable dataTable;

    public SnapshotsTable(FileStoreTable dataTable) {
        this(
                dataTable.fileIO(),
                dataTable.location(),
                dataTable,
                CoreOptions.branch(dataTable.schema().options()));
    }

    public SnapshotsTable(
            FileIO fileIO, Path location, FileStoreTable dataTable, String branchName) {
        this.fileIO = fileIO;
        this.location = location;
        this.dataTable = dataTable;
        this.branch = branchName;
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
        return new SnapshotsRead(fileIO);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new SnapshotsTable(fileIO, location, dataTable.copy(dynamicOptions), branch);
    }

    private class SnapshotsScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            // do filter in read
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new SnapshotsSplit(location));
        }
    }

    private static class SnapshotsSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private final Path location;

        private SnapshotsSplit(Path location) {
            this.location = location;
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

    private class SnapshotsRead implements InnerTableRead {

        private final FileIO fileIO;
        private RowType readType;
        private Optional<Long> optionalFilterSnapshotIdMax = Optional.empty();
        private Optional<Long> optionalFilterSnapshotIdMin = Optional.empty();
        private List<Long> snapshotIds = new ArrayList<>();

        public SnapshotsRead(FileIO fileIO) {
            this.fileIO = fileIO;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            if (predicate == null) {
                return this;
            }

            String leafName = "snapshot_id";
            if (predicate instanceof CompoundPredicate) {
                CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
                if ((compoundPredicate.function()) instanceof And) {
                    PredicateUtils.traverseCompoundPredicate(
                            predicate,
                            leafName,
                            (Predicate p) -> handleLeafPredicate(p, leafName),
                            null);
                }

                // optimize for IN filter
                if ((compoundPredicate.function()) instanceof Or) {
                    PredicateUtils.traverseCompoundPredicate(
                            predicate,
                            leafName,
                            (Predicate p) -> {
                                if (snapshotIds != null) {
                                    snapshotIds.add((Long) ((LeafPredicate) p).literals().get(0));
                                }
                            },
                            (Predicate p) -> snapshotIds = null);
                }
            } else {
                handleLeafPredicate(predicate, leafName);
            }

            return this;
        }

        public void handleLeafPredicate(Predicate predicate, String leafName) {
            LeafPredicate snapshotPred =
                    predicate.visit(LeafPredicateExtractor.INSTANCE).get(leafName);
            if (snapshotPred != null) {
                if (snapshotPred.function() instanceof Equal) {
                    optionalFilterSnapshotIdMin =
                            Optional.of((Long) snapshotPred.literals().get(0));
                    optionalFilterSnapshotIdMax =
                            Optional.of((Long) snapshotPred.literals().get(0));
                }

                if (snapshotPred.function() instanceof GreaterThan) {
                    optionalFilterSnapshotIdMin =
                            Optional.of((Long) snapshotPred.literals().get(0) + 1);
                }

                if (snapshotPred.function() instanceof GreaterOrEqual) {
                    optionalFilterSnapshotIdMin =
                            Optional.of((Long) snapshotPred.literals().get(0));
                }

                if (snapshotPred.function() instanceof LessThan) {
                    optionalFilterSnapshotIdMax =
                            Optional.of((Long) snapshotPred.literals().get(0) - 1);
                }

                if (snapshotPred.function() instanceof LessOrEqual) {
                    optionalFilterSnapshotIdMax =
                            Optional.of((Long) snapshotPred.literals().get(0));
                }
            }
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            this.readType = readType;
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
            SnapshotManager snapshotManager =
                    new SnapshotManager(fileIO, ((SnapshotsSplit) split).location, branch);

            Iterator<Snapshot> snapshots;
            if (snapshotIds != null && !snapshotIds.isEmpty()) {
                snapshots = snapshotManager.snapshotsWithId(snapshotIds);
            } else {
                snapshots =
                        snapshotManager.snapshotsWithinRange(
                                optionalFilterSnapshotIdMax, optionalFilterSnapshotIdMin);
            }

            Iterator<InternalRow> rows = Iterators.transform(snapshots, this::toRow);
            if (readType != null) {
                rows =
                        Iterators.transform(
                                rows,
                                row ->
                                        ProjectedRow.from(readType, SnapshotsTable.TABLE_TYPE)
                                                .replaceRow(row));
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
                                    ZoneId.systemDefault())),
                    BinaryString.fromString(snapshot.baseManifestList()),
                    BinaryString.fromString(snapshot.deltaManifestList()),
                    BinaryString.fromString(snapshot.changelogManifestList()),
                    snapshot.totalRecordCount(),
                    snapshot.deltaRecordCount(),
                    snapshot.changelogRecordCount(),
                    snapshot.watermark());
        }
    }
}
