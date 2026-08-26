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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.SnapshotTest.newSnapshotManager;
import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SnapshotsTable}. */
public class SnapshotsTableTest extends TableTestBase {
    private static final String tableName = "MyTable";

    private SnapshotsTable snapshotsTable;
    private SnapshotManager snapshotManager;

    @BeforeEach
    public void before() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, tableName));
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .option(CoreOptions.BUCKET.key(), "2")
                        .build();
        snapshotManager = newSnapshotManager(fileIO, tablePath);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(new SchemaManager(fileIO, tablePath), schema);
        FileStoreTable table =
                FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);

        Identifier filesTableId =
                identifier(tableName + SYSTEM_TABLE_SPLITTER + SnapshotsTable.SNAPSHOTS);
        snapshotsTable = (SnapshotsTable) catalog.getTable(filesTableId);

        // snapshot 1: append
        write(table, GenericRow.of(1, 1, 1), GenericRow.of(1, 2, 5));

        // snapshot 2: append
        write(table, GenericRow.of(2, 1, 3), GenericRow.of(2, 2, 4));
    }

    @Test
    public void testReadSnapshotsFromLatest() throws Exception {
        List<InternalRow> expectedRow = getExpectedResult(new long[] {1, 2});
        List<InternalRow> result = read(snapshotsTable);
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectedRow);
    }

    @Test
    public void testReadSnapshotsWithInFilterContainingUnknownId() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(snapshotsTable.rowType());
        Predicate predicate =
                builder.in(
                        snapshotsTable.rowType().getFieldNames().indexOf("snapshot_id"),
                        Arrays.asList(1L, 99L));

        ReadBuilder readBuilder = snapshotsTable.newReadBuilder().withFilter(predicate);
        List<InternalRow> result = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(snapshotsTable.rowType());
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(row -> result.add(serializer.copy(row)));

        assertThat(result).containsExactlyInAnyOrderElementsOf(getExpectedResult(new long[] {1}));
    }

    @Test
    public void testReadSnapshotsWithMultipleInFilters() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(snapshotsTable.rowType());
        Predicate predicate =
                PredicateBuilder.and(
                        builder.in(0, Arrays.asList(1L, 2L)), builder.in(0, Arrays.asList(2L, 3L)));

        ReadBuilder readBuilder = snapshotsTable.newReadBuilder().withFilter(predicate);
        List<InternalRow> result = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(snapshotsTable.rowType());
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(row -> result.add(serializer.copy(row)));

        assertThat(result).extracting(row -> row.getLong(0)).containsExactly(2L);
    }

    @Test
    public void testReadSnapshotsWithInAndRangeFilter() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(snapshotsTable.rowType());
        List<Predicate> predicates =
                Arrays.asList(
                        PredicateBuilder.and(
                                builder.in(0, Arrays.asList(1L, 2L)),
                                builder.greaterOrEqual(0, 2L)),
                        PredicateBuilder.and(
                                builder.in(0, Collections.singletonList(1L)),
                                builder.greaterOrEqual(0, 2L)));
        for (int i = 0; i < predicates.size(); i++) {
            Predicate predicate = predicates.get(i);
            ReadBuilder readBuilder = snapshotsTable.newReadBuilder().withFilter(predicate);
            List<InternalRow> result = new ArrayList<>();
            InternalRowSerializer serializer = new InternalRowSerializer(snapshotsTable.rowType());
            readBuilder
                    .newRead()
                    .createReader(readBuilder.newScan().plan())
                    .forEachRemaining(row -> result.add(serializer.copy(row)));

            if (i == 0) {
                assertThat(result).extracting(row -> row.getLong(0)).containsExactly(2L);
            } else {
                assertThat(result).isEmpty();
            }
        }
    }

    @Test
    public void testReadSnapshotsWithEqualFilterOnUnknownId() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(snapshotsTable.rowType());
        Predicate predicate =
                builder.equal(snapshotsTable.rowType().getFieldNames().indexOf("snapshot_id"), 99L);

        ReadBuilder readBuilder = snapshotsTable.newReadBuilder().withFilter(predicate);
        List<InternalRow> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(result::add);

        assertThat(result).isEmpty();
    }

    @Test
    public void testReadSnapshotsWithNestedAndFilter() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(snapshotsTable.rowType());
        Predicate predicate =
                PredicateBuilder.and(
                        builder.greaterOrEqual(0, 0L),
                        builder.equal(0, 99L),
                        builder.lessThan(0, 3L));

        ReadBuilder readBuilder = snapshotsTable.newReadBuilder().withFilter(predicate);
        List<InternalRow> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(result::add);

        assertThat(result).isEmpty();
    }

    @Test
    public void testReadSnapshotsWithExclusiveBoundOverflow() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(snapshotsTable.rowType());
        for (Predicate predicate :
                Arrays.asList(
                        builder.greaterThan(0, Long.MAX_VALUE),
                        builder.lessThan(0, Long.MIN_VALUE))) {
            ReadBuilder readBuilder = snapshotsTable.newReadBuilder().withFilter(predicate);
            List<InternalRow> result = new ArrayList<>();
            readBuilder
                    .newRead()
                    .createReader(readBuilder.newScan().plan())
                    .forEachRemaining(result::add);

            assertThat(result).isEmpty();
        }
    }

    @Test
    public void testFilterBySnapshotIdEqualAndGreaterOrEqual() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(snapshotsTable.rowType());
        Predicate predicate =
                PredicateBuilder.and(builder.equal(0, 2L), builder.greaterOrEqual(0, 0L));
        ReadBuilder readBuilder = snapshotsTable.newReadBuilder().withFilter(predicate);
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        List<InternalRow> result = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(snapshotsTable.rowType());
        reader.forEachRemaining(row -> result.add(serializer.copy(row)));

        assertThat(result).extracting(row -> row.getLong(0)).containsExactly(2L);
    }

    @Test
    public void testFilterBySnapshotIdEqualAndLessOrEqual() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(snapshotsTable.rowType());
        for (Predicate predicate :
                Arrays.asList(
                        PredicateBuilder.and(builder.equal(0, 1L), builder.lessOrEqual(0, 2L)),
                        PredicateBuilder.and(builder.lessOrEqual(0, 2L), builder.equal(0, 1L)))) {
            ReadBuilder readBuilder = snapshotsTable.newReadBuilder().withFilter(predicate);
            RecordReader<InternalRow> reader =
                    readBuilder.newRead().createReader(readBuilder.newScan().plan());
            List<InternalRow> result = new ArrayList<>();
            InternalRowSerializer serializer = new InternalRowSerializer(snapshotsTable.rowType());
            reader.forEachRemaining(row -> result.add(serializer.copy(row)));

            assertThat(result).extracting(row -> row.getLong(0)).containsExactly(1L);
        }
    }

    @Test
    public void testFilterBySnapshotIdWithEmptyRange() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(snapshotsTable.rowType());
        for (Predicate predicate :
                Arrays.asList(
                        PredicateBuilder.and(builder.equal(0, 1L), builder.greaterOrEqual(0, 2L)),
                        PredicateBuilder.and(
                                builder.greaterOrEqual(0, 2L), builder.equal(0, 1L)))) {
            ReadBuilder readBuilder = snapshotsTable.newReadBuilder().withFilter(predicate);
            RecordReader<InternalRow> reader =
                    readBuilder.newRead().createReader(readBuilder.newScan().plan());
            List<InternalRow> result = new ArrayList<>();
            reader.forEachRemaining(result::add);

            assertThat(result).isEmpty();
        }
    }

    private List<InternalRow> getExpectedResult(long[] snapshotIds) {
        List<InternalRow> expectedRow = new ArrayList<>();
        for (long snapshotId : snapshotIds) {
            Snapshot snapshot = snapshotManager.snapshot(snapshotId);
            expectedRow.add(
                    GenericRow.of(
                            snapshotId,
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
                            snapshot.watermark(),
                            snapshot.nextRowId(),
                            snapshot.operation() == null
                                    ? null
                                    : BinaryString.fromString(snapshot.operation().toString()),
                            BinaryString.fromString(snapshot.writerVersion())));
        }

        return expectedRow;
    }
}
