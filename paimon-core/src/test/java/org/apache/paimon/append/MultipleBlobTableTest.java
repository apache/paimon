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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.DataEvolutionSplitRead.FieldBunch;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.paimon.append.dataevolution.DataEvolutionCompactTask.TaskType.BLOB;
import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.operation.DataEvolutionSplitRead.splitFieldBunches;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for table with blob. */
public class MultipleBlobTableTest extends TableTestBase {

    private final byte[] blobBytes1 = randomBytes();
    private final byte[] blobBytes2 = randomBytes();

    @Test
    public void testBasic() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(1000, 1));

        assertThat(getTableDefault().snapshotManager().latestSnapshot().totalRecordCount())
                .isEqualTo(3000L);
        assertThat(getTableDefault().snapshotManager().latestSnapshot().deltaRecordCount())
                .isEqualTo(3000L);

        AtomicInteger integer = new AtomicInteger(0);

        FileStoreTable table = getTableDefault();
        List<DataFileMeta> filesMetas =
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());

        RowType rowType = table.schema().logicalRowType();
        List<FieldBunch> fieldGroups = splitFieldBunches(filesMetas, file -> rowType);

        assertThat(fieldGroups.size()).isEqualTo(3);
        assertThat(fieldGroups.get(0).files().size()).isEqualTo(1);
        assertThat(fieldGroups.get(1).files().size()).isEqualTo(10);
        assertThat(fieldGroups.get(2).files().size()).isEqualTo(10);

        readDefault(
                row -> {
                    integer.incrementAndGet();
                    if (integer.get() % 50 == 0) {
                        assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes1);
                        assertThat(row.getBlob(3).toData()).isEqualTo(blobBytes2);
                    }
                });

        assertThat(integer.get()).isEqualTo(1000);
    }

    @Test
    public void testDataEvolutionBlobCompaction() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(50, 20));

        FileStoreTable table = getTableDefault();
        List<DataFileMeta> before = currentDataFiles(table);
        long beforeBlobFileCount =
                before.stream().filter(file -> isBlobFile(file.fileName())).count();
        assertThat(beforeBlobFileCount).isEqualTo(40);

        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(
                        table, true, false, table.latestSnapshot().get());
        List<DataEvolutionCompactTask> tasks = coordinator.plan();
        assertThat(tasks.stream().anyMatch(task -> task.type() == BLOB)).isTrue();

        List<CommitMessage> compactMessages = new ArrayList<>();
        for (DataEvolutionCompactTask task : tasks) {
            compactMessages.add(task.doCompact(table, commitUser));
        }
        commitDefault(compactMessages);

        List<DataFileMeta> after = currentDataFiles(table);
        long afterBlobFileCount =
                after.stream().filter(file -> isBlobFile(file.fileName())).count();
        assertThat(afterBlobFileCount).isLessThan(beforeBlobFileCount);
        coordinator =
                new DataEvolutionCompactCoordinator(
                        table, true, false, table.latestSnapshot().get());
        assertThat(coordinator.plan().stream().anyMatch(task -> task.type() == BLOB)).isFalse();

        AtomicInteger integer = new AtomicInteger(0);
        readDefault(
                row -> {
                    integer.incrementAndGet();
                    if (integer.get() % 50 == 0) {
                        assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes1);
                        assertThat(row.getBlob(3).toData()).isEqualTo(blobBytes2);
                    }
                });
        assertThat(integer.get()).isEqualTo(1000);
    }

    @Test
    public void testDataEvolutionBlobCompactionAfterDropBlobColumns() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(1000, 1));

        catalog.alterTable(
                identifier(),
                Arrays.asList(SchemaChange.dropColumn("f2"), SchemaChange.dropColumn("f3")),
                false);

        FileStoreTable table = getTableDefault();
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(
                        table, true, false, table.latestSnapshot().get());
        List<DataEvolutionCompactTask> tasks = coordinator.plan();
        assertThat(tasks.stream().anyMatch(task -> task.type() == BLOB)).isFalse();

        List<DataFileMeta> after = currentDataFiles(getTableDefault());
        assertThat(after.stream().filter(file -> isBlobFile(file.fileName())).count())
                .isEqualTo(20);

        AtomicInteger integer = new AtomicInteger(0);
        readDefault(
                row -> {
                    integer.incrementAndGet();
                    assertThat(row.getFieldCount()).isEqualTo(2);
                });
        assertThat(integer.get()).isEqualTo(1000);
    }

    private List<DataFileMeta> currentDataFiles(FileStoreTable table) {
        return table.store().newScan().plan().files().stream()
                .map(ManifestEntry::file)
                .collect(Collectors.toList());
    }

    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.column("f3", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "1 GB");
        schemaBuilder.option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        return schemaBuilder.build();
    }

    protected InternalRow dataDefault(int time, int size) {
        return GenericRow.of(
                RANDOM.nextInt(),
                BinaryString.fromBytes(randomBytes()),
                new BlobData(blobBytes1),
                new BlobData(blobBytes2));
    }

    @Test
    public void testStreamScanReadsBlobOnlyDelta() throws Exception {
        createTableDefault();
        RowType rowType = getTableDefault().rowType();

        // normal file writes f0/f1 over row ids [0, 1]
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write =
                        builder.newWrite()
                                .withWriteType(rowType.project(Arrays.asList("f0", "f1")));
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(0, BinaryString.fromString("a")));
            write.write(GenericRow.of(1, BinaryString.fromString("b")));
            commit.commit(write.prepareCommit());
        }

        FileStoreTable table = getTableDefault();
        ReadBuilder readBuilder =
                table.newReadBuilder().withReadType(rowType.project(Arrays.asList("f2", "f3")));
        StreamTableScan streamScan = readBuilder.newStreamScan();
        assertThat(readBlobs(readBuilder, streamScan.plan()).size()).isEqualTo(2);

        // One commit updates both blob columns over [0, 1], so its delta holds only two .blob
        // files; their normal file is in the previous snapshot.
        builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write =
                        builder.newWrite()
                                .withWriteType(rowType.project(Arrays.asList("f2", "f3")));
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(new BlobData(blobBytes1), new BlobData(blobBytes2)));
            write.write(GenericRow.of(new BlobData(blobBytes2), new BlobData(blobBytes1)));
            List<CommitMessage> messages = write.prepareCommit();
            assignFirstRowId(messages, 0L);
            commit.commit(messages);
        }

        TableScan.Plan deltaPlan = streamScan.plan();
        assertThat(
                        deltaPlan.splits().stream()
                                .flatMap(split -> ((DataSplit) split).dataFiles().stream())
                                .allMatch(file -> isBlobFile(file.fileName())))
                .isTrue();
        List<byte[][]> delta = readBlobs(readBuilder, deltaPlan);
        assertThat(delta.size()).isEqualTo(2);
        assertThat(delta.get(0)[0]).isEqualTo(blobBytes1);
        assertThat(delta.get(0)[1]).isEqualTo(blobBytes2);
        assertThat(delta.get(1)[0]).isEqualTo(blobBytes2);
        assertThat(delta.get(1)[1]).isEqualTo(blobBytes1);
    }

    @Test
    public void testProjectTwoBlobColumnsCoveringDifferentSubRanges() throws Exception {
        // normal [0, 9], f2 over [0, 6] and f3 over [3, 9]: together the blob files cover the
        // group, but each blob column covers only part of it.
        createTableDefault();
        RowType rowType = getTableDefault().rowType();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write =
                        builder.newWrite()
                                .withWriteType(rowType.project(Arrays.asList("f0", "f1")));
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < 10; i++) {
                write.write(GenericRow.of(i, BinaryString.fromString("row-" + i)));
            }
            commit.commit(write.prepareCommit());
        }
        writeBlobColumn("f2", blobBytes1, 7, 0L);
        writeBlobColumn("f3", blobBytes2, 7, 3L);

        ReadBuilder readBuilder =
                getTableDefault()
                        .newReadBuilder()
                        .withReadType(rowType.project(Arrays.asList("f2", "f3")));
        List<byte[][]> rows = readBlobs(readBuilder, readBuilder.newScan().plan());
        assertThat(rows.size()).isEqualTo(10);
        for (int i = 0; i < 10; i++) {
            assertThat(rows.get(i)[0]).isEqualTo(i <= 6 ? blobBytes1 : null);
            assertThat(rows.get(i)[1]).isEqualTo(i >= 3 ? blobBytes2 : null);
        }
    }

    private void writeBlobColumn(String column, byte[] bytes, int count, long firstRowId)
            throws Exception {
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write =
                        builder.newWrite()
                                .withWriteType(
                                        table.rowType()
                                                .project(Collections.singletonList(column)));
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < count; i++) {
                write.write(GenericRow.of(new BlobData(bytes)));
            }
            List<CommitMessage> messages = write.prepareCommit();
            assignFirstRowId(messages, firstRowId);
            commit.commit(messages);
        }
    }

    private static List<byte[][]> readBlobs(ReadBuilder readBuilder, TableScan.Plan plan)
            throws Exception {
        List<byte[][]> rows = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(plan)
                .forEachRemaining(
                        row ->
                                rows.add(
                                        new byte[][] {
                                            row.isNullAt(0) ? null : row.getBlob(0).toData(),
                                            row.isNullAt(1) ? null : row.getBlob(1).toData()
                                        }));
        return rows;
    }

    private static void assignFirstRowId(List<CommitMessage> messages, long firstRowId) {
        for (CommitMessage message : messages) {
            List<DataFileMeta> newFiles =
                    ((CommitMessageImpl) message).newFilesIncrement().newFiles();
            List<DataFileMeta> written = new ArrayList<>(newFiles);
            newFiles.clear();
            for (DataFileMeta file : written) {
                newFiles.add(file.assignFirstRowId(firstRowId));
            }
        }
    }

    @Test
    public void testProjectOnlyTwoBlobFields() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(100, 1));

        FileStoreTable table = getTableDefault();

        // Project only blob fields: f2 (index 2) and f3 (index 3)
        List<InternalRow> rows = read(table, new int[] {2, 3});

        assertThat(rows.size()).isEqualTo(100);
        for (InternalRow row : rows) {
            assertThat(row.getFieldCount()).isEqualTo(2);
            assertThat(row.getBlob(0).toData()).isEqualTo(blobBytes1);
            assertThat(row.getBlob(1).toData()).isEqualTo(blobBytes2);
        }
    }

    @Test
    public void testProjectOneBlobAndOneNonBlob() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(100, 1));

        // Project f0 (INT, index 0) + f2 (BLOB, index 2)
        FileStoreTable table = getTableDefault();
        List<InternalRow> rows = read(table, new int[] {0, 2});

        assertThat(rows.size()).isEqualTo(100);
        for (InternalRow row : rows) {
            assertThat(row.getFieldCount()).isEqualTo(2);
            assertThat(row.getBlob(1).toData()).isEqualTo(blobBytes1);
        }
    }

    @Test
    public void testProjectSingleBlobField() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(100, 1));

        // Project only f2 (BLOB, index 2)
        FileStoreTable table = getTableDefault();
        List<InternalRow> rows = read(table, new int[] {2});

        assertThat(rows.size()).isEqualTo(100);
        for (InternalRow row : rows) {
            assertThat(row.getFieldCount()).isEqualTo(1);
            assertThat(row.getBlob(0).toData()).isEqualTo(blobBytes1);
        }
    }

    @Test
    public void testProjectOnlyNonBlobFields() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(100, 1));

        // Project only non-blob fields: f0 (index 0) and f1 (index 1)
        FileStoreTable table = getTableDefault();
        List<InternalRow> rows = read(table, new int[] {0, 1});

        assertThat(rows.size()).isEqualTo(100);
        for (InternalRow row : rows) {
            assertThat(row.getFieldCount()).isEqualTo(2);
            assertThat(row.isNullAt(0)).isFalse();
            assertThat(row.isNullAt(1)).isFalse();
        }
    }

    @Test
    public void testProjectSecondBlobOnly() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(100, 1));

        // Project only f3 (BLOB, index 3) — the second blob field
        FileStoreTable table = getTableDefault();
        List<InternalRow> rows = read(table, new int[] {3});

        assertThat(rows.size()).isEqualTo(100);
        for (InternalRow row : rows) {
            assertThat(row.getFieldCount()).isEqualTo(1);
            assertThat(row.getBlob(0).toData()).isEqualTo(blobBytes2);
        }
    }

    @Test
    public void testMultiBatchThenReadBothBlobs() throws Exception {
        createTableDefault();

        // Write multiple batches
        for (int i = 0; i < 5; i++) {
            commitDefault(writeDataDefault(100, 1));
        }

        AtomicInteger count = new AtomicInteger(0);
        readDefault(
                row -> {
                    count.incrementAndGet();
                    assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes1);
                    assertThat(row.getBlob(3).toData()).isEqualTo(blobBytes2);
                });
        assertThat(count.get()).isEqualTo(500);
    }

    @Test
    public void testDropOneBlobColumnKeepOther() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(100, 1));

        // Drop f2, keep f3
        catalog.alterTable(identifier(), Arrays.asList(SchemaChange.dropColumn("f2")), false);

        FileStoreTable table = getTableDefault();
        AtomicInteger count = new AtomicInteger(0);
        readDefault(
                row -> {
                    count.incrementAndGet();
                    assertThat(row.getFieldCount()).isEqualTo(3);
                    // f0, f1, f3 (f2 dropped)
                    assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes2);
                });
        assertThat(count.get()).isEqualTo(100);
    }

    @Test
    public void testNullBlobValues() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.column("f3", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "1 GB");
        schemaBuilder.option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        catalog.createTable(identifier(), schemaBuilder.build(), true);

        FileStoreTable table = getTableDefault();
        org.apache.paimon.table.sink.BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (org.apache.paimon.table.sink.BatchTableWrite write = builder.newWrite()) {
            // Mix null and non-null blob values
            write.write(
                    GenericRow.of(1, BinaryString.fromString("a"), new BlobData(blobBytes1), null));
            write.write(
                    GenericRow.of(2, BinaryString.fromString("b"), null, new BlobData(blobBytes2)));
            write.write(
                    GenericRow.of(
                            3,
                            BinaryString.fromString("c"),
                            new BlobData(blobBytes1),
                            new BlobData(blobBytes2)));
            org.apache.paimon.table.sink.BatchTableCommit commit = builder.newCommit();
            commit.commit(write.prepareCommit());
        }

        List<InternalRow> rows = read(table, new int[] {0, 2, 3});
        assertThat(rows.size()).isEqualTo(3);

        rows.sort((a, b) -> Integer.compare(a.getInt(0), b.getInt(0)));
        // Row 1: f2=blobBytes1, f3=null
        assertThat(rows.get(0).getBlob(1).toData()).isEqualTo(blobBytes1);
        assertThat(rows.get(0).isNullAt(2)).isTrue();
        // Row 2: f2=null, f3=blobBytes2
        assertThat(rows.get(1).isNullAt(1)).isTrue();
        assertThat(rows.get(1).getBlob(2).toData()).isEqualTo(blobBytes2);
        // Row 3: f2=blobBytes1, f3=blobBytes2
        assertThat(rows.get(2).getBlob(1).toData()).isEqualTo(blobBytes1);
        assertThat(rows.get(2).getBlob(2).toData()).isEqualTo(blobBytes2);
    }

    @Test
    public void testProjectTwoBlobsAfterCompaction() throws Exception {
        createTableDefault();

        // Write enough batches to trigger compaction
        commitDefault(writeDataDefault(50, 20));

        FileStoreTable table = getTableDefault();
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(
                        table, true, false, table.latestSnapshot().get());
        List<DataEvolutionCompactTask> tasks = coordinator.plan();

        List<CommitMessage> compactMessages = new ArrayList<>();
        for (DataEvolutionCompactTask task : tasks) {
            compactMessages.add(task.doCompact(table, commitUser));
        }
        commitDefault(compactMessages);

        // Project only both blob fields after compaction
        table = getTableDefault();
        List<InternalRow> rows = read(table, new int[] {2, 3});

        assertThat(rows.size()).isEqualTo(1000);
        for (InternalRow row : rows) {
            assertThat(row.getFieldCount()).isEqualTo(2);
            assertThat(row.getBlob(0).toData()).isEqualTo(blobBytes1);
            assertThat(row.getBlob(1).toData()).isEqualTo(blobBytes2);
        }
    }

    @Test
    public void testAddBlobColumnThenProjectBothBlobs() throws Exception {
        // Start with table having only one blob column
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "1 GB");
        schemaBuilder.option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        catalog.createTable(identifier(), schemaBuilder.build(), true);

        // Write data with only f2 blob
        FileStoreTable table = getTableDefault();
        org.apache.paimon.table.sink.BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (org.apache.paimon.table.sink.BatchTableWrite write = builder.newWrite()) {
            for (int i = 0; i < 50; i++) {
                write.write(
                        GenericRow.of(
                                i, BinaryString.fromString("row" + i), new BlobData(blobBytes1)));
            }
            org.apache.paimon.table.sink.BatchTableCommit commit = builder.newCommit();
            commit.commit(write.prepareCommit());
        }

        // Add new blob column f3
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.addColumn("f3", DataTypes.BLOB(), "__BLOB_FIELD", null)),
                false);

        // Write more data with both f2 and f3
        table = getTableDefault();
        builder = table.newBatchWriteBuilder();
        try (org.apache.paimon.table.sink.BatchTableWrite write = builder.newWrite()) {
            for (int i = 50; i < 100; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("row" + i),
                                new BlobData(blobBytes1),
                                new BlobData(blobBytes2)));
            }
            org.apache.paimon.table.sink.BatchTableCommit commit = builder.newCommit();
            commit.commit(write.prepareCommit());
        }

        // Project only both blob fields: f2 (index 2) and f3 (index 3)
        table = getTableDefault();
        List<InternalRow> rows = read(table, new int[] {2, 3});

        assertThat(rows.size()).isEqualTo(100);
        // Sort by checking which rows have null f3
        int nullF3Count = 0;
        int nonNullF3Count = 0;
        for (InternalRow row : rows) {
            assertThat(row.getFieldCount()).isEqualTo(2);
            assertThat(row.getBlob(0).toData()).isEqualTo(blobBytes1);
            if (row.isNullAt(1)) {
                nullF3Count++;
            } else {
                assertThat(row.getBlob(1).toData()).isEqualTo(blobBytes2);
                nonNullF3Count++;
            }
        }
        // First 50 rows have null f3 (added before schema evolution)
        assertThat(nullF3Count).isEqualTo(50);
        assertThat(nonNullF3Count).isEqualTo(50);
    }

    @Test
    public void testAsymmetricCompactionThenProjectBothBlobs() throws Exception {
        // Use smaller blob target to produce more blob files per field
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.column("f3", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "1 GB");
        schemaBuilder.option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "3");
        catalog.createTable(identifier(), schemaBuilder.build(), true);

        // Write multiple batches — each batch creates separate blob files
        FileStoreTable table = getTableDefault();
        org.apache.paimon.table.sink.BatchWriteBuilder builder = table.newBatchWriteBuilder();
        for (int batch = 0; batch < 6; batch++) {
            try (org.apache.paimon.table.sink.BatchTableWrite write = builder.newWrite()) {
                for (int i = 0; i < 10; i++) {
                    write.write(
                            GenericRow.of(
                                    batch * 10 + i,
                                    BinaryString.fromString("v" + i),
                                    new BlobData(blobBytes1),
                                    new BlobData(blobBytes2)));
                }
                org.apache.paimon.table.sink.BatchTableCommit commit = builder.newCommit();
                commit.commit(write.prepareCommit());
            }
        }

        table = getTableDefault();

        // Run compaction
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(
                        table, true, false, table.latestSnapshot().get());
        List<DataEvolutionCompactTask> tasks = coordinator.plan();
        if (!tasks.isEmpty()) {
            List<CommitMessage> compactMessages = new ArrayList<>();
            for (DataEvolutionCompactTask task : tasks) {
                compactMessages.add(task.doCompact(table, commitUser));
            }
            commitDefault(compactMessages);
        }

        // Project only both blob fields after compaction
        table = getTableDefault();
        List<InternalRow> rows = read(table, new int[] {2, 3});
        assertThat(rows.size()).isEqualTo(60);
        for (InternalRow row : rows) {
            assertThat(row.getFieldCount()).isEqualTo(2);
            assertThat(row.getBlob(0).toData()).isEqualTo(blobBytes1);
            assertThat(row.getBlob(1).toData()).isEqualTo(blobBytes2);
        }
    }

    @Test
    public void testBlobOnlyProjectionWithRowRanges() throws Exception {
        createTableDefault();

        // Write 1000 rows (will create ~10 blob files per field)
        commitDefault(writeDataDefault(1000, 1));

        FileStoreTable table = getTableDefault();

        // Read with row ranges [100, 199] — only blob projection
        org.apache.paimon.table.source.ReadBuilder readBuilder = table.newReadBuilder();
        readBuilder.withProjection(new int[] {2, 3});
        readBuilder.withRowRanges(Arrays.asList(new org.apache.paimon.utils.Range(100, 199)));
        org.apache.paimon.reader.RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(rows::add);

        assertThat(rows.size()).isEqualTo(100);
        for (InternalRow row : rows) {
            assertThat(row.getFieldCount()).isEqualTo(2);
            assertThat(row.getBlob(0).toData()).isEqualTo(blobBytes1);
            assertThat(row.getBlob(1).toData()).isEqualTo(blobBytes2);
        }
    }

    @Override
    protected byte[] randomBytes() {
        byte[] binary = new byte[2 * 1024 * 124];
        RANDOM.nextBytes(binary);
        return binary;
    }
}
