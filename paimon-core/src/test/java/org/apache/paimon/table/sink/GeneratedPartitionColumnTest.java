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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Storage and schema contracts for generated columns. */
class GeneratedPartitionColumnTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @ValueSource(strings = {"append", "primary-key", "data-evolution"})
    void testGenerateRouteCommitAndRead(String mode) throws Exception {
        FileStoreTable table = createTable(mode);
        RowType input = RowType.of(table.rowType().getField("ts"), table.rowType().getField("id"));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.withWriteType(input);
            GenericRow first = GenericRow.of(Timestamp.fromEpochMillis(-1), 1);
            assertThat(write.getPartition(first).getInt(0)).isEqualTo(-1);
            write.write(first);
            write.write(GenericRow.of(Timestamp.fromEpochMillis(0), 2));
            commit.commit(write.prepareCommit());
        }
        List<Split> splits = table.newScan().plan().splits();
        List<Integer> partitions = new ArrayList<>();
        for (Split split : splits) {
            partitions.add(((DataSplit) split).partition().getInt(0));
        }
        assertThat(partitions).containsExactlyInAnyOrder(-1, 0);
        List<String> rows = new ArrayList<>();
        try (RecordReaderIterator<InternalRow> reader =
                new RecordReaderIterator<>(table.newRead().createReader(table.newScan().plan()))) {
            while (reader.hasNext()) {
                InternalRow row = reader.next();
                rows.add(row.getInt(0) + ":" + row.getInt(1));
            }
        }
        assertThat(rows).containsExactlyInAnyOrder("1:-1", "2:0");
        // A reader and writer reopened from persistent schema retain the contract.
        FileStoreTable reopened =
                FileStoreTableFactory.create(LocalFileIO.create(), table.location());
        BatchWriteBuilder explicit = reopened.newBatchWriteBuilder();
        try (BatchTableWrite write = explicit.newWrite().withWriteType(reopened.rowType());
                BatchTableCommit commit = explicit.newCommit()) {
            GenericRow supplied = GenericRow.of(3, 1, Timestamp.fromEpochMillis(0));
            assertThat(write.getPartition(supplied).getInt(0)).isEqualTo(1);
            write.write(supplied);
            assertThatThrownBy(
                            () -> write.write(GenericRow.of(3, null, Timestamp.fromEpochMillis(0))))
                    .hasMessageContaining("Cannot write null to non-null column(dt)");
            commit.commit(write.prepareCommit());
        }
        rows.clear();
        try (RecordReaderIterator<InternalRow> reader =
                new RecordReaderIterator<>(
                        reopened.newRead().createReader(reopened.newScan().plan()))) {
            while (reader.hasNext()) {
                InternalRow row = reader.next();
                rows.add(row.getInt(0) + ":" + row.getInt(1));
            }
        }
        assertThat(rows).containsExactlyInAnyOrder("1:-1", "2:0", "3:1");
    }

    @Test
    void testSchemaEvolutionProtectsDependencies() throws Exception {
        FileStoreTable table = createTable(false);
        FileSystemSchemaManager manager =
                new FileSystemSchemaManager(LocalFileIO.create(), table.location());
        TableSchema changed =
                manager.commitChanges(
                        SchemaChange.updateColumnComment(new String[] {"dt"}, "generated date"));
        assertThat(changed.logicalRowType().getField("dt").generationExpression())
                .isEqualTo("CAST(ts AS DATE)");
        assertThatThrownBy(() -> manager.commitChanges(SchemaChange.renameColumn("ts", "time")))
                .hasMessageContaining("referenced by generated column");
        assertThatThrownBy(() -> manager.commitChanges(SchemaChange.dropColumn("ts")))
                .hasMessageContaining("referenced by generated column");
        assertThatThrownBy(
                        () ->
                                manager.commitChanges(
                                        SchemaChange.updateColumnType(
                                                "ts", DataTypes.TIMESTAMP(6))))
                .hasMessageContaining("referenced by generated column");
        assertThatThrownBy(
                        () ->
                                manager.commitChanges(
                                        SchemaChange.updateColumnDefaultValue(
                                                new String[] {"dt"}, "'2026-10-30'")))
                .hasMessageContaining("cannot have a default value");
    }

    @ParameterizedTest
    @ValueSource(strings = {"partial-update", "aggregation", "first-row"})
    void testGeneratedValuesFollowMergeEngine(String engine) throws Exception {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.INT())
                        .generatedColumn("copy", DataTypes.BIGINT(), "CAST(value AS BIGINT)")
                        .primaryKey("id")
                        .option("bucket", "1")
                        .option("merge-engine", engine);
        if (engine.equals("aggregation")) {
            schema.option("fields.value.aggregate-function", "sum")
                    .option("fields.copy.aggregate-function", "sum");
        }
        Path path = new Path(tempDir.resolve(UUID.randomUUID().toString()).toUri());
        new FileSystemSchemaManager(LocalFileIO.create(), path).createTable(schema.build());
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), path);
        for (int step = 0; step < 3; step++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (IOManagerImpl ioManager = new IOManagerImpl(tempDir.toString());
                    BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                write.withIOManager(ioManager);
                if (step == 0) {
                    write.withWriteType(table.rowType().project("id", "value"));
                    write.write(GenericRow.of(1, 10));
                } else {
                    write.write(
                            step == 1 ? GenericRow.of(1, 20, null) : GenericRow.of(1, null, 99L));
                }
                commit.commit(write.prepareCommit());
            }
        }
        try (RecordReaderIterator<InternalRow> rows =
                new RecordReaderIterator<>(table.newRead().createReader(table.newScan().plan()))) {
            InternalRow row = rows.next();
            assertThat(row.getInt(1))
                    .isEqualTo(
                            engine.equals("aggregation")
                                    ? 30
                                    : engine.equals("first-row") ? 10 : 20);
            assertThat(row.getLong(2))
                    .isEqualTo(
                            engine.equals("aggregation")
                                    ? 109L
                                    : engine.equals("first-row") ? 10L : 99L);
            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testDataEvolutionPartialWriteKeepsUnselectedGeneratedValues() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .generatedColumn("label", DataTypes.STRING(), "UPPER(name)")
                        .option("row-tracking.enabled", "true")
                        .option("data-evolution.enabled", "true")
                        .build();
        Path path = new Path(tempDir.resolve(UUID.randomUUID().toString()).toUri());
        new FileSystemSchemaManager(LocalFileIO.create(), path).createTable(schema);
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), path);
        String[] expectedNames = {"old", "new", "new"};
        String[] expectedLabels = {"OLD", "OLD", "external"};
        for (int step = 0; step < 3; step++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                // Switching from a partial layout must restore omitted-column generation.
                if (step == 0) {
                    write.withWriteType(table.rowType().project("label"));
                }
                write.withWriteType(
                        table.rowType()
                                .project(
                                        step == 0
                                                ? new String[] {"id", "name"}
                                                : step == 1
                                                        ? new String[] {"name"}
                                                        : new String[] {"label"}));
                write.write(
                        step == 0
                                ? GenericRow.of(1, BinaryString.fromString("old"))
                                : GenericRow.of(
                                        BinaryString.fromString(step == 1 ? "new" : "external")));
                List<CommitMessage> messages = write.prepareCommit();
                if (step > 0) {
                    for (CommitMessage message : messages) {
                        List<DataFileMeta> files =
                                ((CommitMessageImpl) message).newFilesIncrement().newFiles();
                        List<DataFileMeta> originals = new ArrayList<>(files);
                        files.clear();
                        for (DataFileMeta file : originals) {
                            files.add(file.assignFirstRowId(0));
                        }
                    }
                }
                commit.commit(messages);
            }
            try (RecordReaderIterator<InternalRow> rows =
                    new RecordReaderIterator<>(
                            table.newRead().createReader(table.newScan().plan()))) {
                InternalRow row = rows.next();
                assertThat(row.getInt(0)).isEqualTo(1);
                assertThat(row.getString(1).toString()).isEqualTo(expectedNames[step]);
                assertThat(row.getString(2).toString()).isEqualTo(expectedLabels[step]);
                assertThat(rows.hasNext()).isFalse();
            }
        }
    }

    @Test
    void testRejectInvalidDefinitions() {
        Schema plain =
                Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .generatedColumn("dt", DataTypes.INT(), "CAST(ts AS DATE)")
                        .build();
        assertThatThrownBy(() -> validate(plain)).hasMessageContaining("does not match");
        Schema defaults =
                Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .column("dt", DataTypes.DATE(), null, "'2026-10-30'", "CAST(ts AS DATE)")
                        .partitionKeys("dt")
                        .build();
        assertThatThrownBy(() -> validate(defaults))
                .hasMessageContaining("cannot have a default value");
        Schema nested =
                Schema.newBuilder()
                        .column(
                                "nested",
                                DataTypes.ROW(
                                        new DataField(0, "dt", DataTypes.DATE())
                                                .newGenerationExpression("CAST(ts AS DATE)")))
                        .build();
        assertThatThrownBy(() -> validate(nested)).hasMessageContaining("top-level columns");
    }

    @ParameterizedTest
    @ValueSource(strings = {"append", "primary-key", "data-evolution"})
    void testNonPartitionTransformsAndAllDependencies(String mode) throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .column("name", DataTypes.STRING())
                        .column("days", DataTypes.INT())
                        .generatedColumn("dt", DataTypes.DATE(), "DATE_ADD(CAST(ts AS DATE), days)")
                        .generatedColumn(
                                "label", DataTypes.STRING(), "CONCAT(UPPER(name), '_', name)")
                        .generatedColumn("year", DataTypes.INT(), "YEAR(ts)")
                        .partitionKeys("dt");
        if (mode.equals("primary-key")) {
            builder.primaryKey("id", "dt").option("bucket", "1");
        } else if (mode.equals("data-evolution")) {
            builder.option("row-tracking.enabled", "true").option("data-evolution.enabled", "true");
        }
        Path path = new Path(tempDir.resolve(UUID.randomUUID().toString()).toUri());
        FileSystemSchemaManager manager = new FileSystemSchemaManager(LocalFileIO.create(), path);
        manager.createTable(builder.build());
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), path);
        BatchWriteBuilder writes = table.newBatchWriteBuilder();
        try (BatchTableWrite write =
                        writes.newWrite()
                                .withWriteType(
                                        table.rowType().project("name", "days", "ts", "id"));
                BatchTableCommit commit = writes.newCommit()) {
            write.write(
                    GenericRow.of(
                            BinaryString.fromString("ab"), 2, Timestamp.fromEpochMillis(-1), 1));
            commit.commit(write.prepareCommit());
        }
        try (RecordReaderIterator<InternalRow> rows =
                new RecordReaderIterator<>(table.newRead().createReader(table.newScan().plan()))) {
            InternalRow row = rows.next();
            assertThat(row.getInt(4)).isEqualTo(1);
            assertThat(row.getString(5).toString()).isEqualTo("AB_ab");
            assertThat(row.getInt(6)).isEqualTo(1969);
            assertThat(rows.hasNext()).isFalse();
        }
        assertThatThrownBy(() -> manager.commitChanges(SchemaChange.renameColumn("days", "offset")))
                .hasMessageContaining("referenced by generated column");
    }

    @Test
    void testRoutingWithoutGeneratedColumnsPreservesRawValues() throws Exception {
        List<FileStoreTable> tables = new ArrayList<>();
        for (boolean defaults : new boolean[] {false, true}) {
            Schema.Builder builder =
                    Schema.newBuilder()
                            .column("pt", DataTypes.INT(), null, defaults ? "7" : null)
                            .column("id", DataTypes.INT(), null, defaults ? "9" : null)
                            .partitionKeys("pt")
                            .primaryKey("pt", "id")
                            .option("bucket", "1031")
                            .option("bucket-key", "id");
            Path path = new Path(tempDir.resolve(UUID.randomUUID().toString()).toUri());
            new FileSystemSchemaManager(LocalFileIO.create(), path).createTable(builder.build());
            tables.add(FileStoreTableFactory.create(LocalFileIO.create(), path));
        }
        GenericRow raw = GenericRow.of(null, null);
        try (BatchTableWrite plain = tables.get(0).newBatchWriteBuilder().newWrite();
                BatchTableWrite defaults = tables.get(1).newBatchWriteBuilder().newWrite()) {
            assertThat(defaults.getPartition(raw).isNullAt(0)).isTrue();
            assertThat(defaults.getBucket(raw)).isEqualTo(plain.getBucket(raw));
            assertThat(defaults.getBucket(raw))
                    .isNotEqualTo(defaults.getBucket(GenericRow.of(7, 9)));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEffectiveRowKindWithSuppliedGeneratedValues(boolean reordered) throws Exception {
        Schema schema =
                schema(true)
                        .column("kind", DataTypes.STRING(), null, "'-D'")
                        .option("rowkind.field", "kind")
                        .build();
        Path path = new Path(tempDir.resolve(UUID.randomUUID().toString()).toUri());
        new FileSystemSchemaManager(LocalFileIO.create(), path).createTable(schema);
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), path);
        BatchWriteBuilder inserts = table.newBatchWriteBuilder();
        try (BatchTableWrite write = inserts.newWrite();
                BatchTableCommit commit = inserts.newCommit()) {
            for (int id : new int[] {1, 2}) {
                write.write(
                        GenericRow.of(
                                id,
                                0,
                                Timestamp.fromEpochMillis(0),
                                BinaryString.fromString("+I")));
            }
            commit.commit(write.prepareCommit());
        }

        RowType inputType =
                reordered ? table.rowType().project("kind", "ts", "id", "dt") : table.rowType();
        BatchWriteBuilder deletes = table.newBatchWriteBuilder();
        try (BatchTableWrite write = deletes.newWrite().withWriteType(inputType);
                BatchTableCommit commit = deletes.newCommit()) {
            GenericRow insert =
                    reordered
                            ? GenericRow.of(BinaryString.fromString("+I"), null, 3, 0)
                            : GenericRow.of(3, 0, null, BinaryString.fromString("+I"));
            insert.setRowKind(RowKind.DELETE);
            assertThat(write.getPartition(insert).getInt(0)).isZero();
            assertThat(write.getBucket(insert)).isZero();
            write.write(insert);

            for (int id : new int[] {1, 2, 3}) {
                BinaryString kind = id == 1 ? null : BinaryString.fromString("-D");
                GenericRow delete =
                        reordered
                                ? GenericRow.of(kind, null, id, 0)
                                : GenericRow.of(id, 0, null, kind);
                assertThat(delete.getRowKind()).isEqualTo(RowKind.INSERT);
                assertThat(write.getPartition(delete).getInt(0)).isZero();
                assertThat(write.getBucket(delete)).isZero();
                write.write(delete);
            }
            commit.commit(write.prepareCommit());
        }
        try (RecordReaderIterator<InternalRow> rows =
                new RecordReaderIterator<>(table.newRead().createReader(table.newScan().plan()))) {
            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testRejectGeneratedRowKindFieldAtCreation() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("operation", DataTypes.STRING())
                        .generatedColumn("kind", DataTypes.STRING(), "UPPER(operation)")
                        .primaryKey("id")
                        .option("bucket", "1")
                        .option("rowkind.field", "kind")
                        .build();
        Path path = new Path(tempDir.resolve(UUID.randomUUID().toString()).toUri());
        FileSystemSchemaManager manager = new FileSystemSchemaManager(LocalFileIO.create(), path);
        assertThatThrownBy(() -> manager.createTable(schema))
                .hasMessageContaining("cannot be used as rowkind.field");
        assertThat(manager.latest()).isEmpty();
    }

    private FileStoreTable createTable(boolean primaryKey) throws Exception {
        return createTable(primaryKey ? "primary-key" : "append");
    }

    private FileStoreTable createTable(String mode) throws Exception {
        Path path = new Path(tempDir.resolve(UUID.randomUUID().toString()).toUri());
        Schema.Builder builder = schema(mode.equals("primary-key"));
        if (mode.equals("data-evolution")) {
            builder.option("row-tracking.enabled", "true").option("data-evolution.enabled", "true");
        }
        new FileSystemSchemaManager(LocalFileIO.create(), path).createTable(builder.build());
        return FileStoreTableFactory.create(LocalFileIO.create(), path);
    }

    private Schema.Builder schema(boolean primaryKey) {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .generatedColumn("dt", DataTypes.DATE().notNull(), "CAST(ts AS DATE)")
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .partitionKeys("dt");
        if (primaryKey) {
            builder.primaryKey("id", "dt").option("bucket", "1");
        }
        return builder;
    }

    private void validate(Schema schema) {
        SchemaValidation.validateTableSchema(
                new TableSchema(
                        0,
                        schema.fields(),
                        10,
                        schema.partitionKeys(),
                        schema.primaryKeys(),
                        schema.options(),
                        null));
    }
}
