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

package org.apache.paimon.iceberg;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergRef;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for Iceberg compatibility. */
public class IcebergCompatibilityTest {

    @TempDir java.nio.file.Path tempDir;

    // ------------------------------------------------------------------------
    //  Constructed Tests
    // ------------------------------------------------------------------------

    @Test
    public void testFileLevelChange() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType, Collections.emptyList(), Collections.singletonList("k"), 1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");

        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));
        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");

        write.close();
        commit.close();
    }

    @Test
    public void testDelete() throws Exception {
        testDeleteImpl(false);
    }

    @Test
    public void testDeleteWithDeletionVector() throws Exception {
        testDeleteImpl(true);
    }

    private void testDeleteImpl(boolean deletionVector) throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> customOptions = new HashMap<>();
        if (deletionVector) {
            customOptions.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        }
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        customOptions);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write =
                table.newWrite(commitUser)
                        .withIOManager(new IOManagerImpl(tempDir.toString() + "/tmp"));
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");

        write.write(GenericRow.of(2, 21));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));
        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 21)");

        write.write(GenericRow.ofKind(RowKind.DELETE, 1, 10));
        commit.commit(3, write.prepareCommit(false, 3));
        // not changed because no full compaction
        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 21)");

        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(4, write.prepareCommit(true, 4));

        // In dv mode, full compaction will remove all dv index and rewrite data files
        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(2, 21)");

        write.close();
        commit.close();
    }

    @Test
    public void testDropPartition() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING(), DataTypes.INT(), DataTypes.INT(), DataTypes.INT()
                        },
                        new String[] {"pt1", "pt2", "k", "v"});

        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Arrays.asList("pt1", "pt2"),
                        Arrays.asList("pt1", "pt2", "k"),
                        1,
                        Collections.emptyMap());

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(BinaryString.fromString("20250304"), 15, 1, 1));
        write.write(GenericRow.of(BinaryString.fromString("20250304"), 16, 1, 1));
        write.write(GenericRow.of(BinaryString.fromString("20250305"), 15, 1, 1));
        commit.commit(1, write.prepareCommit(false, 1));

        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(20250304, 15, 1, 1)",
                        "Record(20250304, 16, 1, 1)",
                        "Record(20250305, 15, 1, 1)");

        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            if (ThreadLocalRandom.current().nextBoolean()) {
                // delete the second-level partition
                Map<String, String> partition = new HashMap<>();
                partition.put("pt1", "20250304");
                partition.put("pt2", "16");
                commit.truncatePartitions(Collections.singletonList(partition));

                assertThat(getIcebergResult())
                        .containsExactlyInAnyOrder(
                                "Record(20250304, 15, 1, 1)", "Record(20250305, 15, 1, 1)");
            } else {
                // delete the first-level partition
                Map<String, String> partition = new HashMap<>();
                partition.put("pt1", "20250304");
                commit.truncatePartitions(Collections.singletonList(partition));

                assertThat(getIcebergResult())
                        .containsExactlyInAnyOrder("Record(20250305, 15, 1, 1)");
            }
        }

        write.close();
        commit.close();
    }

    @Test
    public void testRetryCreateMetadata() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType, Collections.emptyList(), Collections.singletonList("k"), 1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");

        write.write(GenericRow.of(1, 11));
        write.write(GenericRow.of(3, 30));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        List<CommitMessage> commitMessages2 = write.prepareCommit(true, 2);
        commit.commit(2, commitMessages2);
        assertThat(table.latestSnapshot()).isPresent().map(Snapshot::id).hasValue(3L);

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        Path metadata3Path = pathFactory.toMetadataPath(3);
        assertThat(table.fileIO().exists(metadata3Path)).isTrue();

        table.fileIO().deleteQuietly(metadata3Path);
        Map<Long, List<CommitMessage>> retryMessages = new HashMap<>();
        retryMessages.put(2L, commitMessages2);
        commit.filterAndCommit(retryMessages);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder("Record(1, 11)", "Record(2, 20)", "Record(3, 30)");

        write.close();
        commit.close();
    }

    @Test
    public void testSchemaChange() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType, Collections.emptyList(), Collections.singletonList("k"), 1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(SchemaChange.addColumn("v2", DataTypes.STRING()));
        table = table.copyWithLatestSchema();
        write.close();
        write = table.newWrite(commitUser);
        commit.close();
        commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 11, BinaryString.fromString("one")));
        write.write(GenericRow.of(3, 30, BinaryString.fromString("three")));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 11, one)", "Record(2, 20, null)", "Record(3, 30, three)");

        write.close();
        commit.close();
    }

    @Test
    public void testIcebergSnapshotExpire() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "3");
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "3");
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        options);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1L);
        FileIO fileIO = table.fileIO();
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(
                        fileIO, new Path(table.location(), "metadata/v1.metadata.json"));
        assertThat(metadata.snapshots()).hasSize(1);
        assertThat(metadata.currentSnapshotId()).isEqualTo(1);

        write.write(GenericRow.of(1, 11));
        write.write(GenericRow.of(3, 30));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(3L);
        metadata =
                IcebergMetadata.fromPath(
                        fileIO, new Path(table.location(), "metadata/v3.metadata.json"));
        assertThat(metadata.snapshots()).hasSize(3);
        assertThat(metadata.currentSnapshotId()).isEqualTo(3);

        // Number of snapshots will become 5 with the next commit, however only 3 Iceberg snapshots
        // are kept. So the first 2 Iceberg snapshots will be expired.

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestList manifestList = IcebergManifestList.create(table, pathFactory);
        assertThat(manifestList.compression()).isEqualTo("snappy");

        IcebergManifestFile manifestFile = IcebergManifestFile.create(table, pathFactory);
        assertThat(manifestFile.compression()).isEqualTo("snappy");

        Set<String> usingManifests = new HashSet<>();
        String manifestListFile = new Path(metadata.currentSnapshot().manifestList()).getName();

        assertThat(fileIO.readFileUtf8(new Path(pathFactory.metadataDirectory(), manifestListFile)))
                .contains("snappy");

        for (IcebergManifestFileMeta fileMeta : manifestList.read(manifestListFile)) {
            usingManifests.add(fileMeta.manifestPath());
            assertThat(
                            fileIO.readFileUtf8(
                                    new Path(
                                            pathFactory.metadataDirectory(),
                                            fileMeta.manifestPath())))
                    .contains("snappy");
        }

        IcebergManifestList legacyManifestList =
                IcebergManifestList.create(
                        table.copy(
                                Collections.singletonMap(
                                        IcebergOptions.MANIFEST_LEGACY_VERSION.key(), "true")),
                        pathFactory);
        assertThatThrownBy(() -> legacyManifestList.read(manifestListFile))
                .rootCause()
                .isInstanceOf(NullPointerException.class);

        Set<String> unusedFiles = new HashSet<>();
        for (int i = 0; i < 2; i++) {
            unusedFiles.add(metadata.snapshots().get(i).manifestList());
            for (IcebergManifestFileMeta fileMeta :
                    manifestList.read(
                            new Path(metadata.snapshots().get(i).manifestList()).getName())) {
                String p = fileMeta.manifestPath();
                if (!usingManifests.contains(p)) {
                    unusedFiles.add(p);
                }
            }
        }

        write.write(GenericRow.of(2, 21));
        write.write(GenericRow.of(3, 31));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(3, write.prepareCommit(true, 3));
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(5L);
        metadata =
                IcebergMetadata.fromPath(
                        fileIO, new Path(table.location(), "metadata/v5.metadata.json"));
        assertThat(metadata.snapshots()).hasSize(3);
        assertThat(metadata.currentSnapshotId()).isEqualTo(5);

        write.close();
        commit.close();

        // The old metadata.json is removed when the new metadata.json is created
        // depending on the old metadata retention configuration.
        for (int i = 1; i <= 3; i++) {
            unusedFiles.add(pathFactory.toMetadataPath(i).toString());
        }

        for (String path : unusedFiles) {
            assertThat(fileIO.exists(new Path(path))).isFalse();
        }

        // Check existence of retained Iceberg metadata.json files
        for (int i = 4; i <= 5; i++) {
            assertThat(fileIO.exists(new Path(pathFactory.toMetadataPath(i).toString()))).isTrue();
        }

        // Test all existing Iceberg snapshots are valid.
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder("Record(1, 11)", "Record(2, 21)", "Record(3, 31)");
        assertThat(
                        getIcebergResult(
                                icebergTable ->
                                        IcebergGenerics.read(icebergTable).useSnapshot(3).build(),
                                Record::toString))
                .containsExactlyInAnyOrder("Record(1, 11)", "Record(2, 20)", "Record(3, 30)");
        assertThat(
                        getIcebergResult(
                                icebergTable ->
                                        IcebergGenerics.read(icebergTable).useSnapshot(4).build(),
                                Record::toString))
                .containsExactlyInAnyOrder("Record(1, 11)", "Record(2, 20)", "Record(3, 30)");
    }

    @Test
    public void testAllTypeStatistics() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.TINYINT(),
                            DataTypes.SMALLINT(),
                            DataTypes.BOOLEAN(),
                            DataTypes.BIGINT(),
                            DataTypes.FLOAT(),
                            DataTypes.DOUBLE(),
                            DataTypes.DECIMAL(8, 3),
                            DataTypes.CHAR(20),
                            DataTypes.STRING(),
                            DataTypes.BINARY(20),
                            DataTypes.VARBINARY(20),
                            DataTypes.DATE(),
                            DataTypes.TIMESTAMP(6)
                        },
                        new String[] {
                            "v_int",
                            "v_tinyint",
                            "v_smallint",
                            "v_boolean",
                            "v_bigint",
                            "v_float",
                            "v_double",
                            "v_decimal",
                            "v_char",
                            "v_varchar",
                            "v_binary",
                            "v_varbinary",
                            "v_date",
                            "v_timestamp"
                        });
        FileStoreTable table =
                createPaimonTable(rowType, Collections.emptyList(), Collections.emptyList(), -1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        GenericRow lowerBounds =
                GenericRow.of(
                        1,
                        (byte) 1,
                        (short) 1,
                        true,
                        10L,
                        100.0f,
                        1000.0,
                        Decimal.fromUnscaledLong(123456, 8, 3),
                        BinaryString.fromString("apple"),
                        BinaryString.fromString("cat"),
                        "B_apple".getBytes(),
                        "B_cat".getBytes(),
                        100,
                        Timestamp.fromLocalDateTime(LocalDateTime.of(2024, 10, 10, 11, 22, 33)));
        write.write(lowerBounds);
        GenericRow upperBounds =
                GenericRow.of(
                        2,
                        (byte) 3,
                        (short) 4,
                        true,
                        20L,
                        200.0f,
                        2000.0,
                        Decimal.fromUnscaledLong(234567, 8, 3),
                        BinaryString.fromString("banana"),
                        BinaryString.fromString("dog"),
                        "B_banana".getBytes(),
                        "B_dog".getBytes(),
                        200,
                        Timestamp.fromLocalDateTime(LocalDateTime.of(2024, 10, 20, 11, 22, 33)));
        write.write(upperBounds);
        commit.commit(1, write.prepareCommit(false, 1));

        write.close();
        commit.close();

        int numFields = rowType.getFieldCount();
        for (int i = 0; i < numFields; i++) {
            DataType type = rowType.getTypeAt(i);
            String name = rowType.getFieldNames().get(i);
            if (type.getTypeRoot() == DataTypeRoot.BOOLEAN
                    || type.getTypeRoot() == DataTypeRoot.BINARY
                    || type.getTypeRoot() == DataTypeRoot.VARBINARY) {
                // lower bounds and upper bounds of these types have no actual use case
                continue;
            }

            final Object lower;
            final Object upper;
            // change Paimon objects to Iceberg Java API objects
            if (type.getTypeRoot() == DataTypeRoot.CHAR
                    || type.getTypeRoot() == DataTypeRoot.VARCHAR) {
                lower = lowerBounds.getField(i).toString();
                upper = upperBounds.getField(i).toString();
            } else if (type.getTypeRoot() == DataTypeRoot.DECIMAL) {
                lower = new BigDecimal(lowerBounds.getField(i).toString());
                upper = new BigDecimal(upperBounds.getField(i).toString());
            } else if (type.getTypeRoot() == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                lower = ((Timestamp) lowerBounds.getField(i)).toMicros();
                upper = ((Timestamp) upperBounds.getField(i)).toMicros();
            } else {
                lower = lowerBounds.getField(i);
                upper = upperBounds.getField(i);
            }

            String expectedLower = lower.toString();
            String expectedUpper = upper.toString();
            if (type.getTypeRoot() == DataTypeRoot.DATE) {
                expectedLower = LocalDate.ofEpochDay((int) lower).toString();
                expectedUpper = LocalDate.ofEpochDay((int) upper).toString();
            } else if (type.getTypeRoot() == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                expectedLower = Timestamp.fromMicros((long) lower).toString();
                expectedUpper = Timestamp.fromMicros((long) upper).toString();
            }

            assertThat(
                            getIcebergResult(
                                    icebergTable ->
                                            IcebergGenerics.read(icebergTable)
                                                    .select(name)
                                                    .where(
                                                            // Handle numeric primitive wrappers
                                                            // that need conversion
                                                            upper instanceof Short
                                                                    ? Expressions.lessThan(
                                                                            name,
                                                                            ((Short) upper)
                                                                                    .intValue())
                                                                    : upper instanceof Byte
                                                                            ? Expressions.lessThan(
                                                                                    name,
                                                                                    ((Byte) upper)
                                                                                            .intValue())
                                                                            : Expressions.lessThan(
                                                                                    name, upper))
                                                    .build(),
                                    Record::toString))
                    .containsExactly("Record(" + expectedLower + ")");
            assertThat(
                            getIcebergResult(
                                    icebergTable ->
                                            IcebergGenerics.read(icebergTable)
                                                    .select(name)
                                                    .where(
                                                            // Handle numeric primitive wrappers
                                                            // that need conversion
                                                            lower instanceof Short
                                                                    ? Expressions.greaterThan(
                                                                            name,
                                                                            ((Short) lower)
                                                                                    .intValue())
                                                                    : lower instanceof Byte
                                                                            ? Expressions
                                                                                    .greaterThan(
                                                                                            name,
                                                                                            ((Byte)
                                                                                                            lower)
                                                                                                    .intValue())
                                                                            : Expressions
                                                                                    .greaterThan(
                                                                                            name,
                                                                                            lower))
                                                    .build(),
                                    Record::toString))
                    .containsExactly("Record(" + expectedUpper + ")");
            assertThat(
                            getIcebergResult(
                                    icebergTable ->
                                            IcebergGenerics.read(icebergTable)
                                                    .select(name)
                                                    .where(
                                                            // Handle numeric primitive wrappers
                                                            // that need conversion
                                                            lower instanceof Short
                                                                    ? Expressions.lessThan(
                                                                            name,
                                                                            ((Short) lower)
                                                                                    .intValue())
                                                                    : lower instanceof Byte
                                                                            ? Expressions.lessThan(
                                                                                    name,
                                                                                    ((Byte) lower)
                                                                                            .intValue())
                                                                            : Expressions.lessThan(
                                                                                    name, lower))
                                                    .build(),
                                    Record::toString))
                    .isEmpty();
            assertThat(
                            getIcebergResult(
                                    icebergTable ->
                                            IcebergGenerics.read(icebergTable)
                                                    .select(name)
                                                    .where(
                                                            // Handle numeric primitive wrappers
                                                            // that need conversion
                                                            upper instanceof Short
                                                                    ? Expressions.greaterThan(
                                                                            name,
                                                                            ((Short) upper)
                                                                                    .intValue())
                                                                    : upper instanceof Byte
                                                                            ? Expressions
                                                                                    .greaterThan(
                                                                                            name,
                                                                                            ((Byte)
                                                                                                            upper)
                                                                                                    .intValue())
                                                                            : Expressions
                                                                                    .greaterThan(
                                                                                            name,
                                                                                            upper))
                                                    .build(),
                                    Record::toString))
                    .isEmpty();
        }
    }

    @Test
    public void testNestedTypes() throws Exception {
        RowType innerType =
                RowType.of(
                        new DataField(2, "f1", DataTypes.STRING()),
                        new DataField(3, "f2", DataTypes.INT()));
        RowType rowType =
                RowType.of(
                        new DataField(0, "k", DataTypes.INT()),
                        new DataField(
                                1,
                                "v",
                                DataTypes.MAP(DataTypes.INT(), DataTypes.ARRAY(innerType))));
        FileStoreTable table =
                createPaimonTable(rowType, Collections.emptyList(), Collections.emptyList(), -1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        Map<Integer, GenericArray> map1 = new HashMap<>();
        map1.put(
                10,
                new GenericArray(
                        new GenericRow[] {
                            GenericRow.of(BinaryString.fromString("apple"), 100),
                            GenericRow.of(BinaryString.fromString("banana"), 101)
                        }));
        write.write(GenericRow.of(1, new GenericMap(map1)));

        Map<Integer, GenericArray> map2 = new HashMap<>();
        map2.put(
                20,
                new GenericArray(
                        new GenericRow[] {
                            GenericRow.of(BinaryString.fromString("cherry"), 200),
                            GenericRow.of(BinaryString.fromString("pear"), 201)
                        }));
        write.write(GenericRow.of(2, new GenericMap(map2)));

        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, {10=[Record(apple, 100), Record(banana, 101)]})",
                        "Record(2, {20=[Record(cherry, 200), Record(pear, 201)]})");
    }

    @Test
    public void testStringPartitionNullPadding() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.VARCHAR(20)},
                        new String[] {"k", "country"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.singletonList("country"),
                        Collections.singletonList("k"),
                        -1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, BinaryString.fromString("Switzerland")), 1);
        write.write(GenericRow.of(2, BinaryString.fromString("Australia")), 1);
        write.write(GenericRow.of(3, BinaryString.fromString("Brazil")), 1);
        write.write(GenericRow.of(4, BinaryString.fromString("Grand Duchy of Luxembourg")), 1);
        commit.commit(1, write.prepareCommit(false, 1));
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, Switzerland)",
                        "Record(2, Australia)",
                        "Record(3, Brazil)",
                        "Record(4, Grand Duchy of Luxembourg)");

        FileIO fileIO = table.fileIO();
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(
                        fileIO, new Path(table.location(), "metadata/v1.metadata.json"));

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestList manifestList = IcebergManifestList.create(table, pathFactory);
        String currentSnapshotManifest = metadata.currentSnapshot().manifestList();

        File snapShotAvroFile = new File(currentSnapshotManifest);
        String expectedPartitionSummary =
                "[{\"contains_null\": false, \"contains_nan\": false, \"lower_bound\": \"Australia\", \"upper_bound\": \"Switzerland\"}]";
        try (DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<>(
                        new SeekableFileInput(snapShotAvroFile), new GenericDatumReader<>())) {
            while (dataFileReader.hasNext()) {
                GenericRecord record = dataFileReader.next();
                String partitionSummary = record.get("partitions").toString();
                assertThat(partitionSummary).doesNotContain("\\u0000");
                assertThat(partitionSummary).isEqualTo(expectedPartitionSummary);
            }
        }

        String tableManifest = manifestList.read(snapShotAvroFile.getName()).get(0).manifestPath();

        try (DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<>(
                        new SeekableFileInput(new File(tableManifest)),
                        new GenericDatumReader<>())) {

            while (dataFileReader.hasNext()) {
                GenericRecord record = dataFileReader.next();
                GenericRecord dataFile = (GenericRecord) record.get("data_file");

                // Check lower bounds
                GenericData.Array<?> lowerBounds =
                        (GenericData.Array<?>) dataFile.get("lower_bounds");
                if (lowerBounds != null) {
                    for (Object bound : lowerBounds) {
                        GenericRecord boundRecord = (GenericRecord) bound;
                        int key = (Integer) boundRecord.get("key");
                        if (key == 1) { // key = 1 is the partition key
                            ByteBuffer value = (ByteBuffer) boundRecord.get("value");
                            String boundValue = new String(value.array(), StandardCharsets.UTF_8);
                            assertThat(boundValue).doesNotContain("\u0000");
                        }
                    }
                }

                // Check upper bounds
                GenericData.Array<?> upperBounds =
                        (GenericData.Array<?>) dataFile.get("upper_bounds");
                if (upperBounds != null) {
                    for (Object bound : upperBounds) {
                        GenericRecord boundRecord = (GenericRecord) bound;
                        int key = (Integer) boundRecord.get("key");
                        if (key == 1) { // key = 1 is the partition key
                            ByteBuffer value = (ByteBuffer) boundRecord.get("value");
                            String boundValue = new String(value.array(), StandardCharsets.UTF_8);
                            assertThat(boundValue).doesNotContain("\u0000");
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testWithIncorrectBase() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        1,
                        Collections.emptyMap());

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));

        write.write(GenericRow.of(1, 11));
        write.write(GenericRow.of(3, 30));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder("Record(1, 11)", "Record(2, 20)", "Record(3, 30)");

        // disable iceberg compatibility
        Map<String, String> options = new HashMap<>();
        options.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "disabled");
        table = table.copy(options);
        write.close();
        write = table.newWrite(commitUser);
        commit.close();
        commit = table.newCommit(commitUser);

        write.write(GenericRow.of(4, 40));
        write.write(GenericRow.of(5, 50));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(3, write.prepareCommit(true, 3));
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(5L);

        // enable iceberg compatibility
        options.put(
                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                IcebergOptions.StorageType.TABLE_LOCATION.toString());
        table = table.copy(options);
        write.close();
        write = table.newWrite(commitUser);
        commit.close();
        commit = table.newCommit(commitUser);

        write.write(GenericRow.of(6, 60));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(4, write.prepareCommit(true, 4));
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(7L);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 11)",
                        "Record(2, 20)",
                        "Record(3, 30)",
                        "Record(4, 40)",
                        "Record(5, 50)",
                        "Record(6, 60)");

        write.close();
        commit.close();
    }

    /*
    Create snapshots
    Create tags
    Verify tags
    Delete a tag
    Verify tags
     */
    @Test
    public void testCreateAndDeleteTags() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType, Collections.emptyList(), Collections.singletonList("k"), 1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));

        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");

        write.write(GenericRow.of(3, 30));
        write.write(GenericRow.of(4, 40));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));

        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 10)", "Record(2, 20)", "Record(3, 30)", "Record(4, 40)");

        String tagV1 = "v1";
        table.createTag(tagV1, 1);
        String tagV3 = "v3";
        table.createTag(tagV3, 3);

        long latestSnapshotId = table.snapshotManager().latestSnapshotId();
        Map<String, IcebergRef> refs = getIcebergRefsFromSnapshot(table, latestSnapshotId);

        assertThat(refs.size() == 2).isTrue();

        assertThat(refs.get(tagV1).snapshotId() == 1).isTrue();
        assertThat(refs.get(tagV1).type().equals("tag")).isTrue(); // constant
        assertThat(refs.get(tagV1).maxRefAgeMs() == Long.MAX_VALUE).isTrue(); // constant

        assertThat(refs.get(tagV3).snapshotId() == latestSnapshotId).isTrue();

        assertThat(
                        getIcebergResult(
                                icebergTable ->
                                        IcebergGenerics.read(icebergTable)
                                                .useSnapshot(
                                                        icebergTable.refs().get(tagV1).snapshotId())
                                                .build(),
                                Record::toString))
                .containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");
        assertThat(
                        getIcebergResult(
                                icebergTable ->
                                        IcebergGenerics.read(icebergTable)
                                                .useSnapshot(
                                                        icebergTable.refs().get(tagV3).snapshotId())
                                                .build(),
                                Record::toString))
                .containsExactlyInAnyOrder(
                        "Record(1, 10)", "Record(2, 20)", "Record(3, 30)", "Record(4, 40)");

        table.deleteTag(tagV1);

        Map<String, IcebergRef> refsAfterDelete =
                getIcebergRefsFromSnapshot(table, latestSnapshotId);

        assertThat(refsAfterDelete.size() == 1).isTrue();
        assertThat(refsAfterDelete.get(tagV3).snapshotId() == latestSnapshotId).isTrue();

        assertThat(
                        getIcebergResult(
                                icebergTable ->
                                        IcebergGenerics.read(icebergTable)
                                                .useSnapshot(
                                                        icebergTable.refs().get(tagV3).snapshotId())
                                                .build(),
                                Record::toString))
                .containsExactlyInAnyOrder(
                        "Record(1, 10)", "Record(2, 20)", "Record(3, 30)", "Record(4, 40)");

        write.close();
        commit.close();
    }

    // Create snapshots and Iceberg metadata
    // Delete Iceberg metadata
    // Create tag - this should not create any metadata file
    // Delete tag - this should not create any metadata file
    @Test
    public void testTagCallbackTakesNoActionIfIcebergMetadataDoesNotExist() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType, Collections.emptyList(), Collections.singletonList("k"), 1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));

        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");

        // Delete Iceberg metadata
        Path metadataPath = new Path(table.location(), "metadata");
        table.fileIO().deleteDirectoryQuietly(metadataPath);

        assertThat(table.fileIO().listFiles(metadataPath, false).length == 0);

        String tagV1 = "v1";
        table.createTag(tagV1, 1);

        assertThat(table.fileIO().listFiles(metadataPath, false).length == 0);

        table.deleteTag(tagV1);

        assertThat(table.fileIO().listFiles(metadataPath, false).length == 0);

        write.close();
        commit.close();
    }

    // Create snapshots and Iceberg metadata
    // Delete Iceberg metadata
    // Create a snapshot to create Iceberg metadata
    // Create tag on a snapshot that does not exist in Iceberg - this should not add a tag.
    @Test
    public void testTagCallbackDoesNotAddTagIfSnapshotDoesNotExistInIceberg() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType, Collections.emptyList(), Collections.singletonList("k"), 1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));

        // Delete Iceberg metadata
        Path metadataPath = new Path(table.location(), "metadata");
        table.fileIO().deleteDirectoryQuietly(metadataPath);

        write.write(GenericRow.of(3, 30));
        write.write(GenericRow.of(4, 40));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));

        String tagV1 = "v1";
        long tagV1SnapshotId = 1;
        table.createTag(tagV1, tagV1SnapshotId);

        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 10)", "Record(2, 20)", "Record(3, 30)", "Record(4, 40)");

        assertThat(
                        getIcebergRefsFromSnapshot(
                                                table, table.snapshotManager().latestSnapshotId())
                                        .size()
                                == 0)
                .isTrue();

        write.close();
        commit.close();
    }

    /*
    Create a snapshot and tag t1
    Verify t1 in Iceberg
    Delete Iceberg metadata
    Create a snapshot
    Verify Iceberg tags are empty
    Create a snapshot and tag t5
    Create a snapshot
    Only t5 should be visible in Iceberg
     */
    @Test
    public void testTagsAreOnlyAddedToIcebergDuringCommitCallbackIfSnapshotExistsInIceberg()
            throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        FileStoreTable table =
                createPaimonTable(
                        rowType, Collections.emptyList(), Collections.singletonList("k"), 1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));

        String tagV1 = "v1";
        long tagV1SnapshotId = 1;
        table.createTag(tagV1, tagV1SnapshotId);

        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");

        write.write(GenericRow.of(3, 30));
        write.write(GenericRow.of(4, 40));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(2, write.prepareCommit(true, 2));

        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 10)", "Record(2, 20)", "Record(3, 30)", "Record(4, 40)");

        // Verify tag
        Map<String, IcebergRef> refs =
                getIcebergRefsFromSnapshot(table, table.snapshotManager().latestSnapshotId());
        assertThat(refs.size() == 1).isTrue();
        assertThat(refs.get(tagV1).snapshotId() == tagV1SnapshotId).isTrue();

        assertThat(
                        getIcebergResult(
                                icebergTable ->
                                        IcebergGenerics.read(icebergTable)
                                                .useSnapshot(
                                                        icebergTable.refs().get(tagV1).snapshotId())
                                                .build(),
                                Record::toString))
                .containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");

        // Delete Iceberg metadata so that metadata is created without base
        table.fileIO().deleteDirectoryQuietly(new Path(table.location(), "metadata"));

        write.write(GenericRow.of(5, 50));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(4, write.prepareCommit(true, 4));

        refs = getIcebergRefsFromSnapshot(table, table.snapshotManager().latestSnapshotId());
        assertThat(refs.size() == 0).isTrue();

        String tagV5 = "v5";
        long tagV5SnapshotId = 5;
        table.createTag(tagV5, tagV5SnapshotId);

        write.write(GenericRow.of(6, 60));
        write.compact(BinaryRow.EMPTY_ROW, 0, true);
        commit.commit(6, write.prepareCommit(true, 4));

        refs = getIcebergRefsFromSnapshot(table, table.snapshotManager().latestSnapshotId());
        assertThat(refs.size() == 1).isTrue();
        assertThat(refs.get(tagV5).snapshotId() == tagV5SnapshotId).isTrue();

        assertThat(
                        getIcebergResult(
                                icebergTable ->
                                        IcebergGenerics.read(icebergTable)
                                                .useSnapshot(
                                                        icebergTable.refs().get(tagV5).snapshotId())
                                                .build(),
                                Record::toString))
                .containsExactlyInAnyOrder(
                        "Record(1, 10)",
                        "Record(2, 20)",
                        "Record(3, 30)",
                        "Record(4, 40)",
                        "Record(5, 50)");
    }

    private Map<String, IcebergRef> getIcebergRefsFromSnapshot(
            FileStoreTable table, long snapshotId) {
        return IcebergMetadata.fromPath(
                        table.fileIO(),
                        new Path(table.location(), "metadata/v" + snapshotId + ".metadata.json"))
                .refs();
    }

    // ------------------------------------------------------------------------
    //  Random Tests
    // ------------------------------------------------------------------------

    @Test
    public void testUnPartitionedPrimaryKeyTable() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(), DataTypes.STRING(), DataTypes.INT(), DataTypes.BIGINT()
                        },
                        new String[] {"k1", "k2", "v1", "v2"});

        int numRounds = 20;
        int numRecords = 1000;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<List<TestRecord>> testRecords = new ArrayList<>();
        List<List<String>> expected = new ArrayList<>();
        Map<String, String> expectedMap = new LinkedHashMap<>();
        for (int r = 0; r < numRounds; r++) {
            List<TestRecord> round = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                int k1 = random.nextInt(0, 100);
                String k2 = String.valueOf(random.nextInt(1000, 1010));
                int v1 = random.nextInt();
                long v2 = random.nextLong();
                round.add(
                        new TestRecord(
                                BinaryRow.EMPTY_ROW,
                                GenericRow.of(k1, BinaryString.fromString(k2), v1, v2)));
                expectedMap.put(String.format("%d, %s", k1, k2), String.format("%d, %d", v1, v2));
            }
            testRecords.add(round);
            expected.add(
                    expectedMap.entrySet().stream()
                            .map(e -> String.format("Record(%s, %s)", e.getKey(), e.getValue()))
                            .collect(Collectors.toList()));
        }

        runCompatibilityTest(
                rowType,
                Collections.emptyList(),
                Arrays.asList("k1", "k2"),
                testRecords,
                expected,
                Record::toString);
    }

    @Test
    public void testPartitionedPrimaryKeyTable() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"pt1", "pt2", "k", "v1", "v2"});

        BiFunction<Integer, String, BinaryRow> binaryRow =
                (pt1, pt2) -> {
                    BinaryRow b = new BinaryRow(2);
                    BinaryRowWriter writer = new BinaryRowWriter(b);
                    writer.writeInt(0, pt1);
                    writer.writeString(1, BinaryString.fromString(pt2));
                    writer.complete();
                    return b;
                };

        int numRounds = 20;
        int numRecords = 500;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        boolean samePartitionEachRound = random.nextBoolean();

        List<List<TestRecord>> testRecords = new ArrayList<>();
        List<List<String>> expected = new ArrayList<>();
        Map<String, String> expectedMap = new LinkedHashMap<>();
        for (int r = 0; r < numRounds; r++) {
            List<TestRecord> round = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                int pt1 = (random.nextInt(0, samePartitionEachRound ? 1 : 2) + r) % 3;
                String pt2 = String.valueOf(random.nextInt(10, 12));
                String k = String.valueOf(random.nextInt(0, 100));
                int v1 = random.nextInt();
                long v2 = random.nextLong();
                round.add(
                        new TestRecord(
                                binaryRow.apply(pt1, pt2),
                                GenericRow.of(
                                        pt1,
                                        BinaryString.fromString(pt2),
                                        BinaryString.fromString(k),
                                        v1,
                                        v2)));
                expectedMap.put(
                        String.format("%d, %s, %s", pt1, pt2, k), String.format("%d, %d", v1, v2));
            }
            testRecords.add(round);
            expected.add(
                    expectedMap.entrySet().stream()
                            .map(e -> String.format("Record(%s, %s)", e.getKey(), e.getValue()))
                            .collect(Collectors.toList()));
        }

        runCompatibilityTest(
                rowType,
                Arrays.asList("pt1", "pt2"),
                Arrays.asList("pt1", "pt2", "k"),
                testRecords,
                expected,
                Record::toString);
    }

    @Test
    public void testIcebergAvroFieldIds() throws Exception {

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(), DataTypes.VARCHAR(20), DataTypes.VARCHAR(20)
                        },
                        new String[] {"k", "country", "day"});
        FileStoreTable table =
                createPaimonTable(
                        rowType,
                        Arrays.asList("country", "day"),
                        Collections.singletonList("k"),
                        -1);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(
                GenericRow.of(
                        1, BinaryString.fromString("Switzerland"), BinaryString.fromString("June")),
                1);
        write.write(
                GenericRow.of(
                        2, BinaryString.fromString("Australia"), BinaryString.fromString("July")),
                1);
        write.write(
                GenericRow.of(
                        3, BinaryString.fromString("Brazil"), BinaryString.fromString("October")),
                1);
        write.write(
                GenericRow.of(
                        4,
                        BinaryString.fromString("Grand Duchy of Luxembourg"),
                        BinaryString.fromString("November")),
                1);
        commit.commit(1, write.prepareCommit(false, 1));
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, Switzerland, June)",
                        "Record(2, Australia, July)",
                        "Record(3, Brazil, October)",
                        "Record(4, Grand Duchy of Luxembourg, November)");

        org.apache.iceberg.Table icebergTable = getIcebergTable();
        String manifestListLocation = icebergTable.currentSnapshot().manifestListLocation();

        Map<String, Integer> manifestListFieldIdsMap =
                parseAvroSchemaFieldIds(manifestListLocation);
        assertThat(manifestListFieldIdsMap)
                .hasSize(19)
                .containsEntry("manifest_file:r508:contains_null", 509)
                .containsEntry("manifest_file:r508:contains_nan", 518)
                .containsEntry("manifest_file:added_snapshot_id", 503)
                .containsEntry("manifest_file:added_files_count", 504)
                .containsEntry("manifest_file:deleted_rows_count", 514)
                .containsEntry("manifest_file:added_rows_count", 512)
                .containsEntry("manifest_file:manifest_length", 501)
                .containsEntry("manifest_file:partition_spec_id", 502)
                .containsEntry("manifest_file:deleted_files_count", 506)
                .containsEntry("manifest_file:partitions", 507)
                .containsEntry("manifest_file:existing_files_count", 505)
                .containsEntry("manifest_file:r508:upper_bound", 511)
                .containsEntry("manifest_file:sequence_number", 515)
                .containsEntry("manifest_file:min_sequence_number", 516)
                .containsEntry("manifest_file:r508:lower_bound", 510)
                .containsEntry("manifest_file:manifest_path", 500)
                .containsEntry("manifest_file:content", 517)
                .containsEntry("manifest_file:existing_rows_count", 513)
                .containsEntry("r508", 508);

        String manifestPath =
                icebergTable.currentSnapshot().allManifests(icebergTable.io()).get(0).path();
        Map<String, Integer> manifestFieldIdsMap = parseAvroSchemaFieldIds(manifestPath);
        assertThat(manifestFieldIdsMap)
                .hasSize(28)
                .containsEntry("manifest_entry:status", 0)
                .containsEntry("manifest_entry:snapshot_id", 1)
                .containsEntry("manifest_entry:data_file", 2)
                .containsEntry("manifest_entry:sequence_number", 3)
                .containsEntry("manifest_entry:file_sequence_number", 4)
                .containsEntry("manifest_entry:r2:file_path", 100)
                .containsEntry("manifest_entry:r2:file_format", 101)
                .containsEntry("manifest_entry:r2:partition", 102)
                .containsEntry("manifest_entry:r2:record_count", 103)
                .containsEntry("manifest_entry:r2:file_size_in_bytes", 104)
                .containsEntry("manifest_entry:r2:null_value_counts", 110)
                .containsEntry("manifest_entry:r2:k121_v122:value", 122)
                .containsEntry("manifest_entry:r2:k121_v122:key", 121)
                .containsEntry("manifest_entry:r2:lower_bounds", 125)
                .containsEntry("manifest_entry:r2:k126_v127:key", 126)
                .containsEntry("manifest_entry:r2:k126_v127:value", 127)
                .containsEntry("manifest_entry:r2:upper_bounds", 128)
                .containsEntry("manifest_entry:r2:k129_v130:key", 129)
                .containsEntry("manifest_entry:r2:k129_v130:value", 130)
                .containsEntry("manifest_entry:r2:content", 134)
                .containsEntry("manifest_entry:r2:referenced_data_file", 143)
                .containsEntry("manifest_entry:r2:content_offset", 144)
                .containsEntry("manifest_entry:r2:content_size_in_bytes", 145)
                .containsEntry("manifest_entry:r2:r102:country", 1000)
                .containsEntry("manifest_entry:r2:r102:day", 1001);

        write.close();
        commit.close();
    }

    private void runCompatibilityTest(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<List<TestRecord>> testRecords,
            List<List<String>> expected,
            Function<Record, String> icebergRecordToString)
            throws Exception {
        FileStoreTable table =
                createPaimonTable(
                        rowType, partitionKeys, primaryKeys, primaryKeys.isEmpty() ? -1 : 2);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        for (int r = 0; r < testRecords.size(); r++) {
            List<TestRecord> round = testRecords.get(r);
            for (TestRecord testRecord : round) {
                write.write(testRecord.record);
            }

            if (!primaryKeys.isEmpty()) {
                for (BinaryRow partition :
                        round.stream().map(t -> t.partition).collect(Collectors.toSet())) {
                    for (int b = 0; b < 2; b++) {
                        write.compact(partition, b, true);
                    }
                }
            }
            commit.commit(r, write.prepareCommit(true, r));

            assertThat(
                            getIcebergResult(
                                    icebergTable -> IcebergGenerics.read(icebergTable).build(),
                                    icebergRecordToString))
                    .hasSameElementsAs(expected.get(r));
        }

        write.close();
        commit.close();
    }

    private static class TestRecord {

        private final BinaryRow partition;
        private final GenericRow record;

        private TestRecord(BinaryRow partition, GenericRow record) {
            this.partition = partition;
            this.record = record;
        }
    }

    // ------------------------------------------------------------------------
    //  Utils
    // ------------------------------------------------------------------------

    private FileStoreTable createPaimonTable(
            RowType rowType, List<String> partitionKeys, List<String> primaryKeys, int numBuckets)
            throws Exception {
        return createPaimonTable(rowType, partitionKeys, primaryKeys, numBuckets, new HashMap<>());
    }

    private FileStoreTable createPaimonTable(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            int numBuckets,
            Map<String, String> customOptions)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        Options options = new Options(customOptions);
        options.set(CoreOptions.BUCKET, numBuckets);
        options.set(
                IcebergOptions.METADATA_ICEBERG_STORAGE, IcebergOptions.StorageType.TABLE_LOCATION);
        options.set(CoreOptions.FILE_FORMAT, "avro");
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));
        options.set(IcebergOptions.COMPACT_MIN_FILE_NUM, 4);
        options.set(IcebergOptions.COMPACT_MIN_FILE_NUM, 8);
        options.set(IcebergOptions.METADATA_DELETE_AFTER_COMMIT, true);
        options.set(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX, 1);
        options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE, MemorySize.ofKibiBytes(8));
        Schema schema =
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, options.toMap(), "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }
    }

    private List<String> getIcebergResult() throws Exception {
        return getIcebergResult(
                icebergTable -> IcebergGenerics.read(icebergTable).build(), Record::toString);
    }

    private org.apache.iceberg.Table getIcebergTable() {
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of("mydb.db", "t");
        return icebergCatalog.loadTable(icebergIdentifier);
    }

    private List<String> getIcebergResult(
            Function<org.apache.iceberg.Table, CloseableIterable<Record>> query,
            Function<Record, String> icebergRecordToString)
            throws Exception {
        org.apache.iceberg.Table icebergTable = getIcebergTable();
        CloseableIterable<Record> result = query.apply(icebergTable);
        List<String> actual = new ArrayList<>();
        for (Record record : result) {
            actual.add(icebergRecordToString.apply(record));
        }
        result.close();
        return actual;
    }

    private Map<String, Integer> parseAvroSchemaFieldIds(String avroPath) throws Exception {
        Map<String, Integer> fieldIdMap = new HashMap<>();
        try (DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<>(
                        new SeekableFileInput(new File(avroPath)), new GenericDatumReader<>())) {
            org.apache.avro.Schema schema = dataFileReader.getSchema();
            parseAvroFields(schema, fieldIdMap, schema.getName());
        }
        return fieldIdMap;
    }

    private void parseAvroFields(
            org.apache.avro.Schema schema, Map<String, Integer> fieldIdMap, String rootName) {
        for (Field field : schema.getFields()) {
            String fieldIdStr = field.getProp("field-id");
            if (fieldIdStr != null) {
                fieldIdMap.put(rootName + ":" + field.name(), Integer.parseInt(fieldIdStr));
            }

            org.apache.avro.Schema fieldSchema = field.schema();
            if (fieldSchema.getType() == org.apache.avro.Schema.Type.UNION) {
                fieldSchema =
                        fieldSchema.getTypes().stream()
                                .filter(s -> s.getType() != Type.NULL)
                                .findFirst()
                                .get();
            }
            if (Objects.requireNonNull(fieldSchema.getType()) == Type.RECORD) {
                parseAvroFields(fieldSchema, fieldIdMap, rootName + ":" + fieldSchema.getName());
            } else if (fieldSchema.getType() == Type.ARRAY) {
                org.apache.avro.Schema elementType = fieldSchema.getElementType();
                String elementIdStr = fieldSchema.getProp("element-id");
                if (elementIdStr != null) {
                    fieldIdMap.put(elementType.getName(), Integer.parseInt(elementIdStr));
                }
                if (elementType.getType() == Type.RECORD) {
                    parseAvroFields(
                            elementType, fieldIdMap, rootName + ":" + elementType.getName());
                }
            }
        }
    }
}
