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
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
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
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
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
        if (deletionVector) {
            // level 0 file is compacted into deletion vector, so max level data file does not
            // change
            // this is still a valid table state at some time in the history
            assertThat(getIcebergResult())
                    .containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 21)");
        } else {
            assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(2, 21)");
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
        assertThat(table.latestSnapshotId()).hasValue(3L);

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

        // The old metadata.json is removed when the new metadata.json is created.
        for (int i = 1; i <= 4; i++) {
            unusedFiles.add(pathFactory.toMetadataPath(i).toString());
        }

        for (String path : unusedFiles) {
            assertThat(fileIO.exists(new Path(path))).isFalse();
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
                                                    .where(Expressions.lessThan(name, upper))
                                                    .build(),
                                    Record::toString))
                    .containsExactly("Record(" + expectedLower + ")");
            assertThat(
                            getIcebergResult(
                                    icebergTable ->
                                            IcebergGenerics.read(icebergTable)
                                                    .select(name)
                                                    .where(Expressions.greaterThan(name, lower))
                                                    .build(),
                                    Record::toString))
                    .containsExactly("Record(" + expectedUpper + ")");
            assertThat(
                            getIcebergResult(
                                    icebergTable ->
                                            IcebergGenerics.read(icebergTable)
                                                    .select(name)
                                                    .where(Expressions.lessThan(name, lower))
                                                    .build(),
                                    Record::toString))
                    .isEmpty();
            assertThat(
                            getIcebergResult(
                                    icebergTable ->
                                            IcebergGenerics.read(icebergTable)
                                                    .select(name)
                                                    .where(Expressions.greaterThan(name, upper))
                                                    .build(),
                                    Record::toString))
                    .isEmpty();
        }
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

    private List<String> getIcebergResult(
            Function<org.apache.iceberg.Table, CloseableIterable<Record>> query,
            Function<Record, String> icebergRecordToString)
            throws Exception {
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of("mydb.db", "t");
        org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(icebergIdentifier);
        CloseableIterable<Record> result = query.apply(icebergTable);
        List<String> actual = new ArrayList<>();
        for (Record record : result) {
            actual.add(icebergRecordToString.apply(record));
        }
        result.close();
        return actual;
    }
}
