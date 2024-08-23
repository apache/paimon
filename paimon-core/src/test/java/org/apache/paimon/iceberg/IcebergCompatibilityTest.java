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
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
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
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

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

        IcebergPathFactory pathFactory = new IcebergPathFactory(table.location());
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
    public void testPartitionedPrimaryKeyTableTimestamp() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.TIMESTAMP(6),
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"pt", "k", "v1", "v2"});

        List<TestRecord> testRecords = new ArrayList<>();
        Function<Timestamp, BinaryRow> binaryRow =
                (pt) -> {
                    BinaryRow b = new BinaryRow(2);
                    BinaryRowWriter writer = new BinaryRowWriter(b);
                    writer.writeTimestamp(0, pt, 6);
                    writer.complete();
                    return b;
                };

        int numRecords = 1000;
        int[] precisions = {3, 6};
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < numRecords; i++) {
            Timestamp pt =
                    generateRandomTimestamp(random, precisions[random.nextInt(precisions.length)]);
            String k = String.valueOf(random.nextInt(0, 100));
            int v1 = random.nextInt();
            long v2 = random.nextLong();
            testRecords.add(
                    new TestRecord(
                            binaryRow.apply(pt),
                            String.format("%s|%s", pt.toInstant().atOffset(ZoneOffset.UTC), k),
                            String.format("%d|%d", v1, v2),
                            GenericRow.of(pt, BinaryString.fromString(k), v1, v2)));
        }

        runCompatibilityTest(
                rowType,
                Arrays.asList("pt"),
                Arrays.asList("pt", "k"),
                testRecords,
                r -> String.format("%s|%s", r.get(0), r.get(1)),
                r -> String.format("%d|%d", r.get(2, Integer.class), r.get(3, Long.class)));
    }

    private Timestamp generateRandomTimestamp(ThreadLocalRandom random, int precision) {
        long milliseconds = random.nextLong(0, 10_000_000_000_000L);
        int nanoAdjustment;
        switch (precision) {
            case 3:
                nanoAdjustment = 0;
                break;
            case 6:
                nanoAdjustment = random.nextInt(0, 1_000) * 1000;
                break;
            default:
                throw new IllegalArgumentException("Unsupported precision: " + precision);
        }

        return Timestamp.fromEpochMillis(milliseconds, nanoAdjustment);
    }

    @Test
    public void testAppendOnlyTableWithAllTypes() throws Exception {
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
                            DataTypes.DATE()
                        },
                        new String[] {
                            "pt",
                            "v_boolean",
                            "v_bigint",
                            "v_float",
                            "v_double",
                            "v_decimal",
                            "v_char",
                            "v_varchar",
                            "v_binary",
                            "v_varbinary",
                            "v_date"
                        });

        Function<Integer, BinaryRow> binaryRow =
                (pt) -> {
                    BinaryRow b = new BinaryRow(1);
                    BinaryRowWriter writer = new BinaryRowWriter(b);
                    writer.writeInt(0, pt);
                    writer.complete();
                    return b;
                };

        int numRounds = 5;
        int numRecords = 500;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<List<TestRecord>> testRecords = new ArrayList<>();
        List<List<String>> expected = new ArrayList<>();
        List<String> currentExpected = new ArrayList<>();
        for (int r = 0; r < numRounds; r++) {
            List<TestRecord> round = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                int pt = random.nextInt(0, 2);
                Boolean vBoolean = random.nextBoolean() ? random.nextBoolean() : null;
                Long vBigInt = random.nextBoolean() ? random.nextLong() : null;
                Float vFloat = random.nextBoolean() ? random.nextFloat() : null;
                Double vDouble = random.nextBoolean() ? random.nextDouble() : null;
                Decimal vDecimal =
                        random.nextBoolean()
                                ? Decimal.fromUnscaledLong(random.nextLong(0, 100000000), 8, 3)
                                : null;
                String vChar = random.nextBoolean() ? String.valueOf(random.nextInt()) : null;
                String vVarChar = random.nextBoolean() ? String.valueOf(random.nextInt()) : null;
                byte[] vBinary =
                        random.nextBoolean() ? String.valueOf(random.nextInt()).getBytes() : null;
                byte[] vVarBinary =
                        random.nextBoolean() ? String.valueOf(random.nextInt()).getBytes() : null;
                Integer vDate = random.nextBoolean() ? random.nextInt(0, 30000) : null;

                round.add(
                        new TestRecord(
                                binaryRow.apply(pt),
                                GenericRow.of(
                                        pt,
                                        vBoolean,
                                        vBigInt,
                                        vFloat,
                                        vDouble,
                                        vDecimal,
                                        BinaryString.fromString(vChar),
                                        BinaryString.fromString(vVarChar),
                                        vBinary,
                                        vVarBinary,
                                        vDate)));
                currentExpected.add(
                        String.format(
                                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s",
                                pt,
                                vBoolean,
                                vBigInt,
                                vFloat,
                                vDouble,
                                vDecimal,
                                vChar,
                                vVarChar,
                                vBinary == null ? null : new String(vBinary),
                                vVarBinary == null ? null : new String(vVarBinary),
                                vDate == null ? null : LocalDate.ofEpochDay(vDate)));
            }
            testRecords.add(round);
            expected.add(new ArrayList<>(currentExpected));
        }

        runCompatibilityTest(
                rowType,
                Collections.emptyList(),
                Collections.emptyList(),
                testRecords,
                expected,
                r ->
                        IntStream.range(0, rowType.getFieldCount())
                                .mapToObj(
                                        i -> {
                                            Object field = r.get(i);
                                            if (field instanceof ByteBuffer) {
                                                return new String(((ByteBuffer) field).array());
                                            } else {
                                                return String.valueOf(field);
                                            }
                                        })
                                .collect(Collectors.joining(", ")));
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

            assertThat(getIcebergResult(icebergRecordToString)).hasSameElementsAs(expected.get(r));
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
        options.set(CoreOptions.METADATA_ICEBERG_COMPATIBLE, true);
        options.set(CoreOptions.FILE_FORMAT, "avro");
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));
        options.set(AbstractIcebergCommitCallback.COMPACT_MIN_FILE_NUM, 4);
        options.set(AbstractIcebergCommitCallback.COMPACT_MIN_FILE_NUM, 8);
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
        return getIcebergResult(Record::toString);
    }

    private List<String> getIcebergResult(Function<Record, String> icebergRecordToString)
            throws Exception {
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of("mydb.db", "t");
        org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(icebergIdentifier);
        CloseableIterable<Record> result = IcebergGenerics.read(icebergTable).build();
        List<String> actual = new ArrayList<>();
        for (Record record : result) {
            actual.add(icebergRecordToString.apply(record));
        }
        result.close();
        return actual;
    }
}
