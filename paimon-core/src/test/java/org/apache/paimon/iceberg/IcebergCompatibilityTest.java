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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

        int numRounds = 5;
        int numRecords = 500;
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

        int numRounds = 2;
        int numRecords = 3;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<List<TestRecord>> testRecords = new ArrayList<>();
        List<List<String>> expected = new ArrayList<>();
        Map<String, String> expectedMap = new LinkedHashMap<>();
        for (int r = 0; r < numRounds; r++) {
            List<TestRecord> round = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                int pt1 = random.nextInt(0, 2);
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
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        Options options = new Options();
        options.set(CoreOptions.BUCKET, numBuckets);
        options.set(CoreOptions.METADATA_ICEBERG_COMPATIBLE, true);
        options.set(CoreOptions.FILE_FORMAT, "avro");
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
