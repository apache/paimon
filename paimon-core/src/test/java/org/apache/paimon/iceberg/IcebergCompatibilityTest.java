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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Iceberg compatibility. */
public class IcebergCompatibilityTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testUnartitionedPrimaryKeyTable() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(), DataTypes.STRING(), DataTypes.INT(), DataTypes.BIGINT()
                        },
                        new String[] {"k1", "k2", "v1", "v2"});

        int numRecords = 1000;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<TestRecord> testRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            int k1 = random.nextInt(0, 100);
            String k2 = String.valueOf(random.nextInt(1000, 1010));
            int v1 = random.nextInt();
            long v2 = random.nextLong();
            testRecords.add(
                    new TestRecord(
                            BinaryRow.EMPTY_ROW,
                            String.format("%d|%s", k1, k2),
                            String.format("%d|%d", v1, v2),
                            GenericRow.of(k1, BinaryString.fromString(k2), v1, v2)));
        }

        runCompatibilityTest(
                rowType,
                Collections.emptyList(),
                Arrays.asList("k1", "k2"),
                testRecords,
                r -> String.format("%d|%s", r.get(0, Integer.class), r.get(1, String.class)),
                r -> String.format("%d|%d", r.get(2, Integer.class), r.get(3, Long.class)));
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

        int numRecords = 1000;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<TestRecord> testRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            int pt1 = random.nextInt(0, 2);
            String pt2 = String.valueOf(random.nextInt(10, 12));
            String k = String.valueOf(random.nextInt(0, 100));
            int v1 = random.nextInt();
            long v2 = random.nextLong();
            testRecords.add(
                    new TestRecord(
                            binaryRow.apply(pt1, pt2),
                            String.format("%d|%s|%s", pt1, pt2, k),
                            String.format("%d|%d", v1, v2),
                            GenericRow.of(
                                    pt1,
                                    BinaryString.fromString(pt2),
                                    BinaryString.fromString(k),
                                    v1,
                                    v2)));
        }

        runCompatibilityTest(
                rowType,
                Arrays.asList("pt1", "pt2"),
                Arrays.asList("pt1", "pt2", "k"),
                testRecords,
                r ->
                        String.format(
                                "%d|%s|%s",
                                r.get(0, Integer.class),
                                r.get(1, String.class),
                                r.get(2, String.class)),
                r -> String.format("%d|%d", r.get(3, Integer.class), r.get(4, Long.class)));
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

        int numRecords = 1000;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<TestRecord> testRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            int pt = random.nextInt(0, 2);
            boolean vBoolean = random.nextBoolean();
            long vBigInt = random.nextLong();
            float vFloat = random.nextFloat();
            double vDouble = random.nextDouble();
            Decimal vDecimal = Decimal.fromUnscaledLong(random.nextLong(0, 100000000), 8, 3);
            String vChar = String.valueOf(random.nextInt());
            String vVarChar = String.valueOf(random.nextInt());
            byte[] vBinary = String.valueOf(random.nextInt()).getBytes();
            byte[] vVarBinary = String.valueOf(random.nextInt()).getBytes();
            int vDate = random.nextInt(0, 30000);

            String k =
                    String.format(
                            "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s",
                            pt,
                            vBoolean,
                            vBigInt,
                            vFloat,
                            vDouble,
                            vDecimal,
                            vChar,
                            vVarChar,
                            new String(vBinary),
                            new String(vVarBinary),
                            LocalDate.ofEpochDay(vDate));
            testRecords.add(
                    new TestRecord(
                            binaryRow.apply(pt),
                            k,
                            "",
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
        }

        runCompatibilityTest(
                rowType,
                Collections.emptyList(),
                Collections.emptyList(),
                testRecords,
                r ->
                        String.format(
                                "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s",
                                r.get(0),
                                r.get(1),
                                r.get(2),
                                r.get(3),
                                r.get(4),
                                r.get(5),
                                r.get(6),
                                r.get(7),
                                new String(r.get(8, ByteBuffer.class).array()),
                                new String(r.get(9, ByteBuffer.class).array()),
                                r.get(10)),
                r -> "");
    }

    private void runCompatibilityTest(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<TestRecord> testRecords,
            Function<Record, String> icebergRecordToKey,
            Function<Record, String> icebergRecordToValue)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        Options options = new Options();
        if (!primaryKeys.isEmpty()) {
            options.set(CoreOptions.BUCKET, 2);
        }
        options.set(CoreOptions.METADATA_ICEBERG_COMPATIBLE, true);
        options.set(CoreOptions.FILE_FORMAT, "avro");
        Schema schema =
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, options.toMap(), "");

        FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path);
        paimonCatalog.createDatabase("mydb", false);
        Identifier paimonIdentifier = Identifier.create("mydb", "t");
        paimonCatalog.createTable(paimonIdentifier, schema, false);
        FileStoreTable table = (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        Map<String, String> expected = new HashMap<>();
        for (TestRecord testRecord : testRecords) {
            expected.put(testRecord.key, testRecord.value);
            write.write(testRecord.record);
        }

        if (!primaryKeys.isEmpty()) {
            for (BinaryRow partition :
                    testRecords.stream().map(t -> t.partition).collect(Collectors.toSet())) {
                for (int b = 0; b < 2; b++) {
                    write.compact(partition, b, true);
                }
            }
        }
        commit.commit(1, write.prepareCommit(true, 1));
        write.close();
        commit.close();

        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of("mydb.db", "t");
        org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(icebergIdentifier);
        CloseableIterable<Record> result = IcebergGenerics.read(icebergTable).build();
        Map<String, String> actual = new HashMap<>();
        for (Record record : result) {
            actual.put(icebergRecordToKey.apply(record), icebergRecordToValue.apply(record));
        }
        result.close();

        assertThat(actual).isEqualTo(expected);
    }

    private static class TestRecord {
        private final BinaryRow partition;
        private final String key;
        private final String value;
        private final GenericRow record;

        private TestRecord(BinaryRow partition, String key, String value, GenericRow record) {
            this.partition = partition;
            this.key = key;
            this.value = value;
            this.record = record;
        }
    }
}
