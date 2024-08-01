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
import org.apache.paimon.data.*;
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
import org.apache.paimon.utils.DateTimeUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/** Tests for Iceberg compatibility. */
public class IcebergDataTypeCompatibilityTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testPartitionedPrimaryKeyTable_Timestamp() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.TIMESTAMP(),
                            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"pt1", "pt2", "k", "v1", "v2"});

        BiFunction<Timestamp, Timestamp, BinaryRow> binaryRow =
                (pt1, pt2) -> {
                    BinaryRow b = new BinaryRow(2);
                    BinaryRowWriter writer = new BinaryRowWriter(b);
                    writer.writeTimestamp(0, pt1, 6);
                    writer.writeTimestamp(1, pt2, 6);
                    writer.complete();
                    return b;
                };

        int numRecords = 1000;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<TestRecord> testRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            Timestamp pt1 = Timestamp.fromEpochMillis(random.nextInt(0, 99999));
            Timestamp pt2 =
                    DateTimeUtils.timestampToTimestampWithLocalZone(pt1, DateTimeUtils.UTC_ZONE);
            String k = String.valueOf(random.nextInt(0, 100));
            int v1 = random.nextInt();
            long v2 = random.nextLong();
            testRecords.add(
                    new TestRecord(
                            binaryRow.apply(pt1, pt2),
                            String.format("%s|%s|%s", pt1, pt2, k),
                            String.format("%d|%d", v1, v2),
                            GenericRow.of(pt1, pt2, BinaryString.fromString(k), v1, v2)));
        }

        runCompatibilityTest(
                rowType,
                Arrays.asList("pt1", "pt2"),
                Arrays.asList("pt1", "pt2", "k"),
                testRecords);
    }

    @Test
    public void testPartitionedPrimaryKeyTable_Time() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.TIMESTAMP(),
                            DataTypes.TIME(),
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"pt1", "pt2", "k", "v1", "v2"});

        BiFunction<Timestamp, LocalTime, BinaryRow> binaryRow =
                (pt1, pt2) -> {
                    BinaryRow b = new BinaryRow(2);
                    BinaryRowWriter writer = new BinaryRowWriter(b);
                    writer.writeTimestamp(0, pt1, 6);
                    writer.writeInt(1, pt2.getNano());
                    writer.complete();
                    return b;
                };

        int numRecords = 1000;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<TestRecord> testRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            Timestamp pt1 = Timestamp.fromEpochMillis(random.nextInt(0, 99999));
            LocalTime pt2 = LocalTime.ofNanoOfDay(LocalTime.now().getNano() + random.nextInt(1000));
            String k = String.valueOf(random.nextInt(0, 100));
            int v1 = random.nextInt();
            long v2 = random.nextLong();
            testRecords.add(
                    new TestRecord(
                            binaryRow.apply(pt1, pt2),
                            String.format("%s|%s|%s", pt1.getMillisecond(), pt2.getNano(), k),
                            String.format("%d|%d", v1, v2),
                            GenericRow.of(pt1, pt2.getNano(), BinaryString.fromString(k), v1, v2)));
        }

        runCompatibilityTest(
                rowType,
                Arrays.asList("pt1", "pt2"),
                Arrays.asList("pt1", "pt2", "k"),
                testRecords);
    }

    private void runCompatibilityTest(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<TestRecord> testRecords)
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
