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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestDataGenerator;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.data.BinaryArray.fromLongArray;
import static org.apache.paimon.data.BinaryRow.singleColumn;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DataSplit}. */
public class SplitTest {

    @Test
    public void testSplitMergedRowCount() {
        // not rawConvertible
        List<DataFileMeta> dataFiles =
                Arrays.asList(newDataFile(1000L), newDataFile(2000L), newDataFile(3000L));
        DataSplit split = newDataSplit(false, dataFiles, null);
        assertThat(split.partialMergedRowCount()).isEqualTo(0L);
        assertThat(split.mergedRowCountAvailable()).isEqualTo(false);

        // rawConvertible without deletion files
        split = newDataSplit(true, dataFiles, null);
        assertThat(split.partialMergedRowCount()).isEqualTo(6000L);
        assertThat(split.mergedRowCountAvailable()).isEqualTo(true);
        assertThat(split.mergedRowCount()).isEqualTo(6000L);

        // rawConvertible with deletion files without cardinality
        ArrayList<DeletionFile> deletionFiles = new ArrayList<>();
        deletionFiles.add(null);
        deletionFiles.add(new DeletionFile("p", 1, 2, null));
        deletionFiles.add(new DeletionFile("p", 1, 2, 100L));
        split = newDataSplit(true, dataFiles, deletionFiles);
        assertThat(split.partialMergedRowCount()).isEqualTo(3900L);
        assertThat(split.mergedRowCountAvailable()).isEqualTo(false);

        // rawConvertible with deletion files with cardinality
        deletionFiles = new ArrayList<>();
        deletionFiles.add(null);
        deletionFiles.add(new DeletionFile("p", 1, 2, 200L));
        deletionFiles.add(new DeletionFile("p", 1, 2, 100L));
        split = newDataSplit(true, dataFiles, deletionFiles);
        assertThat(split.partialMergedRowCount()).isEqualTo(5700L);
        assertThat(split.mergedRowCountAvailable()).isEqualTo(true);
        assertThat(split.mergedRowCount()).isEqualTo(5700L);
    }

    @Test
    public void testSerializer() throws IOException {
        DataFileTestDataGenerator gen = DataFileTestDataGenerator.builder().build();
        DataFileTestDataGenerator.Data data = gen.next();
        List<DataFileMeta> files = new ArrayList<>();
        for (int i = 0; i < ThreadLocalRandom.current().nextInt(10); i++) {
            files.add(gen.next().meta);
        }
        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(ThreadLocalRandom.current().nextLong(100))
                        .withPartition(data.partition)
                        .withBucket(data.bucket)
                        .withDataFiles(files)
                        .withBucketPath("my path")
                        .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        split.serialize(new DataOutputViewStreamWrapper(out));

        DataSplit newSplit = DataSplit.deserialize(new DataInputDeserializer(out.toByteArray()));
        assertThat(newSplit).isEqualTo(split);
    }

    @Test
    public void testSerializerNormal() throws Exception {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));

        DataFileMeta dataFile =
                new DataFileMeta(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        Arrays.asList("field1", "field2", "field3"),
                        "hdfs:///path/to/warehouse");
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        DeletionFile deletionFile = new DeletionFile("deletion_file", 100, 22, 33L);
        List<DeletionFile> deletionFiles = Collections.singletonList(deletionFile);

        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(partition);
        binaryRowWriter.writeString(0, BinaryString.fromString("aaaaa"));
        binaryRowWriter.complete();

        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(18)
                        .withPartition(partition)
                        .withBucket(20)
                        .withDataFiles(dataFiles)
                        .withDataDeletionFiles(deletionFiles)
                        .withBucketPath("my path")
                        .build();

        assertThat(InstantiationUtil.clone(split)).isEqualTo(split);
    }

    @Test
    public void testSerializerCompatibleV1() throws Exception {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));

        DataFileMeta dataFile =
                new DataFileMeta(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        null,
                        null,
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(partition);
        binaryRowWriter.writeString(0, BinaryString.fromString("aaaaa"));
        binaryRowWriter.complete();

        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(18)
                        .withPartition(partition)
                        .withBucket(20)
                        .withDataFiles(dataFiles)
                        .withBucketPath("my path")
                        .build();

        byte[] v2Bytes =
                IOUtils.readFully(
                        SplitTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/datasplit-v1"),
                        true);

        DataSplit actual =
                InstantiationUtil.deserializeObject(v2Bytes, DataSplit.class.getClassLoader());
        assertThat(actual).isEqualTo(split);
    }

    @Test
    public void testSerializerCompatibleV2() throws Exception {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));

        DataFileMeta dataFile =
                new DataFileMeta(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        null,
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(partition);
        binaryRowWriter.writeString(0, BinaryString.fromString("aaaaa"));
        binaryRowWriter.complete();

        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(18)
                        .withPartition(partition)
                        .withBucket(20)
                        .withDataFiles(dataFiles)
                        .withBucketPath("my path")
                        .build();

        byte[] v2Bytes =
                IOUtils.readFully(
                        SplitTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/datasplit-v2"),
                        true);

        DataSplit actual =
                InstantiationUtil.deserializeObject(v2Bytes, DataSplit.class.getClassLoader());
        assertThat(actual).isEqualTo(split);
    }

    @Test
    public void testSerializerCompatibleV3() throws Exception {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));

        DataFileMeta dataFile =
                new DataFileMeta(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        Arrays.asList("field1", "field2", "field3"),
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        DeletionFile deletionFile = new DeletionFile("deletion_file", 100, 22, null);
        List<DeletionFile> deletionFiles = Collections.singletonList(deletionFile);

        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(partition);
        binaryRowWriter.writeString(0, BinaryString.fromString("aaaaa"));
        binaryRowWriter.complete();

        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(18)
                        .withPartition(partition)
                        .withBucket(20)
                        .withDataFiles(dataFiles)
                        .withDataDeletionFiles(deletionFiles)
                        .withBucketPath("my path")
                        .build();

        byte[] v2Bytes =
                IOUtils.readFully(
                        SplitTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/datasplit-v3"),
                        true);

        DataSplit actual =
                InstantiationUtil.deserializeObject(v2Bytes, DataSplit.class.getClassLoader());
        assertThat(actual).isEqualTo(split);
    }

    @Test
    public void testSerializerCompatibleV4() throws Exception {
        SimpleStats keyStats =
                new SimpleStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        SimpleStats valueStats =
                new SimpleStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));

        DataFileMeta dataFile =
                new DataFileMeta(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4},
                        FileSource.COMPACT,
                        Arrays.asList("field1", "field2", "field3"),
                        null);
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        DeletionFile deletionFile = new DeletionFile("deletion_file", 100, 22, 33L);
        List<DeletionFile> deletionFiles = Collections.singletonList(deletionFile);

        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(partition);
        binaryRowWriter.writeString(0, BinaryString.fromString("aaaaa"));
        binaryRowWriter.complete();

        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(18)
                        .withPartition(partition)
                        .withBucket(20)
                        .withDataFiles(dataFiles)
                        .withDataDeletionFiles(deletionFiles)
                        .withBucketPath("my path")
                        .build();

        byte[] v4Bytes =
                IOUtils.readFully(
                        SplitTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/datasplit-v4"),
                        true);

        DataSplit actual =
                InstantiationUtil.deserializeObject(v4Bytes, DataSplit.class.getClassLoader());
        assertThat(actual).isEqualTo(split);
    }

    private DataFileMeta newDataFile(long rowCount) {
        return DataFileMeta.forAppend(
                "my_data_file.parquet",
                1024 * 1024,
                rowCount,
                null,
                0L,
                rowCount,
                1,
                Collections.emptyList(),
                null,
                null,
                null);
    }

    private DataSplit newDataSplit(
            boolean rawConvertible,
            List<DataFileMeta> dataFiles,
            List<DeletionFile> deletionFiles) {
        DataSplit.Builder builder = DataSplit.builder();
        builder.withSnapshot(1)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(1)
                .withBucketPath("my path")
                .rawConvertible(rawConvertible)
                .withDataFiles(dataFiles);
        if (deletionFiles != null) {
            builder.withDataDeletionFiles(deletionFiles);
        }
        return builder.build();
    }
}
