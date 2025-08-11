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
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    public void testSplitMinMaxValue() {
        Map<Long, List<DataField>> schemas = new HashMap<>();

        Timestamp minTs = Timestamp.fromLocalDateTime(LocalDateTime.parse("2025-01-01T00:00:00"));
        Timestamp maxTs1 = Timestamp.fromLocalDateTime(LocalDateTime.parse("2025-03-01T00:00:00"));
        Timestamp maxTs2 = Timestamp.fromLocalDateTime(LocalDateTime.parse("2025-03-12T00:00:00"));
        BinaryRow min1 = newBinaryRow(new Object[] {10, 123L, 888.0D, minTs});
        BinaryRow max1 = newBinaryRow(new Object[] {99, 456L, 999.0D, maxTs1});
        SimpleStats valueStats1 = new SimpleStats(min1, max1, fromLongArray(new Long[] {0L}));

        BinaryRow min2 = newBinaryRow(new Object[] {5, 0L, 777.0D, minTs});
        BinaryRow max2 = newBinaryRow(new Object[] {90, 789L, 899.0D, maxTs2});
        SimpleStats valueStats2 = new SimpleStats(min2, max2, fromLongArray(new Long[] {0L}));

        // test the common case.
        DataFileMeta d1 = newDataFile(100, valueStats1, null);
        DataFileMeta d2 = newDataFile(100, valueStats2, null);
        DataSplit split1 = newDataSplit(true, Arrays.asList(d1, d2), null);

        DataField intField = new DataField(0, "c_int", new IntType());
        DataField longField = new DataField(1, "c_long", new BigIntType());
        DataField doubleField = new DataField(2, "c_double", new DoubleType());
        DataField tsField = new DataField(3, "c_ts", new TimestampType());
        schemas.put(1L, Arrays.asList(intField, longField, doubleField, tsField));

        SimpleStatsEvolutions evolutions = new SimpleStatsEvolutions(schemas::get, 1);
        assertThat(split1.minValue(0, intField, evolutions)).isEqualTo(5);
        assertThat(split1.maxValue(0, intField, evolutions)).isEqualTo(99);
        assertThat(split1.minValue(1, longField, evolutions)).isEqualTo(0L);
        assertThat(split1.maxValue(1, longField, evolutions)).isEqualTo(789L);
        assertThat(split1.minValue(2, doubleField, evolutions)).isEqualTo(777D);
        assertThat(split1.maxValue(2, doubleField, evolutions)).isEqualTo(999D);
        assertThat(split1.minValue(3, tsField, evolutions)).isEqualTo(minTs);
        assertThat(split1.maxValue(3, tsField, evolutions)).isEqualTo(maxTs2);

        // test the case which provide non-null valueStatsCol and there are different between file
        // schema and table schema.
        BinaryRow min3 = newBinaryRow(new Object[] {10, 123L, minTs});
        BinaryRow max3 = newBinaryRow(new Object[] {99, 456L, maxTs1});
        SimpleStats valueStats3 = new SimpleStats(min3, max3, fromLongArray(new Long[] {0L}));
        BinaryRow min4 = newBinaryRow(new Object[] {5, 0L, minTs});
        BinaryRow max4 = newBinaryRow(new Object[] {90, 789L, maxTs2});
        SimpleStats valueStats4 = new SimpleStats(min4, max4, fromLongArray(new Long[] {0L}));
        List<String> valueStatsCols2 = Arrays.asList("c_int", "c_long", "c_ts");
        DataFileMeta d3 = newDataFile(100, valueStats3, valueStatsCols2);
        DataFileMeta d4 = newDataFile(100, valueStats4, valueStatsCols2);
        DataSplit split2 = newDataSplit(true, Arrays.asList(d3, d4), null);

        DataField smallField = new DataField(4, "c_small", new SmallIntType());
        DataField floatField = new DataField(5, "c_float", new FloatType());
        schemas.put(2L, Arrays.asList(intField, smallField, tsField, floatField));

        evolutions = new SimpleStatsEvolutions(schemas::get, 2);
        assertThat(split2.minValue(0, intField, evolutions)).isEqualTo(5);
        assertThat(split2.maxValue(0, intField, evolutions)).isEqualTo(99);
        assertThat(split2.minValue(1, smallField, evolutions)).isEqualTo(null);
        assertThat(split2.maxValue(1, smallField, evolutions)).isEqualTo(null);
        assertThat(split2.minValue(2, tsField, evolutions)).isEqualTo(minTs);
        assertThat(split2.maxValue(2, tsField, evolutions)).isEqualTo(maxTs2);
        assertThat(split2.minValue(3, floatField, evolutions)).isEqualTo(null);
        assertThat(split2.maxValue(3, floatField, evolutions)).isEqualTo(null);
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

        assertThat(InstantiationUtil.clone(split)).isEqualTo(split);

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

        assertThat(InstantiationUtil.clone(split)).isEqualTo(split);

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
                        null,
                        null,
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

        assertThat(InstantiationUtil.clone(split)).isEqualTo(split);

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
                        null,
                        null,
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

        assertThat(InstantiationUtil.clone(split)).isEqualTo(split);

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

    @Test
    public void testSerializerCompatibleV5() throws Exception {
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
                        "hdfs:///path/to/warehouse",
                        null,
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

        assertThat(InstantiationUtil.clone(split)).isEqualTo(split);

        byte[] v5Bytes =
                IOUtils.readFully(
                        SplitTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/datasplit-v5"),
                        true);

        DataSplit actual =
                InstantiationUtil.deserializeObject(v5Bytes, DataSplit.class.getClassLoader());
        assertThat(actual).isEqualTo(split);
    }

    @Test
    public void testSerializerCompatibleV6() throws Exception {
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
                        "hdfs:///path/to/warehouse",
                        null,
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
                        .withTotalBuckets(32)
                        .withDataFiles(dataFiles)
                        .withDataDeletionFiles(deletionFiles)
                        .withBucketPath("my path")
                        .build();

        assertThat(InstantiationUtil.clone(split)).isEqualTo(split);

        byte[] v6Bytes =
                IOUtils.readFully(
                        SplitTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/datasplit-v6"),
                        true);

        DataSplit actual =
                InstantiationUtil.deserializeObject(v6Bytes, DataSplit.class.getClassLoader());
        assertThat(actual).isEqualTo(split);
    }

    @Test
    public void testSerializerCompatibleV7() throws Exception {
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
                        "hdfs:///path/to/warehouse",
                        12L,
                        Arrays.asList("a", "b", "c", "f"));
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
                        .withTotalBuckets(32)
                        .withDataFiles(dataFiles)
                        .withDataDeletionFiles(deletionFiles)
                        .withBucketPath("my path")
                        .build();

        assertThat(InstantiationUtil.clone(split)).isEqualTo(split);

        byte[] v6Bytes =
                IOUtils.readFully(
                        SplitTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/datasplit-v7"),
                        true);

        DataSplit actual =
                InstantiationUtil.deserializeObject(v6Bytes, DataSplit.class.getClassLoader());
        assertThat(actual).isEqualTo(split);
    }

    private DataFileMeta newDataFile(long rowCount) {
        return newDataFile(rowCount, null, null);
    }

    private DataFileMeta newDataFile(
            long rowCount, SimpleStats rowStats, @Nullable List<String> valueStatsCols) {
        return DataFileMeta.forAppend(
                "my_data_file.parquet",
                1024 * 1024,
                rowCount,
                rowStats,
                0L,
                rowCount - 1,
                1,
                Collections.emptyList(),
                null,
                null,
                valueStatsCols,
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

    private BinaryRow newBinaryRow(Object[] objs) {
        BinaryRow row = new BinaryRow(objs.length);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        for (int i = 0; i < objs.length; i++) {
            if (objs[i] instanceof Integer) {
                writer.writeInt(i, (Integer) objs[i]);
            } else if (objs[i] instanceof Long) {
                writer.writeLong(i, (Long) objs[i]);
            } else if (objs[i] instanceof Float) {
                writer.writeFloat(i, (Float) objs[i]);
            } else if (objs[i] instanceof Double) {
                writer.writeDouble(i, (Double) objs[i]);
            } else if (objs[i] instanceof Timestamp) {
                writer.writeTimestamp(i, (Timestamp) objs[i], 5);
            } else {
                throw new UnsupportedOperationException("It's not supported.");
            }
        }
        writer.complete();
        return row;
    }
}
