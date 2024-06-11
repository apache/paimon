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
    public void testSerializerCompatible() throws Exception {
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
}
