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

package org.apache.paimon.flink.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.StatsTestUtils;
import org.apache.paimon.table.source.DataSplit;

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link FileStoreSourceSplitSerializer}. */
public class FileStoreSourceSplitSerializerTest {

    @Test
    public void serializeSplit() throws Exception {
        final FileStoreSourceSplit split =
                newSourceSplit("id", row(1), 2, Arrays.asList(newFile(0), newFile(1)));

        final FileStoreSourceSplit deSerialized = serializeAndDeserialize(split);

        assertSplitsEqual(split, deSerialized);
    }

    @Test
    public void serializeSplitWithReaderPosition() throws Exception {
        final FileStoreSourceSplit split =
                newSourceSplit("id", row(1), 2, Arrays.asList(newFile(0), newFile(1)), 29);

        final FileStoreSourceSplit deSerialized = serializeAndDeserialize(split);

        assertSplitsEqual(split, deSerialized);
    }

    @Test
    public void repeatedSerialization() throws Exception {
        final FileStoreSourceSplit split =
                newSourceSplit("id", row(1), 2, Arrays.asList(newFile(0), newFile(1)), 29);

        serializeAndDeserialize(split);
        serializeAndDeserialize(split);
        final FileStoreSourceSplit deSerialized = serializeAndDeserialize(split);

        assertSplitsEqual(split, deSerialized);
    }

    // ------------------------------------------------------------------------
    //  test utils
    // ------------------------------------------------------------------------

    public static DataFileMeta newFile(int level) {
        return new DataFileMeta(
                "",
                0,
                1,
                row(0),
                row(0),
                StatsTestUtils.newEmptyTableStats(),
                StatsTestUtils.newEmptyTableStats(),
                0,
                1,
                0,
                level,
                0L,
                null);
    }

    public static FileStoreSourceSplit newSourceSplit(
            String id, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        return newSourceSplit(id, partition, bucket, files, false, 0);
    }

    public static FileStoreSourceSplit newSourceSplit(
            String id,
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            boolean isIncremental) {
        return newSourceSplit(id, partition, bucket, files, isIncremental, 0);
    }

    public static FileStoreSourceSplit newSourceSplit(
            String id,
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            long recordsToSkip) {
        return newSourceSplit(id, partition, bucket, files, false, recordsToSkip);
    }

    public static FileStoreSourceSplit newSourceSplit(
            String id,
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            boolean isIncremental,
            long recordsToSkip) {
        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(partition)
                        .withBucket(bucket)
                        .withDataFiles(files)
                        .isStreaming(isIncremental)
                        .rawConvertible(false)
                        .withBucketPath("/temp/" + bucket) // no used
                        .withDefaultFormat("orc")
                        .build();
        return new FileStoreSourceSplit(id, split, recordsToSkip);
    }

    private static FileStoreSourceSplit serializeAndDeserialize(FileStoreSourceSplit split)
            throws IOException {
        final FileStoreSourceSplitSerializer serializer = new FileStoreSourceSplitSerializer();
        final byte[] bytes =
                SimpleVersionedSerialization.writeVersionAndSerialize(serializer, split);
        return SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);
    }

    static void assertSplitsEqual(FileStoreSourceSplit expected, FileStoreSourceSplit actual) {
        assertThat(actual).isEqualTo(expected);
    }
}
