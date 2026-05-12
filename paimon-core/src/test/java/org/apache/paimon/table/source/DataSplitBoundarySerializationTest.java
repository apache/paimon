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

import org.apache.paimon.format.FileSplitBoundary;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.manifest.FileSource;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests that {@link DataSplit#fileSplitBoundaries()} survives serialize/deserialize. */
public class DataSplitBoundarySerializationTest {

    @Test
    public void testRoundTripWithBoundaries() throws Exception {
        Map<Integer, List<FileSplitBoundary>> boundaries = new HashMap<>();
        boundaries.put(
                0,
                Arrays.asList(
                        new FileSplitBoundary(0, 512, 1000),
                        new FileSplitBoundary(512, 256, 500)));

        DataSplit original =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("file:/tmp")
                        .withTotalBuckets(1)
                        .withDataFiles(Collections.singletonList(newDataFile()))
                        .rawConvertible(true)
                        .isStreaming(false)
                        .withFileSplitBoundaries(boundaries)
                        .build();

        DataSplit roundTripped = serializeDeserialize(original);

        assertThat(roundTripped.fileSplitBoundaries()).isNotNull();
        List<FileSplitBoundary> deserialized = roundTripped.fileSplitBoundaries().get(0);
        assertThat(deserialized).hasSize(2);
        assertThat(deserialized.get(0).offset()).isEqualTo(0);
        assertThat(deserialized.get(0).length()).isEqualTo(512);
        assertThat(deserialized.get(0).rowCount()).isEqualTo(1000);
        assertThat(deserialized.get(1).offset()).isEqualTo(512);
        assertThat(deserialized.get(1).length()).isEqualTo(256);
        assertThat(deserialized.get(1).rowCount()).isEqualTo(500);
    }

    @Test
    public void testRoundTripWithoutBoundaries() throws Exception {
        DataSplit original =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("file:/tmp")
                        .withTotalBuckets(1)
                        .withDataFiles(Collections.singletonList(newDataFile()))
                        .rawConvertible(true)
                        .isStreaming(false)
                        .build();

        DataSplit roundTripped = serializeDeserialize(original);

        assertThat(roundTripped.fileSplitBoundaries()).isNull();
    }

    private static DataSplit serializeDeserialize(DataSplit split) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        split.serialize(new DataOutputViewStreamWrapper(out));
        return DataSplit.deserialize(new DataInputDeserializer(out.toByteArray()));
    }

    private static DataFileMeta newDataFile() {
        return DataFileMeta.create(
                "data.parquet",
                1024,
                100L,
                EMPTY_ROW,
                EMPTY_ROW,
                null,
                null,
                0,
                0,
                0,
                0,
                null,
                null,
                null,
                null,
                FileSource.APPEND,
                null,
                null,
                null,
                null);
    }
}
