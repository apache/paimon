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

package org.apache.paimon.globalindex;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestUtils;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link IndexedSplit}. */
public class IndexedSplitTest {

    @Test
    public void testSerializeAndDeserializeWithoutScores() throws IOException {
        // Create test DataSplit
        DataFileMeta file1 = DataFileTestUtils.newFile("file1", 0, 1, 100, 1000L);
        DataFileMeta file2 = DataFileTestUtils.newFile("file2", 0, 101, 200, 2000L);

        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Arrays.asList(file1, file2))
                        .build();

        // Create test ranges
        List<Range> rowRanges = Arrays.asList(new Range(0, 100), new Range(200, 300));

        // Create IndexedSplit without scores
        IndexedSplit split = new IndexedSplit(dataSplit, rowRanges, null);

        // Test custom serialization
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(baos);
        split.serialize(outputView);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(bais);
        IndexedSplit deserialized = IndexedSplit.deserialize(inputView);

        assertThat(deserialized).isEqualTo(split);
    }

    @Test
    public void testSerializeAndDeserializeWithScores() throws IOException {
        // Create test DataSplit
        DataFileMeta file = DataFileTestUtils.newFile("file1", 0, 1, 100, 1000L);

        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(2L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(1)
                        .withBucketPath("bucket-1")
                        .withDataFiles(Collections.singletonList(file))
                        .build();

        // Create test ranges and scores
        List<Range> rowRanges = Arrays.asList(new Range(0, 50), new Range(100, 150));
        float[] scores = new float[] {0.8f, 0.9f};

        // Create IndexedSplit with scores
        IndexedSplit split = new IndexedSplit(dataSplit, rowRanges, scores);

        // Test custom serialization
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(baos);
        split.serialize(outputView);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(bais);
        IndexedSplit deserialized = IndexedSplit.deserialize(inputView);

        assertThat(deserialized).isEqualTo(split);
    }

    @Test
    public void testJavaSerializationWithoutScores() throws IOException, ClassNotFoundException {
        // Create test DataSplit
        DataFileMeta file = DataFileTestUtils.newFile("file1", 0, 1, 100, 1000L);

        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(3L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(2)
                        .withBucketPath("bucket-2")
                        .withDataFiles(Collections.singletonList(file))
                        .build();

        List<Range> rowRanges = Collections.singletonList(new Range(10, 20));

        IndexedSplit split = new IndexedSplit(dataSplit, rowRanges, null);

        // Test Java serialization
        byte[] serialized = InstantiationUtil.serializeObject(split);
        IndexedSplit deserialized =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());

        // Verify
        assertThat(deserialized).isEqualTo(split);
    }

    @Test
    public void testJavaSerializationWithScores() throws IOException, ClassNotFoundException {
        // Create test DataSplit
        DataFileMeta file1 = DataFileTestUtils.newFile("file1", 0, 1, 100, 1000L);
        DataFileMeta file2 = DataFileTestUtils.newFile("file2", 0, 101, 200, 2000L);

        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(4L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(3)
                        .withBucketPath("bucket-3")
                        .withDataFiles(Arrays.asList(file1, file2))
                        .build();

        List<Range> rowRanges =
                Arrays.asList(new Range(5, 10), new Range(20, 30), new Range(100, 200));
        float[] scores = new float[] {0.5f, 0.7f, 0.9f};

        IndexedSplit split = new IndexedSplit(dataSplit, rowRanges, scores);

        // Test Java serialization
        byte[] serialized = InstantiationUtil.serializeObject(split);
        IndexedSplit deserialized =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());

        // Verify
        assertThat(deserialized).isEqualTo(split);
    }

    @Test
    public void testRowCount() {
        DataFileMeta file = DataFileTestUtils.newFile("file1", 0, 1, 100, 1000L);

        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(file))
                        .build();

        // Test with single range
        IndexedSplit split1 =
                new IndexedSplit(dataSplit, Collections.singletonList(new Range(0, 99)), null);
        assertThat(split1.rowCount()).isEqualTo(100);

        // Test with multiple ranges
        List<Range> ranges = Arrays.asList(new Range(0, 9), new Range(20, 29), new Range(50, 59));
        IndexedSplit split2 = new IndexedSplit(dataSplit, ranges, null);
        assertThat(split2.rowCount()).isEqualTo(30); // 10 + 10 + 10

        // Test with single-element range
        IndexedSplit split3 =
                new IndexedSplit(dataSplit, Collections.singletonList(new Range(5, 5)), null);
        assertThat(split3.rowCount()).isEqualTo(1);
    }

    @Test
    public void testEmptyRanges() throws IOException {
        DataFileMeta file = DataFileTestUtils.newFile("file1", 0, 1, 100, 1000L);

        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(file))
                        .build();

        // Create IndexedSplit with empty ranges
        IndexedSplit split = new IndexedSplit(dataSplit, Collections.emptyList(), null);

        // Test serialization
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(baos);
        split.serialize(outputView);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(bais);
        IndexedSplit deserialized = IndexedSplit.deserialize(inputView);

        // Verify
        assertThat(deserialized.rowRanges()).isEmpty();
        assertThat(deserialized.rowCount()).isEqualTo(0);
    }

    @Test
    public void testCorruptedMagicNumber() throws IOException {
        DataFileMeta file = DataFileTestUtils.newFile("file1", 0, 1, 100, 1000L);

        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(file))
                        .build();

        IndexedSplit split =
                new IndexedSplit(dataSplit, Collections.singletonList(new Range(0, 10)), null);

        // Serialize and corrupt the magic number
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(baos);
        split.serialize(outputView);

        byte[] data = baos.toByteArray();
        // Corrupt the magic number (first 8 bytes)
        data[0] = (byte) 0xFF;

        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(bais);

        // Verify that deserialization throws exception
        assertThatThrownBy(() -> IndexedSplit.deserialize(inputView))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Corrupted IndexedSplit: wrong magic number");
    }

    @Test
    public void testLargeRanges() throws IOException {
        DataFileMeta file = DataFileTestUtils.newFile("file1", 0, 1, 1000000, 1000L);

        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(file))
                        .build();

        // Create large ranges
        List<Range> rowRanges =
                Arrays.asList(
                        new Range(0, 1000000),
                        new Range(2000000, 3000000),
                        new Range(5000000, 10000000));
        float[] scores = new float[] {0.1f, 0.5f, 0.9f};

        IndexedSplit split = new IndexedSplit(dataSplit, rowRanges, scores);

        // Test serialization
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(baos);
        split.serialize(outputView);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(bais);
        IndexedSplit deserialized = IndexedSplit.deserialize(inputView);

        // Verify
        assertThat(deserialized.rowRanges()).hasSize(3);
        assertThat(deserialized.rowRanges().get(0)).isEqualTo(new Range(0, 1000000));
        assertThat(deserialized.rowRanges().get(1)).isEqualTo(new Range(2000000, 3000000));
        assertThat(deserialized.rowRanges().get(2)).isEqualTo(new Range(5000000, 10000000));
        assertThat(deserialized.scores()).hasSize(3);
        assertThat(deserialized.rowCount())
                .isEqualTo(7000003); // (1000000-0+1) + (3000000-2000000+1) + (10000000-5000000+1)
    }
}
