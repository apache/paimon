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

package org.apache.paimon.partition;

import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Partition} JSON serialization. */
class PartitionTest {

    @Test
    void testJsonSerializationWithNullValues() {
        Map<String, String> spec = Collections.singletonMap("pt", "1");
        Partition partition =
                new Partition(
                        spec,
                        100L, // recordCount
                        1024L, // fileSizeInBytes
                        2L, // fileCount
                        System.currentTimeMillis(), // lastFileCreationTime
                        10, // totalBuckets
                        false, // done
                        null, // createdAt
                        null, // createdBy
                        null, // updatedAt
                        null, // updatedBy
                        null); // options

        String json = JsonSerdeUtil.toFlatJson(partition);

        assertThat(json).doesNotContain("createdAt");
        assertThat(json).doesNotContain("createdBy");
        assertThat(json).doesNotContain("updatedAt");
        assertThat(json).doesNotContain("updatedBy");
        assertThat(json).doesNotContain("options");

        assertThat(json).contains("done");
        assertThat(json).contains("recordCount");
        assertThat(json).contains("totalBuckets");
    }

    @Test
    void testAbsentStatisticsAreUnknownNotZero() {
        // What listPartitions returns from a catalog that stores no statistics. Every consumer of
        // this response reads the numbers through PartitionStatistics.isKnown, so absence has to
        // arrive as unknown and not as an exact zero.
        String statisticsFreeJson = "{\"spec\":{\"pt\":\"1\"},\"done\":true}";

        Partition partition = JsonSerdeUtil.fromJson(statisticsFreeJson, Partition.class);

        assertThat(partition.spec()).containsEntry("pt", "1");
        assertThat(partition.done()).isTrue();
        assertThat(partition.recordCount()).isEqualTo(PartitionStatistics.UNKNOWN);
        assertThat(partition.fileSizeInBytes()).isEqualTo(PartitionStatistics.UNKNOWN);
        assertThat(partition.fileCount()).isEqualTo(PartitionStatistics.UNKNOWN);
        assertThat(partition.lastFileCreationTime()).isEqualTo(PartitionStatistics.UNKNOWN);
        assertThat(partition.createdAt()).isNull();
        assertThat(partition.options()).isNull();
    }

    @Test
    void testReportedZeroStaysAnExactMeasurement() {
        // The other half of the same boundary: a partition someone measured as empty must not come
        // back as unknown.
        String emptyPartitionJson =
                "{\"spec\":{\"pt\":\"1\"},\"recordCount\":0,\"fileSizeInBytes\":0,"
                        + "\"fileCount\":0,\"lastFileCreationTime\":0}";

        Partition partition = JsonSerdeUtil.fromJson(emptyPartitionJson, Partition.class);

        assertThat(partition.recordCount()).isEqualTo(0L);
        assertThat(partition.fileSizeInBytes()).isEqualTo(0L);
        assertThat(partition.fileCount()).isEqualTo(0L);
        assertThat(PartitionStatistics.isKnown(partition.recordCount())).isTrue();
    }

    @Test
    void testMeasurementsSurviveARoundTrip() {
        Partition partition =
                new Partition(
                        Collections.singletonMap("pt", "1"),
                        0L, // an empty partition someone did measure
                        1024L,
                        2L,
                        1234567890L,
                        10,
                        true,
                        1234567890L,
                        "user1",
                        1234567900L,
                        "user2",
                        Collections.singletonMap("key", "value"));

        Partition parsed =
                JsonSerdeUtil.fromJson(JsonSerdeUtil.toFlatJson(partition), Partition.class);

        assertThat(parsed).isEqualTo(partition);
        assertThat(parsed.recordCount()).isEqualTo(0L);
    }

    @Test
    void testJsonSerializationWithNonNullValues() {
        Map<String, String> spec = Collections.singletonMap("pt", "1");
        Partition partition =
                new Partition(
                        spec,
                        100L,
                        1024L,
                        2L,
                        System.currentTimeMillis(),
                        10, // totalBuckets
                        true,
                        1234567890L, // createdAt
                        "user1", // createdBy
                        1234567900L, // updatedAt
                        "user2", // updatedBy
                        Collections.singletonMap("key", "value")); // options

        String json = JsonSerdeUtil.toFlatJson(partition);

        assertThat(json).contains("totalBuckets");
        assertThat(json).contains("createdAt");
        assertThat(json).contains("createdBy");
        assertThat(json).contains("updatedAt");
        assertThat(json).contains("updatedBy");
        assertThat(json).contains("options");
    }
}
