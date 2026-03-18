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

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PartitionStatistics}. */
public class PartitionStatisticsTest {

    @Test
    void testLegacyPartitionStatisticsDeserialization() {
        String legacyPartitionStatisticsJson =
                "{\"spec\":{\"pt\":\"1\"},\"recordCount\":100,\"fileSizeInBytes\":1024,\"fileCount\":2,\"lastFileCreationTime\":123456789}";
        PartitionStatistics stats =
                JsonSerdeUtil.fromJson(legacyPartitionStatisticsJson, PartitionStatistics.class);

        assertThat(stats.spec()).containsEntry("pt", "1");
        assertThat(stats.recordCount()).isEqualTo(100);
        assertThat(stats.fileSizeInBytes()).isEqualTo(1024);
        assertThat(stats.fileCount()).isEqualTo(2);
        assertThat(stats.lastFileCreationTime()).isEqualTo(123456789L);
        assertThat(stats.totalBuckets()).isEqualTo(0);
    }
}
