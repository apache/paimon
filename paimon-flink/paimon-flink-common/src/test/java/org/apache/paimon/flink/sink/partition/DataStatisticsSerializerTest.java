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

package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.data.BinaryRow;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DataStatisticsSerializer}. */
class DataStatisticsSerializerTest {

    @Test
    void testSerializeAndDeserialize() {
        DataStatisticsSerializer serializer = new DataStatisticsSerializer();
        DataStatistics dataStatistics = StatisticsUtil.createDataStatistics();
        assertThat(
                        StatisticsUtil.deserializeDataStatistics(
                                        StatisticsUtil.serializeDataStatistics(
                                                dataStatistics, serializer),
                                        serializer)
                                .isEmpty())
                .isTrue();

        BinaryRow p1 = BinaryRow.singleColumn("p1");
        BinaryRow p2 = BinaryRow.singleColumn("p2");
        dataStatistics.add(p1, 1);
        dataStatistics.add(p2, 2);
        dataStatistics.add(p1, 3);
        assertThat(
                        StatisticsUtil.deserializeDataStatistics(
                                        StatisticsUtil.serializeDataStatistics(
                                                dataStatistics, serializer),
                                        serializer)
                                .result())
                .isEqualTo(dataStatistics.result());
    }

    @Test
    void testCopy() {
        DataStatisticsSerializer serializer = new DataStatisticsSerializer();
        DataStatistics original = new DataStatistics();
        BinaryRow p1 = BinaryRow.singleColumn("p1");
        BinaryRow p2 = BinaryRow.singleColumn("p2");
        BinaryRow p3 = BinaryRow.singleColumn("p3");
        original.add(p1, 100L);
        original.add(p2, 200L);

        DataStatistics copy = serializer.copy(original);
        assertThat(copy).isEqualTo(original);

        // Mutating copy should not affect original
        copy.add(p3, 300L);
        assertThat(original.result()).doesNotContainKey(p3);
    }
}
