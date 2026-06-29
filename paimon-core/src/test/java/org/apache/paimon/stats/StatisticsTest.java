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

package org.apache.paimon.stats;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Statistics}. */
public class StatisticsTest {

    @Test
    public void testJsonRoundTripWithBlobMetadata() {
        StatisticsBlobMetadata blobMetadata =
                new StatisticsBlobMetadata(
                        "paimon-ndv-theta-sketch-v1",
                        Collections.singletonList(1),
                        10L,
                        20L,
                        Collections.singletonMap("ndv", "5.0"),
                        "stat-00001",
                        0L,
                        128L);
        Statistics statistics =
                new Statistics(
                        10L,
                        0L,
                        100L,
                        1000L,
                        Collections.emptyMap(),
                        Collections.singletonList(blobMetadata));

        String json = statistics.toJson();
        assertThat(json).contains("\"blobMetadata\"");
        assertThat(Statistics.fromJson(json)).isEqualTo(statistics);
    }

    @Test
    public void testReadJsonWithoutBlobMetadata() {
        String json =
                "{\n"
                        + "  \"snapshotId\" : 10,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"mergedRecordCount\" : 100,\n"
                        + "  \"mergedRecordSize\" : 1000,\n"
                        + "  \"colStats\" : { }\n"
                        + "}";

        Statistics statistics = Statistics.fromJson(json);
        assertThat(statistics.blobMetadata()).isEmpty();
        assertThat(statistics.toJson()).doesNotContain("\"blobMetadata\"");
    }
}
