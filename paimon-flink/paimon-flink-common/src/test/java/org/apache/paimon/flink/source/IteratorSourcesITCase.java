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

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/**
 * An integration test for the sources based on iterators.
 *
 * <p>This test uses the {@link NumberSequenceRowSource} as a concrete iterator source
 * implementation, but covers all runtime-related aspects for all the iterator-based sources
 * together.
 */
public class IteratorSourcesITCase extends TestLogger {

    private static final int PARALLELISM = 4;

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    // ------------------------------------------------------------------------

    @Test
    public void testParallelSourceExecution() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        final DataStream<RowData> stream =
                env.fromSource(
                        new NumberSequenceRowSource(1L, 1_000L),
                        WatermarkStrategy.noWatermarks(),
                        "iterator source");

        final List<RowData> result =
                IteratorUtils.toList(stream.executeAndCollect("Iterator Source Test"));

        verifySequence(result, 1L, 1_000L);
    }

    // ------------------------------------------------------------------------
    //  test utils
    // ------------------------------------------------------------------------

    private static void verifySequence(
            final List<RowData> sequence, final long from, final long to) {
        if (sequence.size() != to - from + 1) {
            fail(String.format("Expected: Sequence [%d, %d]. Found: %s", from, to, sequence));
        }

        final List<Long> list =
                sequence.stream()
                        .map(r -> r.getLong(0))
                        .sorted(Long::compareTo)
                        .collect(Collectors.toList());

        int pos = 0;
        for (long value = from; value <= to; value++, pos++) {
            if (value != list.get(pos)) {
                fail(String.format("Expected: Sequence [%d, %d]. Found: %s", from, to, list));
            }
        }
    }
}
