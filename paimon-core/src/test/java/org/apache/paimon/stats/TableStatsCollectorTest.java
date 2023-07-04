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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.format.TableStatsCollector;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.statistics.FullFieldStatsCollector;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableStatsCollector}. */
public class TableStatsCollectorTest {

    @Test
    public void testCollect() {
        RowType rowType =
                RowType.of(new IntType(), new VarCharType(10), new ArrayType(new IntType()));
        TableStatsCollector collector =
                new TableStatsCollector(
                        rowType,
                        IntStream.range(0, rowType.getFieldCount())
                                .mapToObj(
                                        i ->
                                                (FieldStatsCollector.Factory)
                                                        FullFieldStatsCollector::new)
                                .toArray(FieldStatsCollector.Factory[]::new));

        collector.collect(
                GenericRow.of(
                        1, BinaryString.fromString("Paimon"), new GenericArray(new int[] {1, 10})));
        assertThat(collector.extract())
                .isEqualTo(
                        new FieldStats[] {
                            new FieldStats(1, 1, 0L),
                            new FieldStats(
                                    BinaryString.fromString("Paimon"),
                                    BinaryString.fromString("Paimon"),
                                    0L),
                            new FieldStats(null, null, 0L)
                        });

        collector.collect(GenericRow.of(3, null, new GenericArray(new int[] {3, 30})));
        assertThat(collector.extract())
                .isEqualTo(
                        new FieldStats[] {
                            new FieldStats(1, 3, 0L),
                            new FieldStats(
                                    BinaryString.fromString("Paimon"),
                                    BinaryString.fromString("Paimon"),
                                    1L),
                            new FieldStats(null, null, 0L)
                        });

        collector.collect(
                GenericRow.of(
                        null,
                        BinaryString.fromString("Apache"),
                        new GenericArray(new int[] {2, 20})));
        assertThat(collector.extract())
                .isEqualTo(
                        new FieldStats[] {
                            new FieldStats(1, 3, 1L),
                            new FieldStats(
                                    BinaryString.fromString("Apache"),
                                    BinaryString.fromString("Paimon"),
                                    1L),
                            new FieldStats(null, null, 0L)
                        });

        collector.collect(GenericRow.of(2, BinaryString.fromString("Batch"), null));
        assertThat(collector.extract())
                .isEqualTo(
                        new FieldStats[] {
                            new FieldStats(1, 3, 1L),
                            new FieldStats(
                                    BinaryString.fromString("Apache"),
                                    BinaryString.fromString("Paimon"),
                                    1L),
                            new FieldStats(null, null, 1L)
                        });
    }
}
