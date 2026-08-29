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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.plan.stats.TableStats;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for row count inference in {@link FlinkTableSource}. */
class FlinkTableSourceStatisticsTest {

    @Test
    void testSumRowCounts() {
        assertThat(FlinkTableSource.sumRowCounts(LongStream.empty())).isZero();
        assertThat(FlinkTableSource.sumRowCounts(LongStream.of(2L, 3L))).isEqualTo(5L);

        long unknown = TableStats.UNKNOWN.getRowCount();
        assertThat(FlinkTableSource.sumRowCounts(LongStream.of(0L))).isEqualTo(unknown);
        assertThat(FlinkTableSource.sumRowCounts(LongStream.of(3L, 0L))).isEqualTo(unknown);
        assertThat(FlinkTableSource.sumRowCounts(LongStream.of(-1L))).isEqualTo(unknown);
        assertThat(FlinkTableSource.sumRowCounts(LongStream.of(3L, -2L))).isEqualTo(unknown);
        assertThat(FlinkTableSource.sumRowCounts(LongStream.of(Long.MAX_VALUE, 1L)))
                .isEqualTo(unknown);
    }

    @Test
    void testNonDataTableInferenceReturnsUnknownForUnknownSplitRowCount() {
        Table table = mock(Table.class);
        when(table.options()).thenReturn(Collections.emptyMap());
        when(table.statistics()).thenReturn(Optional.empty());
        ReadBuilder readBuilder = mock(ReadBuilder.class, Answers.RETURNS_SELF);
        when(table.newReadBuilder()).thenReturn(readBuilder);
        TableScan scan = mock(TableScan.class);
        when(readBuilder.newScan()).thenReturn(scan);
        TableScan.Plan plan = mock(TableScan.Plan.class);
        when(scan.plan()).thenReturn(plan);
        Split known = split(5L);
        Split unknown = split(-1L);
        when(plan.splits()).thenReturn(Arrays.asList(known, unknown));

        DataTableSource source =
                new DataTableSource(
                        ObjectIdentifier.of("catalog", "database", "table"), table, false, null);

        assertThat(source.reportStatistics().getRowCount())
                .isEqualTo(TableStats.UNKNOWN.getRowCount());
    }

    @Test
    void testDataTableInferenceReturnsUnknownForUnknownPartitionRowCount() {
        DataTable table = mock(DataTable.class);
        when(table.options()).thenReturn(Collections.emptyMap());
        when(table.statistics()).thenReturn(Optional.empty());
        CoreOptions coreOptions = mock(CoreOptions.class);
        when(coreOptions.splitTargetSize()).thenReturn(128L);
        when(table.coreOptions()).thenReturn(coreOptions);
        ReadBuilder readBuilder = mock(ReadBuilder.class, Answers.RETURNS_SELF);
        when(table.newReadBuilder()).thenReturn(readBuilder);
        TableScan scan = mock(TableScan.class);
        when(readBuilder.newScan()).thenReturn(scan);
        PartitionEntry known = mock(PartitionEntry.class);
        when(known.recordCount()).thenReturn(5L);
        PartitionEntry unknown = mock(PartitionEntry.class);
        when(unknown.recordCount()).thenReturn(0L);
        when(scan.listPartitionEntries()).thenReturn(Arrays.asList(known, unknown));

        DataTableSource source =
                new DataTableSource(
                        ObjectIdentifier.of("catalog", "database", "table"), table, false, null);

        assertThat(source.reportStatistics().getRowCount())
                .isEqualTo(TableStats.UNKNOWN.getRowCount());
    }

    private static Split split(long rowCount) {
        Split split = mock(Split.class);
        when(split.rowCount()).thenReturn(rowCount);
        return split;
    }
}
