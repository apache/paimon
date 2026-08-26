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

package org.apache.paimon.flink.globalindex;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.globalindex.SortedIndexTopoBuilder.SortedBuildTask;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexScanner;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexWriter;
import org.apache.paimon.globalindex.sorted.SortedSingleColumnIndexWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests for {@link SortedIndexTopoBuilder}. */
public class SortedIndexTopoBuilderTest {

    @Test
    public void testSupportsBitmapAndBTree() {
        assertThat(SortedIndexTopoBuilder.supports("bitmap")).isTrue();
        assertThat(SortedIndexTopoBuilder.supports("btree")).isTrue();
        assertThat(SortedIndexTopoBuilder.supports("multivalue")).isTrue();
        assertThat(SortedIndexTopoBuilder.supports("inverted")).isFalse();
    }

    @Test
    public void testWriteIndexOperatorClosesActiveWriter() throws Exception {
        Class<?> operatorClass = null;
        for (Class<?> candidate : SortedIndexTopoBuilder.class.getDeclaredClasses()) {
            if (candidate.getSimpleName().equals("WriteIndexOperator")) {
                operatorClass = candidate;
                break;
            }
        }
        assertThat(operatorClass).isNotNull();
        Constructor<?> constructor =
                operatorClass.getDeclaredConstructor(
                        List.class,
                        int.class,
                        SortedGlobalIndexWriter.class,
                        long.class,
                        int.class,
                        int.class,
                        int.class,
                        org.apache.paimon.types.DataType.class);
        constructor.setAccessible(true);
        Object operator =
                constructor.newInstance(
                        Collections.emptyList(),
                        0,
                        mock(SortedGlobalIndexWriter.class),
                        1L,
                        0,
                        0,
                        0,
                        DataTypes.INT());
        GlobalIndexSingleColumnWriter activeWriter =
                mock(
                        GlobalIndexSingleColumnWriter.class,
                        org.mockito.Mockito.withSettings().extraInterfaces(Closeable.class));
        SortedSingleColumnIndexWriter taskWriter =
                SortedSingleColumnIndexWriter.forSourceRowCount(1, activeWriter);
        Field currentWriter = operatorClass.getDeclaredField("currentWriter");
        currentWriter.setAccessible(true);
        currentWriter.set(operator, taskWriter);

        Method close = operatorClass.getMethod("close");
        close.invoke(operator);

        verify((Closeable) activeWriter).close();
    }

    @Test
    public void testBuildIndexReturnsFalseWhenNoBuildTask() throws Exception {
        SortedGlobalIndexScanner indexScanner = mock(SortedGlobalIndexScanner.class);
        when(indexScanner.withIndexField("id")).thenReturn(indexScanner);
        when(indexScanner.incrementalScan()).thenReturn(Optional.empty());
        StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);

        assertThat(
                        SortedIndexTopoBuilder.buildIndex(
                                env,
                                () -> indexScanner,
                                mock(FileStoreTable.class),
                                Collections.singletonList("id"),
                                "btree",
                                null,
                                new Options()))
                .isFalse();
        verify(indexScanner).incrementalScan();
        verifyNoInteractions(env);
    }

    @Test
    public void testBuildIndexStreamReturnsEmptyWhenNoBuildTask() throws Exception {
        SortedGlobalIndexScanner indexScanner = mock(SortedGlobalIndexScanner.class);
        when(indexScanner.withIndexField("id")).thenReturn(indexScanner);
        when(indexScanner.incrementalScan()).thenReturn(Optional.empty());
        StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);

        assertThat(
                        SortedIndexTopoBuilder.buildIndexStream(
                                env,
                                () -> indexScanner,
                                mock(FileStoreTable.class),
                                Collections.singletonList("id"),
                                "btree",
                                null,
                                new Options()))
                .isEmpty();
        verify(indexScanner).incrementalScan();
        verifyNoInteractions(env);
    }

    @Test
    public void testCalculateParallelismByTotalRowsInsteadOfRangeCount() {
        List<SortedBuildTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            tasks.add(new SortedBuildTask(i, new Range(i * 10L, i * 10L + 9), new byte[0]));
        }

        assertThat(SortedIndexTopoBuilder.calculateParallelism(tasks, 1000L, 4096)).isEqualTo(1);
    }

    @Test
    public void testCalculateParallelismHonorsMaxParallelism() {
        List<SortedBuildTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            tasks.add(new SortedBuildTask(i, new Range(i * 1000L, i * 1000L + 999), new byte[0]));
        }

        assertThat(SortedIndexTopoBuilder.calculateParallelism(tasks, 1000L, 16)).isEqualTo(16);
    }

    @Test
    public void testCalculateParallelismKeepsSingleRangeBehavior() {
        List<SortedBuildTask> tasks = new ArrayList<>();
        tasks.add(new SortedBuildTask(0, new Range(0, 1499), new byte[0]));

        assertThat(SortedIndexTopoBuilder.calculateParallelism(tasks, 1000L, 16)).isEqualTo(1);
    }

    @Test
    public void testNormalizedSortColumnsUseRowIdAsTieBreaker() {
        assertThat(SortedIndexTopoBuilder.createSortColumns("task-id", "index-key"))
                .containsExactly("task-id", "index-key", SpecialFields.ROW_ID.name());
    }

    @Test
    public void testBuildTaskPartitioner() {
        assertThat(SortedIndexTopoBuilder.BUILD_TASK_PARTITIONER.partition(0, 4)).isEqualTo(0);
        assertThat(SortedIndexTopoBuilder.BUILD_TASK_PARTITIONER.partition(5, 4)).isEqualTo(1);
        assertThat(SortedIndexTopoBuilder.BUILD_TASK_PARTITIONER.partition(9, 4)).isEqualTo(1);
    }

    @Test
    public void testBuildTaskPartitionUsesBatchExchange() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, 0);
        writer.complete();
        DataStream<InternalRow> input =
                env.fromData(
                        Collections.<InternalRow>singletonList(row),
                        InternalTypeInfo.fromRowType(RowType.of(DataTypes.INT())));

        DataStream<InternalRow> partitioned = SortedIndexTopoBuilder.partitionByBuildTask(input, 0);

        assertThat(partitioned.getTransformation()).isInstanceOf(PartitionTransformation.class);
        assertThat(((PartitionTransformation<?>) partitioned.getTransformation()).getExchangeMode())
                .isEqualTo(StreamExchangeMode.BATCH);
    }
}
