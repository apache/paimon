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

import org.apache.paimon.flink.globalindex.SortedIndexTopoBuilder.SortedBuildTask;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
                        SortedGlobalIndexBuilder.class,
                        int.class,
                        int.class,
                        int.class,
                        org.apache.paimon.types.DataType.class);
        constructor.setAccessible(true);
        Object operator =
                constructor.newInstance(
                        Collections.emptyList(),
                        0,
                        mock(SortedGlobalIndexBuilder.class),
                        0,
                        0,
                        0,
                        DataTypes.INT());
        GlobalIndexSingleColumnWriter activeWriter =
                mock(
                        GlobalIndexSingleColumnWriter.class,
                        org.mockito.Mockito.withSettings().extraInterfaces(Closeable.class));
        Field currentWriter = operatorClass.getDeclaredField("currentWriter");
        currentWriter.setAccessible(true);
        currentWriter.set(operator, activeWriter);

        Method close = operatorClass.getMethod("close");
        close.invoke(operator);

        verify((Closeable) activeWriter).close();
    }

    @Test
    public void testBuildIndexReturnsFalseWhenNoBuildTask() throws Exception {
        SortedGlobalIndexBuilder indexBuilder = mock(SortedGlobalIndexBuilder.class);
        when(indexBuilder.withIndexField("id")).thenReturn(indexBuilder);
        when(indexBuilder.incrementalScan()).thenReturn(Optional.empty());
        StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);

        assertThat(
                        SortedIndexTopoBuilder.buildIndex(
                                env,
                                () -> indexBuilder,
                                mock(FileStoreTable.class),
                                Collections.singletonList("id"),
                                null,
                                new Options(),
                                null))
                .isFalse();
        verify(indexBuilder).incrementalScan();
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
}
