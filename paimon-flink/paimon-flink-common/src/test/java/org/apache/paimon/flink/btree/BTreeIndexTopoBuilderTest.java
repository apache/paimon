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

package org.apache.paimon.flink.btree;

import org.apache.paimon.flink.btree.BTreeIndexTopoBuilder.BTreeBuildTask;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Range;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests for {@link BTreeIndexTopoBuilder}. */
public class BTreeIndexTopoBuilderTest {

    @Test
    public void testBuildIndexReturnsFalseWhenNoBuildTask() throws Exception {
        BTreeGlobalIndexBuilder indexBuilder = mock(BTreeGlobalIndexBuilder.class);
        when(indexBuilder.withIndexField("id")).thenReturn(indexBuilder);
        when(indexBuilder.scan()).thenReturn(Optional.empty());
        StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);

        assertThat(
                        BTreeIndexTopoBuilder.buildIndex(
                                env,
                                () -> indexBuilder,
                                mock(FileStoreTable.class),
                                Collections.singletonList("id"),
                                null,
                                new Options()))
                .isFalse();
        verifyNoInteractions(env);
    }

    @Test
    public void testCalculateParallelismByTotalRowsInsteadOfRangeCount() {
        List<BTreeBuildTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            tasks.add(new BTreeBuildTask(i, new Range(i * 10L, i * 10L + 9), new byte[0]));
        }

        assertThat(BTreeIndexTopoBuilder.calculateParallelism(tasks, 1000L, 4096)).isEqualTo(1);
    }

    @Test
    public void testCalculateParallelismHonorsMaxParallelism() {
        List<BTreeBuildTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            tasks.add(new BTreeBuildTask(i, new Range(i * 1000L, i * 1000L + 999), new byte[0]));
        }

        assertThat(BTreeIndexTopoBuilder.calculateParallelism(tasks, 1000L, 16)).isEqualTo(16);
    }

    @Test
    public void testCalculateParallelismKeepsSingleRangeBehavior() {
        List<BTreeBuildTask> tasks = new ArrayList<>();
        tasks.add(new BTreeBuildTask(0, new Range(0, 1499), new byte[0]));

        assertThat(BTreeIndexTopoBuilder.calculateParallelism(tasks, 1000L, 16)).isEqualTo(1);
    }
}
