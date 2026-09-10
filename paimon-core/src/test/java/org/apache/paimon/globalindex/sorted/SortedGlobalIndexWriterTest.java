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

package org.apache.paimon.globalindex.sorted;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link SortedGlobalIndexWriter}. */
public class SortedGlobalIndexWriterTest extends TableTestBase {

    @Test
    public void testSingleColumnWriterRotationPreservesResultGroups() throws Exception {
        GlobalIndexSingleColumnWriter first = mock(GlobalIndexSingleColumnWriter.class);
        GlobalIndexSingleColumnWriter second = mock(GlobalIndexSingleColumnWriter.class);
        when(first.finish(2))
                .thenReturn(Collections.singletonList(new ResultEntry("index-1", 2, null)));
        when(second.finish(1))
                .thenReturn(Collections.singletonList(new ResultEntry("index-2", 1, null)));
        Queue<GlobalIndexSingleColumnWriter> writers =
                new ArrayDeque<>(Arrays.asList(first, second));
        SortedSingleColumnIndexWriter rotatingWriter =
                new SortedSingleColumnIndexWriter(2, writers::remove);

        rotatingWriter.write(10, 0);
        rotatingWriter.write(20, 1);
        rotatingWriter.write(30, 2);
        List<List<ResultEntry>> results = rotatingWriter.finish();

        verify(first).write(10, 0);
        verify(first).write(20, 1);
        verify(second).write(30, 2);
        assertThat(results).hasSize(2);
        assertThat(results.get(0)).extracting(ResultEntry::fileName).containsExactly("index-1");
        assertThat(results.get(1)).extracting(ResultEntry::fileName).containsExactly("index-2");
    }

    @Test
    public void testSingleColumnWriterPreservesSourceRowCount() throws Exception {
        GlobalIndexSingleColumnWriter writer = mock(GlobalIndexSingleColumnWriter.class);
        when(writer.finish(5))
                .thenReturn(Collections.singletonList(new ResultEntry("index", 5, null)));
        SortedSingleColumnIndexWriter taskWriter =
                SortedSingleColumnIndexWriter.forSourceRowCount(5, writer);

        taskWriter.write(10, 0);
        taskWriter.write(20, 0);
        List<List<ResultEntry>> results = taskWriter.finish();

        verify(writer).finish(5);
        assertThat(results).hasSize(1);
        assertThat(results.get(0)).extracting(ResultEntry::rowCount).containsExactly(5L);
    }

    @Test
    public void testSingleColumnWriterPreservesSourceRangeWithoutEntries() {
        GlobalIndexSingleColumnWriter writer = mock(GlobalIndexSingleColumnWriter.class);
        when(writer.finish(3))
                .thenReturn(Collections.singletonList(new ResultEntry("index", 3, null)));
        SortedSingleColumnIndexWriter taskWriter =
                SortedSingleColumnIndexWriter.forSourceRowCount(3, writer);

        List<List<ResultEntry>> results = taskWriter.finish();

        verify(writer).finish(3);
        assertThat(results).hasSize(1);
        assertThat(results.get(0)).extracting(ResultEntry::rowCount).containsExactly(3L);
    }

    @Test
    public void testSingleColumnWriterClosesActiveWriter() throws Exception {
        GlobalIndexSingleColumnWriter activeWriter =
                mock(
                        GlobalIndexSingleColumnWriter.class,
                        org.mockito.Mockito.withSettings().extraInterfaces(Closeable.class));
        SortedSingleColumnIndexWriter rotatingWriter =
                new SortedSingleColumnIndexWriter(2, () -> activeWriter);
        rotatingWriter.write(10, 0);

        assertThat(rotatingWriter).isInstanceOf(AutoCloseable.class);
        ((AutoCloseable) rotatingWriter).close();

        verify((Closeable) activeWriter).close();
    }

    @Test
    public void testBuildForSinglePartitionClosesWriterAfterFailure() throws Exception {
        createTableDefault();
        GlobalIndexSingleColumnWriter activeWriter =
                mock(
                        GlobalIndexSingleColumnWriter.class,
                        org.mockito.Mockito.withSettings().extraInterfaces(Closeable.class));
        org.mockito.Mockito.doThrow(new RuntimeException("write failed"))
                .when(activeWriter)
                .write(10, 0);
        SortedGlobalIndexWriter writer =
                new SortedGlobalIndexWriter(getTableDefault(), "btree") {
                    @Override
                    public GlobalIndexSingleColumnWriter createWriter() {
                        return activeWriter;
                    }
                };
        writer.withIndexField("f0");

        assertThatThrownBy(
                        () ->
                                writer.buildForSinglePartition(
                                        new Range(0, 0),
                                        null,
                                        Collections.<InternalRow>singletonList(
                                                        GenericRow.of(10, 0L))
                                                .iterator(),
                                        1L))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("write failed");

        verify((Closeable) activeWriter).close();
    }

    @Override
    public Schema schemaDefault() {
        return Schema.newBuilder().column("f0", DataTypes.INT()).build();
    }
}
