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

package org.apache.paimon.flink.sink;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.format.FormatTableWrite;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;

import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link FlinkFormatTableDataStreamSink}. */
class FlinkFormatTableDataStreamSinkTest {

    @TempDir java.nio.file.Path temp;

    @Test
    void testOverwriteUsesOneSinkWriterWhileAppendKeepsParallelism() {
        int parallelism = 4;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        RowType rowType = RowType.of(new IntType());
        DataStream<RowData> input =
                env.fromCollection(
                        Collections.singletonList((RowData) GenericRowData.of(1)),
                        InternalTypeInfo.of(toLogicalType(rowType)));
        FormatTable table = mock(FormatTable.class);
        when(table.options()).thenReturn(Collections.singletonMap("path", temp.toUri().toString()));
        when(table.partitionKeys()).thenReturn(Collections.emptyList());
        when(table.primaryKeys()).thenReturn(Collections.emptyList());
        when(table.fullName()).thenReturn("test_db.test_table");
        when(table.rowType()).thenReturn(rowType);

        DataStreamSink<?> overwriteSink =
                new FlinkFormatTableDataStreamSink(table, true, Collections.emptyMap())
                        .sinkFrom(input);
        DataStreamSink<?> appendSink =
                new FlinkFormatTableDataStreamSink(table, false, Collections.emptyMap())
                        .sinkFrom(input);

        assertThat(overwriteSink.getTransformation().getParallelism()).isOne();
        // Without this the adaptive batch scheduler is free to pick the parallelism back up.
        assertThat(overwriteSink.getTransformation().isParallelismConfigured()).isTrue();
        assertThat(appendSink.getTransformation().getParallelism()).isEqualTo(parallelism);
    }

    @Test
    void testEmptyMessagesAreCommittedOnlyForOverwrite() throws Exception {
        FormatTableWrite overwriteWrite = mock(FormatTableWrite.class);
        BatchTableCommit overwriteCommit = mock(BatchTableCommit.class);
        when(overwriteWrite.prepareCommit()).thenReturn(Collections.emptyList());

        SinkWriter<?> overwriteWriter = createWriter(true, overwriteWrite, overwriteCommit);
        overwriteWriter.flush(true);
        overwriteWriter.close();

        verify(overwriteCommit).commit(Collections.emptyList());

        FormatTableWrite appendWrite = mock(FormatTableWrite.class);
        BatchTableCommit appendCommit = mock(BatchTableCommit.class);
        when(appendWrite.prepareCommit()).thenReturn(Collections.emptyList());

        SinkWriter<?> appendWriter = createWriter(false, appendWrite, appendCommit);
        appendWriter.flush(true);
        appendWriter.close();

        verify(appendCommit, never()).commit(anyList());
    }

    @Test
    void testEmptyOverwriteIsNotCommittedBeforeEndOfInput() throws Exception {
        FormatTableWrite tableWrite = mock(FormatTableWrite.class);
        BatchTableCommit tableCommit = mock(BatchTableCommit.class);
        when(tableWrite.prepareCommit()).thenReturn(Collections.emptyList());

        SinkWriter<?> writer = createWriter(true, tableWrite, tableCommit);
        // A checkpoint is not the end of the input, and a job that fails after one must not have
        // replaced the target.
        writer.flush(false);
        writer.close();

        verify(tableCommit, never()).commit(anyList());
        verify(tableCommit, never()).abort(anyList());
    }

    @Test
    void testFailedEmptyOverwriteCommitIsAborted() throws Exception {
        FormatTableWrite tableWrite = mock(FormatTableWrite.class);
        BatchTableCommit tableCommit = mock(BatchTableCommit.class);
        when(tableWrite.prepareCommit()).thenReturn(Collections.emptyList());
        doThrow(new RuntimeException("commit failed"))
                .when(tableCommit)
                .commit(Collections.emptyList());
        doThrow(new RuntimeException("abort failed"))
                .when(tableCommit)
                .abort(Collections.emptyList());
        SinkWriter<?> writer = createWriter(true, tableWrite, tableCommit);
        writer.flush(true);

        assertThatThrownBy(writer::close)
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("commit failed")
                .rootCause()
                .satisfies(
                        cause ->
                                assertThat(cause.getSuppressed())
                                        .extracting(Throwable::getMessage)
                                        .containsExactly("abort failed"));
        verify(tableCommit).abort(Collections.emptyList());
    }

    @Test
    void testFormatTableSinkLineageVertex() throws Exception {
        FormatTable table =
                FormatTable.builder()
                        .fileIO(LocalFileIO.create())
                        .identifier(Identifier.create("test_db", "test_table"))
                        .rowType(RowType.of(new IntType()))
                        .partitionKeys(Collections.emptyList())
                        .location(new Path(temp.toUri().toString()).toString())
                        .format(FormatTable.Format.PARQUET)
                        .options(Collections.singletonMap("path", temp.toUri().toString()))
                        .catalogContext(CatalogContext.create(new Options()))
                        .build();

        FlinkFormatTableDataStreamSink.FormatTableSink sink =
                new FlinkFormatTableDataStreamSink.FormatTableSink(
                        table, false, Collections.emptyMap());

        assertThat(sink).isInstanceOf(LineageVertexProvider.class);
        LineageVertex vertex = sink.getLineageVertex();
        assertThat(vertex.datasets()).hasSize(1);
        assertThat(vertex.datasets().get(0).name()).isEqualTo("paimon." + table.fullName());
    }

    private SinkWriter<?> createWriter(
            boolean overwrite, FormatTableWrite tableWrite, BatchTableCommit tableCommit)
            throws Exception {
        FormatTable table = mock(FormatTable.class);
        BatchWriteBuilder writeBuilder = mock(BatchWriteBuilder.class);
        when(table.newBatchWriteBuilder()).thenReturn(writeBuilder);
        when(writeBuilder.newWrite()).thenReturn(tableWrite);
        when(writeBuilder.newCommit()).thenReturn(tableCommit);
        if (overwrite) {
            when(writeBuilder.withOverwrite(Collections.emptyMap())).thenReturn(writeBuilder);
        }

        return new FlinkFormatTableDataStreamSink.FormatTableSink.FormatTableSinkWriter(
                table, overwrite, Collections.emptyMap());
    }
}
