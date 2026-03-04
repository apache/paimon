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

package org.apache.paimon.flink.lineage;

import org.apache.paimon.table.Table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.data.RowData;

/**
 * Factory that wraps {@link DataStreamScanProvider} and {@link DataStreamSinkProvider} with {@link
 * LineageVertexProvider} support for Flink 2.0+.
 */
public class DataStreamProviderFactory {

    /**
     * Returns a {@link ScanRuntimeProvider} that also implements {@link LineageVertexProvider} so
     * Flink's lineage graph discovers the Paimon source table.
     */
    public static ScanRuntimeProvider getScanProvider(
            ScanRuntimeProvider provider, String name, Table table) {
        return new LineageAwarePaimonDataStreamScanProvider(
                (DataStreamScanProvider) provider, name, table);
    }

    /**
     * Returns a {@link SinkRuntimeProvider} that also implements {@link LineageVertexProvider} so
     * Flink's lineage graph discovers the Paimon sink table.
     */
    public static SinkRuntimeProvider getSinkProvider(
            SinkRuntimeProvider provider, String name, Table table) {
        return new LineageAwarePaimonDataStreamSinkProvider(
                (DataStreamSinkProvider) provider, name, table);
    }

    private static class LineageAwarePaimonDataStreamScanProvider
            implements DataStreamScanProvider, LineageVertexProvider {

        private final DataStreamScanProvider delegate;
        private final String name;
        private final Table table;

        LineageAwarePaimonDataStreamScanProvider(
                DataStreamScanProvider delegate, String name, Table table) {
            this.delegate = delegate;
            this.name = name;
            this.table = table;
        }

        @Override
        public DataStream<RowData> produceDataStream(
                ProviderContext context, StreamExecutionEnvironment env) {
            return delegate.produceDataStream(context, env);
        }

        @Override
        public boolean isBounded() {
            return delegate.isBounded();
        }

        @Override
        public LineageVertex getLineageVertex() {
            return LineageUtils.sourceLineageVertex(name, delegate.isBounded(), table);
        }
    }

    private static class LineageAwarePaimonDataStreamSinkProvider
            implements DataStreamSinkProvider, LineageVertexProvider {

        private final DataStreamSinkProvider delegate;
        private final String name;
        private final Table table;

        LineageAwarePaimonDataStreamSinkProvider(
                DataStreamSinkProvider delegate, String name, Table table) {
            this.delegate = delegate;
            this.name = name;
            this.table = table;
        }

        @Override
        public DataStreamSink<?> consumeDataStream(
                ProviderContext providerContext, DataStream<RowData> dataStream) {
            return delegate.consumeDataStream(providerContext, dataStream);
        }

        @Override
        public LineageVertex getLineageVertex() {
            return LineageUtils.sinkLineageVertex(name, table);
        }
    }
}
