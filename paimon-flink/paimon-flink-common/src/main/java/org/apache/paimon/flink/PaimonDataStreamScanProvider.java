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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.lineage.LineageUtils;
import org.apache.paimon.table.Table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.function.Function;

/**
 * Paimon {@link DataStreamScanProvider} that also implements {@link LineageVertexProvider} so
 * Flink's lineage graph discovers the Paimon source table.
 */
public class PaimonDataStreamScanProvider implements DataStreamScanProvider, LineageVertexProvider {

    private final boolean isBounded;
    private final Function<StreamExecutionEnvironment, DataStream<RowData>> producer;
    private final String name;
    private final Table table;
    @Nullable private final Catalog catalog;

    public PaimonDataStreamScanProvider(
            boolean isBounded,
            Function<StreamExecutionEnvironment, DataStream<RowData>> producer,
            String name,
            Table table,
            @Nullable Catalog catalog) {
        this.isBounded = isBounded;
        this.producer = producer;
        this.name = name;
        this.table = table;
        this.catalog = catalog;
    }

    @Override
    public DataStream<RowData> produceDataStream(
            ProviderContext context, StreamExecutionEnvironment env) {
        return producer.apply(env);
    }

    @Override
    public boolean isBounded() {
        return isBounded;
    }

    @Override
    public LineageVertex getLineageVertex() {
        return LineageUtils.sourceLineageVertex(name, isBounded, table, catalog);
    }
}
