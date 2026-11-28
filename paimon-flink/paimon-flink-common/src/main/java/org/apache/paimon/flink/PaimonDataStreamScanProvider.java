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

import org.apache.paimon.flink.lineage.LineageUtils;
import org.apache.paimon.table.Table;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.data.RowData;

import java.util.Optional;
import java.util.function.Function;

/**
 * Paimon {@link DataStreamScanProvider} that also implements {@link LineageVertexProvider} so
 * Flink's lineage graph discovers the Paimon source table.
 */
public class PaimonDataStreamScanProvider
        implements DataStreamScanProvider, ParallelismProvider, LineageVertexProvider {

    /** The transformation name the planner uses for the uid of a table source scan. */
    private static final String SOURCE_TRANSFORMATION = "source";

    private final boolean isBounded;
    private final Function<StreamExecutionEnvironment, DataStream<RowData>> producer;
    private final String name;
    private final Table table;
    private final Optional<Integer> parallelism;

    public PaimonDataStreamScanProvider(
            boolean isBounded,
            Function<StreamExecutionEnvironment, DataStream<RowData>> producer,
            String name,
            Table table) {
        this(isBounded, producer, name, table, Optional.empty());
    }

    public PaimonDataStreamScanProvider(
            boolean isBounded,
            Function<StreamExecutionEnvironment, DataStream<RowData>> producer,
            String name,
            Table table,
            Optional<Integer> parallelism) {
        this.isBounded = isBounded;
        this.producer = producer;
        this.name = name;
        this.table = table;
        this.parallelism = parallelism;
    }

    @Override
    public DataStream<RowData> produceDataStream(
            ProviderContext context, StreamExecutionEnvironment env) {
        DataStream<RowData> dataStream = producer.apply(env);
        // Since Flink 2.3 the planner no longer sets the name, the description and the uid of the
        // returned transformation, it hands them to the provider through the context instead.
        Transformation<?> transformation = dataStream.getTransformation();
        contextValue(context, "getName").ifPresent(transformation::setName);
        contextValue(context, "getDescription").ifPresent(transformation::setDescription);
        // The planner generates a uid only for compiled plans and with
        // 'table.exec.uid.generation' = 'ALWAYS'. Up to Flink 2.2 it then replaced whatever uid the
        // transformation had with this one, so do the same to keep the state of those jobs mapped.
        context.generateUid(SOURCE_TRANSFORMATION).ifPresent(transformation::setUid);
        return dataStream;
    }

    private static Optional<String> contextValue(ProviderContext context, String getter) {
        try {
            String value = (String) ProviderContext.class.getMethod(getter).invoke(context);
            return value == null || value.isEmpty() ? Optional.empty() : Optional.of(value);
        } catch (NoSuchMethodException e) {
            // before Flink 2.3 the context has no such getter and the planner sets the value
            return Optional.empty();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isBounded() {
        return isBounded;
    }

    @Override
    public Optional<Integer> getParallelism() {
        return parallelism;
    }

    @Override
    public LineageVertex getLineageVertex() {
        return LineageUtils.sourceLineageVertex(name, isBounded, table);
    }
}
