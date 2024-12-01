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

package org.apache.paimon.flink.sink.index;

import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.crosspartition.KeyPartOrRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** Operator for {@link IndexBootstrap}. */
public class IndexBootstrapOperator<T> extends AbstractStreamOperator<Tuple2<KeyPartOrRow, T>>
        implements OneInputStreamOperator<T, Tuple2<KeyPartOrRow, T>> {

    private static final long serialVersionUID = 1L;

    private final IndexBootstrap bootstrap;
    private final SerializableFunction<InternalRow, T> converter;

    private IndexBootstrapOperator(
            StreamOperatorParameters<Tuple2<KeyPartOrRow, T>> parameters,
            IndexBootstrap bootstrap,
            SerializableFunction<InternalRow, T> converter) {
        this.bootstrap = bootstrap;
        this.converter = converter;
        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        bootstrap.bootstrap(
                RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext()),
                RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext()),
                this::collect);
    }

    @Override
    public void processElement(StreamRecord<T> streamRecord) throws Exception {
        output.collect(new StreamRecord<>(new Tuple2<>(KeyPartOrRow.ROW, streamRecord.getValue())));
    }

    private void collect(InternalRow row) {
        output.collect(
                new StreamRecord<>(new Tuple2<>(KeyPartOrRow.KEY_PART, converter.apply(row))));
    }

    /** {@link StreamOperatorFactory} of {@link IndexBootstrapOperator}. */
    public static class Factory<T> extends AbstractStreamOperatorFactory<Tuple2<KeyPartOrRow, T>>
            implements OneInputStreamOperatorFactory<T, Tuple2<KeyPartOrRow, T>> {
        private final IndexBootstrap bootstrap;
        private final SerializableFunction<InternalRow, T> converter;

        public Factory(IndexBootstrap bootstrap, SerializableFunction<InternalRow, T> converter) {
            this.chainingStrategy = ChainingStrategy.ALWAYS;
            this.bootstrap = bootstrap;
            this.converter = converter;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <OP extends StreamOperator<Tuple2<KeyPartOrRow, T>>> OP createStreamOperator(
                StreamOperatorParameters<Tuple2<KeyPartOrRow, T>> parameters) {
            return (OP) new IndexBootstrapOperator<>(parameters, bootstrap, converter);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return IndexBootstrapOperator.class;
        }
    }
}
