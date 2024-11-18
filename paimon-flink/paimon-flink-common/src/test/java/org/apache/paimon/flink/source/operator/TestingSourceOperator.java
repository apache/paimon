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

package org.apache.paimon.flink.source.operator;

import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.flink.source.SimpleSourceSplitSerializer;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackendParametersImpl;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import java.util.ArrayList;
import java.util.Collections;

/** A SourceOperator extension to simplify test setup. */
public class TestingSourceOperator<T> extends SourceOperator<T, SimpleSourceSplit> {

    private static final long serialVersionUID = 1L;

    private final int subtaskIndex;
    private final int parallelism;

    public TestingSourceOperator(
            StreamOperatorParameters<T> parameters,
            SourceReader<T, SimpleSourceSplit> reader,
            WatermarkStrategy<T> watermarkStrategy,
            ProcessingTimeService timeService,
            boolean emitProgressiveWatermarks) {

        this(
                parameters,
                reader,
                watermarkStrategy,
                timeService,
                new TestingOperatorEventGateway(),
                1,
                5,
                emitProgressiveWatermarks);
    }

    public TestingSourceOperator(
            StreamOperatorParameters<T> parameters,
            SourceReader<T, SimpleSourceSplit> reader,
            WatermarkStrategy<T> watermarkStrategy,
            ProcessingTimeService timeService,
            OperatorEventGateway eventGateway,
            int subtaskIndex,
            int parallelism,
            boolean emitProgressiveWatermarks) {

        super(
                (context) -> reader,
                eventGateway,
                new SimpleSourceSplitSerializer(),
                watermarkStrategy,
                timeService,
                new Configuration(),
                "localhost",
                emitProgressiveWatermarks,
                () -> false);

        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
        this.metrics = UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
        initSourceMetricGroup();

        // unchecked wrapping is okay to keep tests simpler
        try {
            initReader();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    }

    @Override
    public StreamingRuntimeContext getRuntimeContext() {
        return new MockStreamingRuntimeContext(false, parallelism, subtaskIndex);
    }

    // this is overridden to avoid complex mock injection through the "containingTask"
    @Override
    public ExecutionConfig getExecutionConfig() {
        ExecutionConfig cfg = new ExecutionConfig();
        cfg.setAutoWatermarkInterval(100);
        return cfg;
    }

    public static <T> SourceOperator<T, SimpleSourceSplit> createTestOperator(
            SourceReader<T, SimpleSourceSplit> reader,
            WatermarkStrategy<T> watermarkStrategy,
            boolean emitProgressiveWatermarks)
            throws Exception {

        AbstractStateBackend abstractStateBackend = new HashMapStateBackend();
        Environment env = new MockEnvironmentBuilder().build();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        final OperatorStateStore operatorStateStore =
                abstractStateBackend.createOperatorStateBackend(
                        new OperatorStateBackendParametersImpl(
                                env,
                                "test-operator",
                                Collections.emptyList(),
                                cancelStreamRegistry));

        final StateInitializationContext stateContext =
                new StateInitializationContextImpl(null, operatorStateStore, null, null, null);

        TestProcessingTimeService timeService = new TestProcessingTimeService();
        timeService.setCurrentTime(Integer.MAX_VALUE); // start somewhere that is not zero

        final SourceOperator<T, SimpleSourceSplit> sourceOperator =
                new TestingSourceOperator<>(
                        new StreamOperatorParameters<>(
                                new SourceOperatorStreamTask<Integer>(new DummyEnvironment()),
                                new MockStreamConfig(new Configuration(), 1),
                                new MockOutput<>(new ArrayList<>()),
                                null,
                                null,
                                null),
                        reader,
                        watermarkStrategy,
                        timeService,
                        emitProgressiveWatermarks);
        sourceOperator.initializeState(stateContext);
        sourceOperator.open();

        return sourceOperator;
    }

    private static class TestingOperatorEventGateway implements OperatorEventGateway {
        @Override
        public void sendEventToCoordinator(OperatorEvent event) {}
    }
}
