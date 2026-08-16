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

package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Operator that collects local partition data statistics and sends them to coordinator for global
 * aggregation, then forwards global statistics to the downstream partitioner.
 */
public class DataStatisticsOperator extends AbstractStreamOperator<StatisticsOrRecord>
        implements OneInputStreamOperator<InternalRow, StatisticsOrRecord>, OperatorEventHandler {

    private static final long serialVersionUID = 1L;

    private final String operatorName;
    private final TableSchema schema;
    private final OperatorEventGateway operatorEventGateway;

    private transient int subtaskIndex;
    private transient DataStatistics localStatistics;
    private transient RowPartitionKeyExtractor extractor;
    private transient TypeSerializer<DataStatistics> statisticsSerializer;

    DataStatisticsOperator(
            StreamOperatorParameters<StatisticsOrRecord> parameters,
            String operatorName,
            TableSchema schema,
            OperatorEventGateway operatorEventGateway) {
        super();
        this.operatorName = operatorName;
        this.schema = schema;
        this.operatorEventGateway = operatorEventGateway;
        this.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
    }

    @Override
    public void open() throws Exception {
        this.extractor = new RowPartitionKeyExtractor(schema);
        this.statisticsSerializer = new DataStatisticsSerializer();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        this.subtaskIndex = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
        this.localStatistics = StatisticsUtil.createDataStatistics();
    }

    @Override
    public void handleOperatorEvent(OperatorEvent event) {
        checkArgument(
                event instanceof StatisticsEvent,
                String.format(
                        "Operator %s subtask %s received unexpected operator event %s",
                        operatorName, subtaskIndex, event.getClass()));
        StatisticsEvent statisticsEvent = (StatisticsEvent) event;
        LOG.debug(
                "Operator {} subtask {} received global data event from coordinator checkpoint {}",
                operatorName,
                subtaskIndex,
                statisticsEvent.getCheckpointId());
        DataStatistics globalStatistics =
                StatisticsUtil.deserializeDataStatistics(
                        statisticsEvent.getStatisticsBytes(), statisticsSerializer);
        if (globalStatistics != null) {
            output.collect(new StreamRecord<>(StatisticsOrRecord.fromStatistics(globalStatistics)));
        }
    }

    @Override
    public void processElement(StreamRecord<InternalRow> streamRecord) throws Exception {
        InternalRow row = streamRecord.getValue();
        BinaryRow partition = extractor.partition(row).copy();
        localStatistics.add(partition, 1L);
        output.collect(new StreamRecord<>(StatisticsOrRecord.fromRecord(row)));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        long checkpointId = context.getCheckpointId();
        LOG.debug(
                "Operator {} subtask {} sending local statistics to coordinator for checkpoint {}",
                operatorName,
                subtaskIndex,
                checkpointId);
        operatorEventGateway.sendEventToCoordinator(
                StatisticsEvent.createStatisticsEvent(
                        checkpointId, localStatistics, statisticsSerializer));
        localStatistics = StatisticsUtil.createDataStatistics();
    }
}
