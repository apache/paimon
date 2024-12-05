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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

/**
 * The operator that reads the {@link Split splits} received from the preceding {@link
 * MonitorFunction}. Contrary to the {@link MonitorFunction} which has a parallelism of 1, this
 * operator can have DOP > 1.
 */
public class ReadOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<Split, RowData> {

    private static final long serialVersionUID = 1L;

    private final ReadBuilder readBuilder;

    private transient TableRead read;
    private transient StreamRecord<RowData> reuseRecord;
    private transient FlinkRowData reuseRow;
    private transient IOManager ioManager;

    private transient FileStoreSourceReaderMetrics sourceReaderMetrics;
    // we create our own gauge for currentEmitEventTimeLag and sourceIdleTime, because this operator
    // is not a FLIP-27
    // source and Flink can't automatically calculate this metric
    private transient long emitEventTimeLag = FileStoreSourceReaderMetrics.UNDEFINED;
    private transient long idleStartTime = FileStoreSourceReaderMetrics.UNDEFINED;
    private transient Counter numRecordsIn;

    public ReadOperator(ReadBuilder readBuilder) {
        this.readBuilder = readBuilder;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.sourceReaderMetrics = new FileStoreSourceReaderMetrics(getMetricGroup());
        getMetricGroup().gauge(MetricNames.CURRENT_EMIT_EVENT_TIME_LAG, () -> emitEventTimeLag);
        getMetricGroup().gauge(MetricNames.SOURCE_IDLE_TIME, this::getIdleTime);
        this.numRecordsIn =
                InternalSourceReaderMetricGroup.wrap(getMetricGroup())
                        .getIOMetricGroup()
                        .getNumRecordsInCounter();

        this.ioManager =
                IOManager.create(
                        getContainingTask()
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectoriesPaths());
        this.read = readBuilder.newRead().withIOManager(ioManager);
        this.reuseRow = new FlinkRowData(null);
        this.reuseRecord = new StreamRecord<>(reuseRow);
        this.idlingStarted();
    }

    @Override
    public void processElement(StreamRecord<Split> record) throws Exception {
        Split split = record.getValue();
        // update metric when reading a new split
        long eventTime =
                ((DataSplit) split)
                        .earliestFileCreationEpochMillis()
                        .orElse(FileStoreSourceReaderMetrics.UNDEFINED);
        sourceReaderMetrics.recordSnapshotUpdate(eventTime);
        // update idleStartTime when reading a new split
        idleStartTime = FileStoreSourceReaderMetrics.UNDEFINED;

        boolean firstRecord = true;
        try (CloseableIterator<InternalRow> iterator =
                read.createReader(split).toCloseableIterator()) {
            while (iterator.hasNext()) {
                emitEventTimeLag = System.currentTimeMillis() - eventTime;

                // each Split is already counted as one input record,
                // so we don't need to count the first record
                if (firstRecord) {
                    firstRecord = false;
                } else {
                    numRecordsIn.inc();
                }

                reuseRow.replace(iterator.next());
                output.collect(reuseRecord);
            }
        }
        // start idle when data sending is completed
        this.idlingStarted();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (ioManager != null) {
            ioManager.close();
        }
    }

    private void idlingStarted() {
        if (!isIdling()) {
            idleStartTime = System.currentTimeMillis();
        }
    }

    private boolean isIdling() {
        return idleStartTime != FileStoreSourceReaderMetrics.UNDEFINED;
    }

    private long getIdleTime() {
        return isIdling() ? System.currentTimeMillis() - idleStartTime : 0;
    }
}
