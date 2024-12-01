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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.log.LogWriteCallback;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;

/** A {@link PrepareCommitOperator} to write {@link InternalRow}. Record schema is fixed. */
public class RowDataStoreWriteOperator extends TableWriteOperator<InternalRow> {

    private static final long serialVersionUID = 3L;

    @Nullable private final LogSinkFunction logSinkFunction;
    private transient SimpleContext sinkContext;
    @Nullable private transient LogWriteCallback logCallback;

    /** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
    private long currentWatermark = Long.MIN_VALUE;

    protected RowDataStoreWriteOperator(
            StreamOperatorParameters<Committable> parameters,
            FileStoreTable table,
            @Nullable LogSinkFunction logSinkFunction,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        super(parameters, table, storeSinkWriteProvider, initialCommitUser);
        this.logSinkFunction = logSinkFunction;
        if (logSinkFunction != null) {
            FunctionUtils.setFunctionRuntimeContext(logSinkFunction, getRuntimeContext());
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        if (logSinkFunction != null) {
            StreamingFunctionUtils.restoreFunctionState(context, logSinkFunction);
        }
    }

    @Override
    protected boolean containLogSystem() {
        return logSinkFunction != null;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.sinkContext = new SimpleContext(getProcessingTimeService());
        if (logSinkFunction != null) {
            openFunction(logSinkFunction);
            logCallback = new LogWriteCallback();
            logSinkFunction.setWriteCallback(logCallback);
        }
    }

    private static void openFunction(Function function) throws Exception {
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;

            try {
                Method method = RichFunction.class.getDeclaredMethod("open", OpenContext.class);
                method.invoke(richFunction, new OpenContext() {});
                return;
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                // to stay compatible with Flink 1.18-
            }

            Method method = RichFunction.class.getDeclaredMethod("open", Configuration.class);
            method.invoke(richFunction, new Configuration());
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);

        this.currentWatermark = mark.getTimestamp();
        if (logSinkFunction != null) {
            logSinkFunction.writeWatermark(
                    new org.apache.flink.api.common.eventtime.Watermark(mark.getTimestamp()));
        }
    }

    @Override
    public void processElement(StreamRecord<InternalRow> element) throws Exception {
        sinkContext.timestamp = element.hasTimestamp() ? element.getTimestamp() : null;

        SinkRecord record;
        try {
            record = write.write(element.getValue());
        } catch (Exception e) {
            throw new IOException(e);
        }

        if (record != null && logSinkFunction != null) {
            // write to log store, need to preserve original pk (which includes partition fields)
            SinkRecord logRecord = write.toLogRecord(record);
            logSinkFunction.invoke(logRecord, sinkContext);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        if (logSinkFunction != null) {
            StreamingFunctionUtils.snapshotFunctionState(
                    context, getOperatorStateBackend(), logSinkFunction);
        }
    }

    @Override
    public void finish() throws Exception {
        super.finish();

        if (logSinkFunction != null) {
            logSinkFunction.finish();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (logSinkFunction != null) {
            FunctionUtils.closeFunction(logSinkFunction);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);

        if (logSinkFunction instanceof CheckpointListener) {
            ((CheckpointListener) logSinkFunction).notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);

        if (logSinkFunction instanceof CheckpointListener) {
            ((CheckpointListener) logSinkFunction).notifyCheckpointAborted(checkpointId);
        }
    }

    @Override
    protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<Committable> committables = super.prepareCommit(waitCompaction, checkpointId);

        if (logCallback != null) {
            try {
                Objects.requireNonNull(logSinkFunction).flush();
            } catch (Exception e) {
                throw new IOException(e);
            }
            logCallback
                    .offsets()
                    .forEach(
                            (k, v) ->
                                    committables.add(
                                            new Committable(
                                                    checkpointId,
                                                    Committable.Kind.LOG_OFFSET,
                                                    new LogOffsetCommittable(k, v))));
        }

        return committables;
    }

    private class SimpleContext implements SinkFunction.Context {

        @Nullable private Long timestamp;

        private final ProcessingTimeService processingTimeService;

        public SimpleContext(ProcessingTimeService processingTimeService) {
            this.processingTimeService = processingTimeService;
        }

        @Override
        public long currentProcessingTime() {
            return processingTimeService.getCurrentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        public Long timestamp() {
            return timestamp;
        }
    }

    /** {@link StreamOperatorFactory} of {@link RowDataStoreWriteOperator}. */
    public static class Factory extends TableWriteOperator.Factory<InternalRow> {

        @Nullable private final LogSinkFunction logSinkFunction;

        public Factory(
                FileStoreTable table,
                @Nullable LogSinkFunction logSinkFunction,
                StoreSinkWrite.Provider storeSinkWriteProvider,
                String initialCommitUser) {
            super(table, storeSinkWriteProvider, initialCommitUser);
            this.logSinkFunction = logSinkFunction;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<Committable>> T createStreamOperator(
                StreamOperatorParameters<Committable> parameters) {
            return (T)
                    new RowDataStoreWriteOperator(
                            parameters,
                            table,
                            logSinkFunction,
                            storeSinkWriteProvider,
                            initialCommitUser);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return RowDataStoreWriteOperator.class;
        }
    }
}
