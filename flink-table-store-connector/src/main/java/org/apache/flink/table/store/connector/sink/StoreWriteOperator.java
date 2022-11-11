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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.log.LogWriteCallback;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.table.store.table.sink.LogSinkFunction;
import org.apache.flink.table.store.table.sink.SinkRecord;
import org.apache.flink.table.store.table.sink.TableWrite;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** A {@link PrepareCommitOperator} to write records. */
public class StoreWriteOperator extends PrepareCommitOperator {

    private static final long serialVersionUID = 1L;

    protected final FileStoreTable table;

    /**
     * This commitUser is valid only for new jobs. After the job starts, this commitUser will be
     * recorded into the states of write and commit operators. When the job restarts, commitUser
     * will be recovered from states and this value is ignored.
     */
    private final String initialCommitUser;

    @Nullable private final Map<String, String> overwritePartition;

    @Nullable private final LogSinkFunction logSinkFunction;

    private transient SimpleContext sinkContext;

    /** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
    private long currentWatermark = Long.MIN_VALUE;

    @Nullable protected transient TableWrite write;

    /** This is the real commit user read from state. */
    @Nullable protected transient String commitUser;

    @Nullable private transient LogWriteCallback logCallback;

    public StoreWriteOperator(
            FileStoreTable table,
            String initialCommitUser,
            @Nullable Map<String, String> overwritePartition,
            @Nullable LogSinkFunction logSinkFunction) {
        this.table = table;
        this.initialCommitUser = initialCommitUser;
        this.overwritePartition = overwritePartition;
        this.logSinkFunction = logSinkFunction;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Committable>> output) {
        super.setup(containingTask, config, output);
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

        // each job can only have one user name and this name must be consistent across restarts
        // we cannot use job id as commit user name here because user may change job id by creating
        // a savepoint, stop the job and then resume from savepoint
        commitUser =
                StateUtils.getSingleValueFromState(
                        context, "commit_user_state", String.class, initialCommitUser);
        // see comments of StateUtils.getSingleValueFromState for why commitUser may be null
        if (commitUser == null) {
            write = null;
        } else {
            write =
                    table.newWrite(commitUser)
                            .withIOManager(getContainingTask().getEnvironment().getIOManager())
                            .withOverwrite(overwritePartition != null);
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.sinkContext = new SimpleContext(getProcessingTimeService());
        if (logSinkFunction != null) {
            FunctionUtils.openFunction(logSinkFunction, new Configuration());
            logCallback = new LogWriteCallback();
            logSinkFunction.setWriteCallback(logCallback);
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
    public void processElement(StreamRecord<RowData> element) throws Exception {
        writeRecord(element);
    }

    protected SinkRecord writeRecord(StreamRecord<RowData> element) throws Exception {
        sinkContext.timestamp = element.hasTimestamp() ? element.getTimestamp() : null;

        SinkRecord record;
        try {
            record = write.write(element.getValue());
        } catch (Exception e) {
            throw new IOException(e);
        }

        if (logSinkFunction != null) {
            // write to log store, need to preserve original pk (which includes partition fields)
            SinkRecord logRecord = write.recordConverter().convertToLogSinkRecord(record);
            logSinkFunction.invoke(logRecord, sinkContext);
        }

        return record;
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
        if (write != null) {
            write.close();
        }

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
    protected List<Committable> prepareCommit(boolean doCompaction, long checkpointId)
            throws IOException {
        List<Committable> committables = new ArrayList<>();
        if (write != null) {
            try {
                for (FileCommittable committable :
                        write.prepareCommit(doCompaction, checkpointId)) {
                    committables.add(
                            new Committable(checkpointId, Committable.Kind.FILE, committable));
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        if (logCallback != null) {
            try {
                logSinkFunction.flush();
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
}
