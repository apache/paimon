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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.table.source.ReadBuilder;

import org.apache.flink.util.FlinkRuntimeException;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.paimon.flink.FlinkConnectorOptions.CheckpointAlignMode;

/** {@link AlignedSourceReader} for {@link CheckpointAlignMode#STRICTLY}. */
public class StrictlyAlignedSourceReader extends AlignedSourceReader {

    public StrictlyAlignedSourceReader(
            ReadBuilder readBuilder, long scanInterval, boolean emitSnapshotWatermark) {
        super(
                readBuilder,
                scanInterval,
                emitSnapshotWatermark,
                Executors.newScheduledThreadPool(
                        1,
                        r ->
                                new Thread(
                                        r,
                                        "Strictly aligned source scan for "
                                                + Thread.currentThread().getName())));
    }

    @VisibleForTesting
    public StrictlyAlignedSourceReader(
            ReadBuilder readBuilder,
            long scanInterval,
            boolean emitSnapshotWatermark,
            ScheduledExecutorService executors) {
        super(readBuilder, scanInterval, emitSnapshotWatermark, executors);
    }

    @Override
    protected boolean blockingAfterSendSnapshot(boolean hasPendingCheckpoint) {
        return true;
    }

    @Override
    protected boolean shouldTriggerCheckpoint(boolean blocking) {
        return blocking;
    }

    @Override
    protected void handleCheckpointCompletedOutOfOrder(long checkpointId) {
        throw new FlinkRuntimeException(
                String.format(
                        "The alignment mode of strictly requires that the checkpoint %s must be completed in order.",
                        checkpointId));
    }

    @Override
    protected void handleCheckpointAborted(long checkpointId) {
        throw new FlinkRuntimeException(
                String.format(
                        "The alignment mode of strictly requires that the checkpoint %s must be successful.",
                        checkpointId));
    }
}
