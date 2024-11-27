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

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.source.FileStoreSourceReader;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.FileStoreSourceSplitState;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.table.source.TableRead;

import org.apache.flink.api.connector.source.ExternallyInducedSourceReader;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

/**
 * The difference with {@link FileStoreSourceReader} is that only after the allocated splits are
 * fully consumed, checkpoints will be made and the next batch of splits will be requested.
 */
public class AlignedSourceReader extends FileStoreSourceReader
        implements ExternallyInducedSourceReader<RowData, FileStoreSourceSplit> {

    private final FutureCompletingBlockingQueue<
                    RecordsWithSplitIds<BulkFormat.RecordIterator<RowData>>>
            elementsQueue;
    private Long nextCheckpointId;

    public AlignedSourceReader(
            SourceReaderContext readerContext,
            TableRead tableRead,
            FileStoreSourceReaderMetrics metrics,
            IOManager ioManager,
            @Nullable Long limit,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<BulkFormat.RecordIterator<RowData>>>
                    elementsQueue) {
        super(readerContext, tableRead, metrics, ioManager, limit);
        this.elementsQueue = elementsQueue;
        this.nextCheckpointId = null;
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof CheckpointEvent) {
            nextCheckpointId = ((CheckpointEvent) sourceEvent).getCheckpointId();
            elementsQueue.notifyAvailable();
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    @Override
    protected void onSplitFinished(Map<String, FileStoreSourceSplitState> finishedSplitIds) {
        // ignore
    }

    @Override
    public Optional<Long> shouldTriggerCheckpoint() {
        if (getNumberOfCurrentlyAssignedSplits() == 0 && nextCheckpointId != null) {
            long checkpointId = nextCheckpointId;
            nextCheckpointId = null;
            context.sendSplitRequest();
            return Optional.of(checkpointId);
        }
        return Optional.empty();
    }
}
