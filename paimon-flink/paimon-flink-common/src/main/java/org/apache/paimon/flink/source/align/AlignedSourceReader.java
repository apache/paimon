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
import org.apache.paimon.flink.NestedProjectedRowData;
import org.apache.paimon.flink.source.FileStoreSourceReader;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.FileStoreSourceSplitState;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ReflectionUtils;

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

    private static final String ELEMENTS_QUEUE_FIELD = "elementsQueue";

    private Long nextCheckpointId;
    private final FutureCompletingBlockingQueue<
                    RecordsWithSplitIds<BulkFormat.RecordIterator<RowData>>>
            elementsQueue;

    public AlignedSourceReader(
            SourceReaderContext readerContext,
            TableRead tableRead,
            FileStoreSourceReaderMetrics metrics,
            IOManager ioManager,
            @Nullable Long limit,
            @Nullable NestedProjectedRowData rowData,
            @Nullable RowType readType) {
        super(readerContext, tableRead, metrics, ioManager, limit, rowData, readType);
        this.nextCheckpointId = null;
        try {
            // In lower versions of Flink, the SplitFetcherManager does not provide the getQueue
            // method. To reduce code redundancy, we use reflection to access the elementsQueue
            // object.
            this.elementsQueue =
                    ReflectionUtils.getPrivateFieldValue(splitFetcherManager, ELEMENTS_QUEUE_FIELD);
        } catch (Exception e) {
            throw new RuntimeException("The elementsQueue object cannot be accessed.", e);
        }
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof CheckpointEvent) {
            nextCheckpointId = ((CheckpointEvent) sourceEvent).getCheckpointId();
            // The ExternallyInducedSourceReader has two scenarios:
            // 1.When the JM triggers a checkpoint, the external messages have not yet
            // arrived(common case).
            // 2.Before the JM triggers the checkpoint, the external messages have already
            // arrived(rare case).
            //
            // In case 2, if the AlignedSourceReader has finished consuming the split, the
            // shouldTriggerCheckpoint method will return empty. Since the
            // AlignedSourceReader depends on the CheckpointEvent sent by the
            // AlignedContinuousFileSplitEnumerator to decide whether to trigger a checkpoint, upon
            // receiving the CheckpointEvent, it should explicitly call
            // elementsQueue.notifyAvailable() to trigger shouldTriggerCheckpoint again.
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
