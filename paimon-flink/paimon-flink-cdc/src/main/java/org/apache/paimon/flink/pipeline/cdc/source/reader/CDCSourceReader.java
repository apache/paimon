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

package org.apache.paimon.flink.pipeline.cdc.source.reader;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.pipeline.cdc.source.CDCSource;
import org.apache.paimon.flink.pipeline.cdc.source.TableAwareFileStoreSourceSplit;
import org.apache.paimon.flink.source.FileStoreSourceSplitState;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.file.src.reader.BulkFormat.RecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** A {@link SourceReader} that read records from {@link TableAwareFileStoreSourceSplit}. */
public class CDCSourceReader
        extends SingleThreadMultiplexSourceReaderBase<
                RecordIterator<Event>,
                Event,
                TableAwareFileStoreSourceSplit,
                FileStoreSourceSplitState> {
    private static final Logger LOG = LoggerFactory.getLogger(CDCSourceReader.class);

    private final IOManager ioManager;

    public CDCSourceReader(
            SourceReaderContext readerContext,
            FileStoreSourceReaderMetrics metrics,
            IOManager ioManager,
            CDCSource.TableManager tableManager) {
        super(
                () -> new CDCSourceSplitReader(metrics, tableManager),
                (element, output, state) ->
                        CDCRecordsWithSplitIds.emitRecord(
                                readerContext, element, output, state, metrics),
                readerContext.getConfiguration(),
                readerContext);
        this.ioManager = ioManager;
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, FileStoreSourceSplitState> finishedSplitIds) {
        // this method is called each time when we consume one split
        // it is possible that one response from the coordinator contains multiple splits
        // we should only require for more splits after we've consumed all given splits
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected FileStoreSourceSplitState initializedState(TableAwareFileStoreSourceSplit split) {
        LOG.info("Initializing split {}", split);
        return new FileStoreSourceSplitState(split);
    }

    @Override
    protected TableAwareFileStoreSourceSplit toSplitType(
            String splitId, FileStoreSourceSplitState splitState) {
        LOG.info("Converting split state {} with id {} to split", splitState, splitId);
        return (TableAwareFileStoreSourceSplit) splitState.toSourceSplit();
    }

    @Override
    public void close() throws Exception {
        super.close();
        ioManager.close();
    }
}
