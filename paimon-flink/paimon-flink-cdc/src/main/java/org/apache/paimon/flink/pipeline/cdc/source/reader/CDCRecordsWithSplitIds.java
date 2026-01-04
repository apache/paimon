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

import org.apache.paimon.flink.source.FileStoreSourceSplitState;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.utils.Reference;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.file.src.reader.BulkFormat.RecordIterator;
import org.apache.flink.connector.file.src.util.RecordAndPosition;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Set;

/**
 * A {@link RecordsWithSplitIds} which contains only one iterator record. This can ensure that there
 * will be no checkpoint segmentation in iterator consumption.
 */
public class CDCRecordsWithSplitIds implements RecordsWithSplitIds<RecordIterator<Event>> {

    @Nullable private String splitId;

    @Nullable private Reference<RecordIterator<Event>> recordsForSplitCurrent;

    @Nullable private final RecordIterator<Event> recordsForSplit;

    private final Set<String> finishedSplits;

    private CDCRecordsWithSplitIds(
            @Nullable String splitId,
            @Nullable RecordIterator<Event> recordsForSplit,
            Set<String> finishedSplits) {
        this.splitId = splitId;
        this.recordsForSplit = recordsForSplit;
        this.finishedSplits = finishedSplits;
    }

    @Nullable
    @Override
    public String nextSplit() {
        // move the split one (from current value to null)
        final String nextSplit = this.splitId;
        this.splitId = null;

        // move the iterator, from null to value (if first move) or to null (if second move)
        this.recordsForSplitCurrent =
                nextSplit != null ? new Reference<>(this.recordsForSplit) : null;

        return nextSplit;
    }

    @Nullable
    @Override
    public RecordIterator<Event> nextRecordFromSplit() {
        if (this.recordsForSplitCurrent == null) {
            throw new IllegalStateException();
        }

        RecordIterator<Event> recordsForSplit = this.recordsForSplitCurrent.get();
        this.recordsForSplitCurrent.set(null);
        return recordsForSplit;
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }

    @Override
    public void recycle() {
        if (recordsForSplit != null) {
            recordsForSplit.releaseBatch();
        }
    }

    public static CDCRecordsWithSplitIds forRecords(
            String splitId, RecordIterator<Event> recordsForSplit) {
        return new CDCRecordsWithSplitIds(splitId, recordsForSplit, Collections.emptySet());
    }

    public static CDCRecordsWithSplitIds finishedSplit(String splitId) {
        return new CDCRecordsWithSplitIds(null, null, Collections.singleton(splitId));
    }

    public static void emitRecord(
            SourceReaderContext context,
            RecordIterator<Event> element,
            SourceOutput<Event> output,
            FileStoreSourceSplitState state,
            FileStoreSourceReaderMetrics metrics) {
        long timestamp = TimestampAssigner.NO_TIMESTAMP;
        if (metrics.getLatestFileCreationTime() != FileStoreSourceReaderMetrics.UNDEFINED) {
            timestamp = metrics.getLatestFileCreationTime();
        }

        // This metric only counts the number of RecordIterator<Event> emitted,
        // however what we really want is to count the number of Events emitted,
        // so we replenish the missing record count here.
        org.apache.flink.metrics.Counter numRecordsIn =
                context.metricGroup().getIOMetricGroup().getNumRecordsInCounter();
        boolean firstRecord = true;

        RecordAndPosition<Event> record;
        while ((record = element.next()) != null) {
            // First record in the iterator is already counted by SourceReaderBase#pollNext.
            if (firstRecord) {
                firstRecord = false;
            } else {
                numRecordsIn.inc();
            }

            Event event = record.getRecord();
            output.collect(event, timestamp);
            state.setPosition(record);
        }
    }
}
