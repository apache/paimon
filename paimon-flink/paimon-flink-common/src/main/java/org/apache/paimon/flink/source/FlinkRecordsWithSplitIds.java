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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.NestedProjectedRowData;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.utils.Reference;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.file.src.reader.BulkFormat.RecordIterator;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A {@link RecordsWithSplitIds} owning one physical batch. By default the batch is emitted as one
 * iterator record. An optional emission budget exposes bounded views of the same batch, allowing
 * checkpoints between views without recycling the underlying batch early.
 */
public class FlinkRecordsWithSplitIds implements RecordsWithSplitIds<RecordIterator<RowData>> {

    @Nullable private String splitId;

    @Nullable private Reference<RecordIterator<RowData>> recordsForSplitCurrent;

    @Nullable private final RecordIterator<RowData> recordsForSplit;

    private final Set<String> finishedSplits;

    @Nullable private final BoundedIterator boundedIterator;

    private boolean recycled;

    private FlinkRecordsWithSplitIds(
            @Nullable String splitId,
            @Nullable RecordIterator<RowData> recordsForSplit,
            Set<String> finishedSplits,
            int maxRecordsPerPoll) {
        checkArgument(maxRecordsPerPoll >= 0, "scan.max-records-per-poll must not be negative.");
        this.splitId = splitId;
        this.recordsForSplit = recordsForSplit;
        this.finishedSplits = finishedSplits;
        this.boundedIterator =
                recordsForSplit != null && maxRecordsPerPoll > 0
                        ? new BoundedIterator(recordsForSplit, maxRecordsPerPoll)
                        : null;
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
    public RecordIterator<RowData> nextRecordFromSplit() {
        if (this.recordsForSplitCurrent == null) {
            throw new IllegalStateException();
        }

        RecordIterator<RowData> recordsForSplit = this.recordsForSplitCurrent.get();
        if (recordsForSplit != null && boundedIterator != null) {
            if (boundedIterator.startNext()) {
                return boundedIterator;
            }
            this.recordsForSplitCurrent.set(null);
            return null;
        }
        this.recordsForSplitCurrent.set(null);
        return recordsForSplit;
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }

    @Override
    public synchronized void recycle() {
        if (!recycled && recordsForSplit != null) {
            recycled = true;
            recordsForSplit.releaseBatch();
        }
    }

    public static FlinkRecordsWithSplitIds forRecords(
            String splitId, RecordIterator<RowData> recordsForSplit) {
        return forRecords(splitId, recordsForSplit, 0);
    }

    public static FlinkRecordsWithSplitIds forRecords(
            String splitId, RecordIterator<RowData> recordsForSplit, int maxRecordsPerPoll) {
        return new FlinkRecordsWithSplitIds(
                splitId, recordsForSplit, Collections.emptySet(), maxRecordsPerPoll);
    }

    public static FlinkRecordsWithSplitIds finishedSplit(String splitId) {
        return new FlinkRecordsWithSplitIds(null, null, Collections.singleton(splitId), 0);
    }

    private static class BoundedIterator implements RecordIterator<RowData> {

        private final RecordIterator<RowData> delegate;
        private final int budget;
        private int remaining;
        private boolean exhausted;
        @Nullable private RecordAndPosition<RowData> first;

        private BoundedIterator(RecordIterator<RowData> delegate, int budget) {
            this.delegate = delegate;
            this.budget = budget;
        }

        private boolean startNext() {
            if (exhausted) {
                return false;
            }
            first = delegate.next();
            exhausted = first == null;
            remaining = budget;
            return !exhausted;
        }

        @Nullable
        @Override
        public RecordAndPosition<RowData> next() {
            if (remaining == 0 || exhausted) {
                return null;
            }
            remaining--;
            if (first != null) {
                RecordAndPosition<RowData> record = first;
                first = null;
                return record;
            }
            RecordAndPosition<RowData> record = delegate.next();
            exhausted = record == null;
            return record;
        }

        @Override
        public void releaseBatch() {
            // A view does not own the batch. Only FlinkRecordsWithSplitIds.recycle releases it.
        }
    }

    public static void emitRecord(
            SourceReaderContext context,
            RecordIterator<RowData> element,
            SourceOutput<RowData> output,
            FileStoreSourceSplitState state,
            FileStoreSourceReaderMetrics metrics,
            @Nullable NestedProjectedRowData nestedProjectedRowData) {
        long timestamp = TimestampAssigner.NO_TIMESTAMP;
        if (metrics.getLatestFileCreationTime() != FileStoreSourceReaderMetrics.UNDEFINED) {
            timestamp = metrics.getLatestFileCreationTime();
        }

        // This metric only counts the number of RecordIterator<RowData> emitted,
        // however what we really want is to count the number of RowData emitted,
        // so we replenish the missing record count here.
        org.apache.flink.metrics.Counter numRecordsIn =
                context.metricGroup().getIOMetricGroup().getNumRecordsInCounter();
        boolean firstRecord = true;

        RecordAndPosition<RowData> record;
        while ((record = element.next()) != null) {
            // First record in the iterator is already counted by SourceReaderBase#pollNext.
            if (firstRecord) {
                firstRecord = false;
            } else {
                numRecordsIn.inc();
            }

            RowData rowData = record.getRecord();
            if (nestedProjectedRowData != null) {
                rowData = nestedProjectedRowData.replaceRow(rowData);
            }
            output.collect(rowData, timestamp);
            state.setPosition(record);
        }
    }
}
