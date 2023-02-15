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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.file.src.impl.FileRecords;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;

/** Records construction and emitter in source. */
public interface RecordsFunction<T> extends RecordEmitter<T, RowData, FileStoreSourceSplitState> {

    /** Create a {@link RecordsWithSplitIds} to emit records. */
    RecordsWithSplitIds<T> createRecords(
            String splitId, BulkFormat.RecordIterator<RowData> recordsForSplit);

    /** Create a {@link RecordsWithSplitIds} to indicate that the split is finished. */
    RecordsWithSplitIds<T> createRecordsWithFinishedSplit(String splitId);

    /** Emit records for {@code element}. */
    void emitRecord(T element, SourceOutput<RowData> output, FileStoreSourceSplitState state)
            throws Exception;

    static IterateRecordsFunction forIterate() {
        return new IterateRecordsFunction();
    }

    static SingleRecordsFunction forSingle() {
        return new SingleRecordsFunction();
    }

    /** Return one by one, possibly inserting checkpoint in the middle. */
    class IterateRecordsFunction implements RecordsFunction<RecordAndPosition<RowData>> {

        @Override
        public RecordsWithSplitIds<RecordAndPosition<RowData>> createRecords(
                String splitId, BulkFormat.RecordIterator<RowData> recordsForSplit) {
            return FileRecords.forRecords(splitId, recordsForSplit);
        }

        @Override
        public RecordsWithSplitIds<RecordAndPosition<RowData>> createRecordsWithFinishedSplit(
                String splitId) {
            return FileRecords.finishedSplit(splitId);
        }

        @Override
        public void emitRecord(
                RecordAndPosition<RowData> element,
                SourceOutput<RowData> output,
                FileStoreSourceSplitState state) {
            output.collect(element.getRecord());
            state.setPosition(element);
        }
    }

    /**
     * Return per iterator instead of per record. This can ensure that there will be no checkpoint
     * segmentation in iterator consumption.
     */
    class SingleRecordsFunction implements RecordsFunction<BulkFormat.RecordIterator<RowData>> {

        @Override
        public RecordsWithSplitIds<BulkFormat.RecordIterator<RowData>> createRecords(
                String splitId, BulkFormat.RecordIterator<RowData> recordsForSplit) {
            return SingleIteratorRecords.forRecords(splitId, recordsForSplit);
        }

        @Override
        public RecordsWithSplitIds<BulkFormat.RecordIterator<RowData>>
                createRecordsWithFinishedSplit(String splitId) {
            return SingleIteratorRecords.finishedSplit(splitId);
        }

        @Override
        public void emitRecord(
                BulkFormat.RecordIterator<RowData> element,
                SourceOutput<RowData> output,
                FileStoreSourceSplitState state) {
            RecordAndPosition<RowData> record;
            while ((record = element.next()) != null) {
                output.collect(record.getRecord());
                state.setPosition(record);
            }
        }
    }
}
