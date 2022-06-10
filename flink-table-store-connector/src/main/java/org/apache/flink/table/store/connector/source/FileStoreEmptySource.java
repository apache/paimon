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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** A dedicated empty source for "ALTER TABLE ... COMPACT". */
public class FileStoreEmptySource
        implements Source<RowData, FileStoreSourceSplit, PendingSplitsCheckpoint> {

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, FileStoreSourceSplit> createReader(
            SourceReaderContext readerContext) {
        return new SourceReader<RowData, FileStoreSourceSplit>() {
            @Override
            public void start() {}

            @Override
            public InputStatus pollNext(ReaderOutput<RowData> output) {
                return InputStatus.END_OF_INPUT;
            }

            @Override
            public List<FileStoreSourceSplit> snapshotState(long checkpointId) {
                return Collections.emptyList();
            }

            @Override
            public CompletableFuture<Void> isAvailable() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void addSplits(List<FileStoreSourceSplit> splits) {}

            @Override
            public void notifyNoMoreSplits() {}

            @Override
            public void close() {}
        };
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> createEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context) {
        return restoreEnumerator(context, null);
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        return new StaticFileStoreSplitEnumerator(context, null, Collections.emptyList());
    }

    @Override
    public FileStoreSourceSplitSerializer getSplitSerializer() {
        return new FileStoreSourceSplitSerializer();
    }

    @Override
    public PendingSplitsCheckpointSerializer getEnumeratorCheckpointSerializer() {
        return new PendingSplitsCheckpointSerializer(getSplitSerializer());
    }
}
