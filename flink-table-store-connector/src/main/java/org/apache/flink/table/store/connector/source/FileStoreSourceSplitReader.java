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

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.file.src.impl.FileRecords;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.store.connector.FlinkRowData;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.table.source.TableRead;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

/** The {@link SplitReader} implementation for the file store source. */
public class FileStoreSourceSplitReader
        implements SplitReader<
                RecordAndPosition<org.apache.flink.table.data.RowData>, FileStoreSourceSplit> {

    private final TableRead tableRead;

    @Nullable private final Long limit;

    private final Queue<FileStoreSourceSplit> splits;

    private final Pool<FileStoreRecordIterator> pool;

    @Nullable private RecordReader<InternalRow> currentReader;
    @Nullable private String currentSplitId;
    private long currentNumRead;
    private RecordReader.RecordIterator<InternalRow> currentFirstBatch;

    public FileStoreSourceSplitReader(TableRead tableRead, @Nullable Long limit) {
        this.tableRead = tableRead;
        this.limit = limit;
        this.splits = new LinkedList<>();
        this.pool = new Pool<>(1);
        this.pool.add(new FileStoreRecordIterator());
    }

    @Override
    public RecordsWithSplitIds<RecordAndPosition<org.apache.flink.table.data.RowData>> fetch()
            throws IOException {
        checkSplitOrStartNext();

        // pool first, pool size is 1, the underlying implementation does not allow multiple batches
        // to be read at the same time
        FileStoreRecordIterator iterator = pool();

        RecordReader.RecordIterator<InternalRow> nextBatch;
        if (currentFirstBatch != null) {
            nextBatch = currentFirstBatch;
            currentFirstBatch = null;
        } else {
            nextBatch = reachLimit() ? null : currentReader.readBatch();
        }
        if (nextBatch == null) {
            pool.recycler().recycle(iterator);
            return finishSplit();
        }
        return FileRecords.forRecords(currentSplitId, iterator.replace(nextBatch));
    }

    private boolean reachLimit() {
        return limit != null && currentNumRead >= limit;
    }

    private FileStoreRecordIterator pool() throws IOException {
        try {
            return this.pool.pollEntry();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted");
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<FileStoreSourceSplit> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }

        splits.addAll(splitsChange.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (currentReader != null) {
            currentReader.close();
        }
    }

    private void checkSplitOrStartNext() throws IOException {
        if (currentReader != null) {
            return;
        }

        final FileStoreSourceSplit nextSplit = splits.poll();
        if (nextSplit == null) {
            throw new IOException("Cannot fetch from another split - no split remaining");
        }

        currentSplitId = nextSplit.splitId();
        currentReader = tableRead.createReader(nextSplit.split());
        currentNumRead = nextSplit.recordsToSkip();
        if (currentNumRead > 0) {
            seek(currentNumRead);
        }
    }

    private void seek(long toSkip) throws IOException {
        while (true) {
            RecordReader.RecordIterator<InternalRow> nextBatch = currentReader.readBatch();
            if (nextBatch == null) {
                throw new RuntimeException(
                        String.format(
                                "skip(%s) more than the number of remaining elements.", toSkip));
            }
            while (toSkip > 0 && nextBatch.next() != null) {
                toSkip--;
            }
            if (toSkip == 0) {
                currentFirstBatch = nextBatch;
                return;
            }
            nextBatch.releaseBatch();
        }
    }

    private FileRecords<org.apache.flink.table.data.RowData> finishSplit() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }

        final FileRecords<org.apache.flink.table.data.RowData> finishRecords =
                FileRecords.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishRecords;
    }

    private class FileStoreRecordIterator
            implements BulkFormat.RecordIterator<org.apache.flink.table.data.RowData> {

        private RecordReader.RecordIterator<InternalRow> iterator;

        private final MutableRecordAndPosition<org.apache.flink.table.data.RowData>
                recordAndPosition = new MutableRecordAndPosition<>();

        public FileStoreRecordIterator replace(RecordReader.RecordIterator<InternalRow> iterator) {
            this.iterator = iterator;
            this.recordAndPosition.set(null, RecordAndPosition.NO_OFFSET, currentNumRead);
            return this;
        }

        @Nullable
        @Override
        public RecordAndPosition<org.apache.flink.table.data.RowData> next() {
            if (reachLimit()) {
                return null;
            }
            InternalRow row;
            try {
                row = iterator.next();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (row == null) {
                return null;
            }

            recordAndPosition.setNext(new FlinkRowData(row));
            currentNumRead++;
            return recordAndPosition;
        }

        @Override
        public void releaseBatch() {
            this.iterator.releaseBatch();
            pool.recycler().recycle(this);
        }
    }
}
