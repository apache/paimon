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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.utils.Pool;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/** The {@link SplitReader} implementation for the file store source. */
public class FileStoreSourceSplitReader
        implements SplitReader<BulkFormat.RecordIterator<RowData>, FileStoreSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreSourceSplitReader.class);

    private final TableRead tableRead;

    @Nullable private final RecordLimiter limiter;

    private final Queue<FileStoreSourceSplit> splits;

    private final Pool<FileStoreRecordIterator> pool;

    @Nullable private LazyRecordReader currentReader;
    @Nullable private String currentSplitId;
    private long currentNumRead;
    private RecordIterator<InternalRow> currentFirstBatch;

    private boolean paused;
    private final AtomicBoolean wakeup;
    private final FileStoreSourceReaderMetrics metrics;

    public FileStoreSourceSplitReader(
            TableRead tableRead,
            @Nullable RecordLimiter limiter,
            FileStoreSourceReaderMetrics metrics) {
        this.tableRead = tableRead;
        this.limiter = limiter;
        this.splits = new LinkedList<>();
        this.pool = new Pool<>(1);
        this.pool.add(new FileStoreRecordIterator());
        this.paused = false;
        this.metrics = metrics;
        this.wakeup = new AtomicBoolean(false);
    }

    @Override
    public RecordsWithSplitIds<BulkFormat.RecordIterator<RowData>> fetch() throws IOException {
        if (paused) {
            return new EmptyRecordsWithSplitIds<>();
        }

        if (wakeup.get() && wakeup.compareAndSet(true, false)) {
            return new EmptyRecordsWithSplitIds<>();
        }

        checkSplitOrStartNext();

        // pool first, pool size is 1, the underlying implementation does not allow multiple batches
        // to be read at the same time
        FileStoreRecordIterator iterator = pool();
        if (iterator == null) {
            LOG.info("Skip waiting for object pool due to wakeup");
            return new EmptyRecordsWithSplitIds<>();
        }

        RecordIterator<InternalRow> nextBatch;
        if (currentFirstBatch != null) {
            nextBatch = currentFirstBatch;
            currentFirstBatch = null;
        } else {
            nextBatch =
                    reachLimit()
                            ? null
                            : Objects.requireNonNull(currentReader).recordReader().readBatch();
        }
        if (nextBatch == null) {
            pool.recycler().recycle(iterator);
            return finishSplit();
        }
        return FlinkRecordsWithSplitIds.forRecords(currentSplitId, iterator.replace(nextBatch));
    }

    private boolean reachLimit() {
        return limiter != null && limiter.reachLimit();
    }

    private FileStoreRecordIterator pool() throws IOException {
        FileStoreRecordIterator iterator = null;
        while (iterator == null && !wakeup.get()) {
            try {
                iterator = this.pool.pollEntry(Duration.ofSeconds(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted");
            }
        }
        if (iterator == null) {
            wakeup.compareAndSet(true, false);
        }
        return iterator;
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

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.7-.
     */
    public void pauseOrResumeSplits(
            Collection<FileStoreSourceSplit> splitsToPause,
            Collection<FileStoreSourceSplit> splitsToResume) {
        for (FileStoreSourceSplit split : splitsToPause) {
            if (split.splitId().equals(currentSplitId)) {
                paused = true;
                break;
            }
        }

        for (FileStoreSourceSplit split : splitsToResume) {
            if (split.splitId().equals(currentSplitId)) {
                paused = false;
                break;
            }
        }
    }

    @Override
    public void wakeUp() {
        wakeup.compareAndSet(false, true);
        LOG.info("Wake up the split reader.");
    }

    @Override
    public void close() throws Exception {
        if (currentReader != null) {
            if (currentReader.lazyRecordReader != null) {
                currentReader.lazyRecordReader.close();
            }
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

        // update metric when split changes
        if (nextSplit.split() instanceof DataSplit) {
            long eventTime =
                    ((DataSplit) nextSplit.split())
                            .earliestFileCreationEpochMillis()
                            .orElse(FileStoreSourceReaderMetrics.UNDEFINED);
            metrics.recordSnapshotUpdate(eventTime);
        }

        currentSplitId = nextSplit.splitId();
        currentReader = new LazyRecordReader(nextSplit.split());
        currentNumRead = nextSplit.recordsToSkip();
        if (limiter != null) {
            limiter.add(currentNumRead);
        }
        if (currentNumRead > 0) {
            seek(currentNumRead);
        }
    }

    private void seek(long toSkip) throws IOException {
        while (true) {
            RecordIterator<InternalRow> nextBatch =
                    Objects.requireNonNull(currentReader).recordReader().readBatch();
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

    private FlinkRecordsWithSplitIds finishSplit() throws IOException {
        if (currentReader != null) {
            if (currentReader.lazyRecordReader != null) {
                currentReader.lazyRecordReader.close();
            }
            currentReader = null;
        }

        final FlinkRecordsWithSplitIds finishRecords =
                FlinkRecordsWithSplitIds.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishRecords;
    }

    private class FileStoreRecordIterator implements BulkFormat.RecordIterator<RowData> {

        private RecordIterator<InternalRow> iterator;

        private final MutableRecordAndPosition<RowData> recordAndPosition =
                new MutableRecordAndPosition<>();

        public FileStoreRecordIterator replace(RecordIterator<InternalRow> iterator) {
            this.iterator = iterator;
            this.recordAndPosition.set(null, RecordAndPosition.NO_OFFSET, currentNumRead);
            return this;
        }

        @Nullable
        @Override
        public RecordAndPosition<RowData> next() {
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
            if (limiter != null) {
                limiter.increment();
            }
            return recordAndPosition;
        }

        @Override
        public void releaseBatch() {
            this.iterator.releaseBatch();
            pool.recycler().recycle(this);
        }
    }

    /** Lazy to create {@link RecordReader} to improve performance for limit. */
    private class LazyRecordReader {

        private final Split split;

        private RecordReader<InternalRow> lazyRecordReader;

        private LazyRecordReader(Split split) {
            this.split = split;
        }

        public RecordReader<InternalRow> recordReader() throws IOException {
            if (lazyRecordReader == null) {
                lazyRecordReader = tableRead.createReader(split);
            }
            return lazyRecordReader;
        }
    }

    /**
     * An empty implementation of {@link RecordsWithSplitIds}. It is used to indicate that the
     * {@link FileStoreSourceSplitReader} is paused or wakeup.
     */
    private static class EmptyRecordsWithSplitIds<T> implements RecordsWithSplitIds<T> {

        @Nullable
        @Override
        public String nextSplit() {
            return null;
        }

        @Nullable
        @Override
        public T nextRecordFromSplit() {
            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            return Collections.emptySet();
        }
    }
}
