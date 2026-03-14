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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.pipeline.cdc.source.CDCSource;
import org.apache.paimon.flink.pipeline.cdc.source.TableAwareFileStoreSourceSplit;
import org.apache.paimon.flink.pipeline.cdc.util.PaimonToFlinkCDCDataConverter;
import org.apache.paimon.flink.source.FileStoreSourceSplitReader;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Pool;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.paimon.flink.pipeline.cdc.util.PaimonToFlinkCDCDataConverter.convertRowToDataChangeEvent;
import static org.apache.paimon.flink.pipeline.cdc.util.PaimonToFlinkCDCTypeConverter.convertPaimonSchemaToFlinkCDCSchema;

/** The {@link SplitReader} implementation for the {@link CDCSource}. */
public class CDCSourceSplitReader
        implements SplitReader<BulkFormat.RecordIterator<Event>, TableAwareFileStoreSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(CDCSourceSplitReader.class);

    private final Queue<TableAwareFileStoreSourceSplit> splits;

    private final Pool<FileStoreRecordIterator> pool;

    private final CDCSource.TableManager tableManager;
    private TableReaderInfo currentTableReaderInfo;
    @Nullable private LazyRecordReader currentReader;
    @Nullable private String currentSplitId;
    private long currentNumRead;
    private RecordIterator<InternalRow> currentFirstBatch;

    private boolean paused;
    private final AtomicBoolean wakeup;
    private final FileStoreSourceReaderMetrics metrics;

    public CDCSourceSplitReader(
            FileStoreSourceReaderMetrics metrics, CDCSource.TableManager tableManager) {
        this.splits = new LinkedList<>();
        this.pool = new Pool<>(1);
        this.pool.add(new FileStoreRecordIterator());
        this.paused = false;
        this.metrics = metrics;
        this.wakeup = new AtomicBoolean(false);
        this.tableManager = tableManager;
    }

    @Override
    public RecordsWithSplitIds<BulkFormat.RecordIterator<Event>> fetch() throws IOException {
        if (paused) {
            return new FileStoreSourceSplitReader.EmptyRecordsWithSplitIds<>();
        }

        checkSplitOrStartNext();

        // poll from the pool first, pool size is 1, the underlying implementation does not allow
        // multiple batches to be read at the same time
        FileStoreRecordIterator iterator = poll();
        if (iterator == null) {
            LOG.info("Skip waiting for object pool due to wakeup");
            return new FileStoreSourceSplitReader.EmptyRecordsWithSplitIds<>();
        }

        RecordIterator<InternalRow> nextBatch;
        if (currentFirstBatch != null) {
            nextBatch = currentFirstBatch;
            currentFirstBatch = null;
        } else {
            nextBatch = Objects.requireNonNull(currentReader).recordReader().readBatch();
        }
        if (nextBatch == null) {
            pool.recycler().recycle(iterator);
            return finishSplit();
        }
        return CDCRecordsWithSplitIds.forRecords(
                currentSplitId, iterator.replace(nextBatch, currentTableReaderInfo));
    }

    @Nullable
    private FileStoreRecordIterator poll() throws IOException {
        FileStoreRecordIterator iterator = null;
        while (iterator == null && !wakeup.get()) {
            try {
                iterator = this.pool.pollEntry(Duration.ofSeconds(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted");
            }
        }
        if (wakeup.get()) {
            wakeup.compareAndSet(true, false);
        }
        return iterator;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<TableAwareFileStoreSourceSplit> splitsChange) {
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
            Collection<TableAwareFileStoreSourceSplit> splitsToPause,
            Collection<TableAwareFileStoreSourceSplit> splitsToResume) {
        for (TableAwareFileStoreSourceSplit split : splitsToPause) {
            if (split.splitId().equals(currentSplitId)) {
                paused = true;
                break;
            }
        }

        for (TableAwareFileStoreSourceSplit split : splitsToResume) {
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

        final TableAwareFileStoreSourceSplit nextSplit = splits.poll();
        if (nextSplit == null) {
            return;
        }

        // update metric when split changes
        if (nextSplit.split() instanceof DataSplit) {
            long eventTime =
                    ((DataSplit) nextSplit.split())
                            .earliestFileCreationEpochMillis()
                            .orElse(FileStoreSourceReaderMetrics.UNDEFINED);
            metrics.recordSnapshotUpdate(eventTime);
        }

        Identifier identifier = nextSplit.getIdentifier();
        TableSchema tableSchema = tableManager.getTableSchema(identifier, nextSplit.getSchemaId());
        List<SchemaChangeEvent> schemaChangeEvents =
                tableManager.generateSchemaChangeEventList(
                        identifier, nextSplit.getLastSchemaId(), nextSplit.getSchemaId());
        currentTableReaderInfo = new TableReaderInfo(identifier, tableSchema, schemaChangeEvents);
        currentSplitId = nextSplit.splitId();
        currentReader = createLazyRecordReader(nextSplit.split());
        currentNumRead = nextSplit.recordsToSkip();

        if (currentNumRead > 0) {
            seek(currentNumRead);
        }
    }

    @VisibleForTesting
    protected LazyRecordReader createLazyRecordReader(Split split) {
        return new LazyRecordReader(split, currentTableReaderInfo, tableManager);
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

    private CDCRecordsWithSplitIds finishSplit() throws IOException {
        if (currentReader != null) {
            if (currentReader.lazyRecordReader != null) {
                currentReader.lazyRecordReader.close();
            }
            currentReader = null;
        }

        final CDCRecordsWithSplitIds finishRecords =
                CDCRecordsWithSplitIds.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishRecords;
    }

    private class FileStoreRecordIterator implements BulkFormat.RecordIterator<Event> {

        private RecordIterator<InternalRow> iterator;

        private final MutableRecordAndPosition<Event> recordAndPosition =
                new MutableRecordAndPosition<>();

        private TableReaderInfo tableReaderInfo;
        private final Queue<SchemaChangeEvent> schemaChangeEventList = new LinkedList<>();

        public FileStoreRecordIterator replace(
                RecordIterator<InternalRow> iterator, TableReaderInfo tableReaderInfo) {
            this.iterator = iterator;
            this.recordAndPosition.set(null, RecordAndPosition.NO_OFFSET, currentNumRead);
            this.tableReaderInfo = tableReaderInfo;
            this.schemaChangeEventList.addAll(tableReaderInfo.schemaChangeEvents);
            return this;
        }

        @Nullable
        @Override
        public RecordAndPosition<Event> next() {
            Event event = nextEvent();
            if (event == null) {
                return null;
            }

            recordAndPosition.setNext(event);
            currentNumRead++;
            return recordAndPosition;
        }

        private Event nextEvent() {
            if (!schemaChangeEventList.isEmpty()) {
                return schemaChangeEventList.poll();
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

            return convertRowToDataChangeEvent(
                    tableReaderInfo.tableId,
                    row,
                    tableReaderInfo.fieldGetters,
                    tableReaderInfo.generator);
        }

        @Override
        public void releaseBatch() {
            this.iterator.releaseBatch();
            pool.recycler().recycle(this);
        }
    }

    /** Lazy to create {@link RecordReader} to improve performance for limit. */
    protected static class LazyRecordReader {

        protected final Split split;
        private final TableReaderInfo currentTableReaderInfo;
        private final CDCSource.TableManager tableManager;
        private RecordReader<InternalRow> lazyRecordReader;

        protected LazyRecordReader(
                Split split,
                TableReaderInfo currentTableReaderInfo,
                CDCSource.TableManager tableManager) {
            this.split = split;
            this.currentTableReaderInfo = currentTableReaderInfo;
            this.tableManager = tableManager;
        }

        public RecordReader<InternalRow> recordReader() throws IOException {
            if (lazyRecordReader == null) {
                lazyRecordReader =
                        tableManager
                                .getTableRead(
                                        currentTableReaderInfo.identifier,
                                        currentTableReaderInfo.currentSchema)
                                .createReader(split);
            }
            return lazyRecordReader;
        }
    }

    private static class TableReaderInfo {
        private final Identifier identifier;
        private final TableId tableId;
        private final TableSchema currentSchema;
        private final List<SchemaChangeEvent> schemaChangeEvents;
        private final BinaryRecordDataGenerator generator;
        private final List<InternalRow.FieldGetter> fieldGetters;

        private TableReaderInfo(
                Identifier identifier,
                TableSchema currentSchema,
                List<SchemaChangeEvent> schemaChangeEvents) {
            this.identifier = identifier;
            this.tableId = TableId.tableId(identifier.getDatabaseName(), identifier.getTableName());

            this.currentSchema = currentSchema;
            this.schemaChangeEvents = schemaChangeEvents;

            org.apache.flink.cdc.common.schema.Schema currentCDCSchema =
                    convertPaimonSchemaToFlinkCDCSchema(currentSchema);
            this.generator =
                    new BinaryRecordDataGenerator(
                            currentCDCSchema.getColumnDataTypes().toArray(new DataType[0]));
            this.fieldGetters =
                    PaimonToFlinkCDCDataConverter.createFieldGetters(
                            currentCDCSchema.getColumnDataTypes());
        }
    }
}
