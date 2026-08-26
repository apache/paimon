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

package org.apache.paimon.flink.service;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link QueryFileMonitor}. */
public class QueryFileMonitorTest {

    private static final long DISCOVERY_INTERVAL_MS = 3_000L;

    @TempDir Path tempDir;

    private Table table;

    @BeforeEach
    public void before() throws Exception {
        Catalog catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(new org.apache.paimon.fs.Path(tempDir.toUri())));
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .primaryKey("a")
                        .option("bucket", "1")
                        .option(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(), "3 s")
                        .build();
        Identifier identifier = Identifier.create("default", "t");
        catalog.createDatabase("default", false);
        catalog.createTable(identifier, schema, false);
        this.table = catalog.getTable(identifier);
    }

    @Test
    public void testPollWithoutNewFilesReportsNothingAvailable() throws Exception {
        SourceReader<InternalRow, SimpleSourceSplit> reader =
                new QueryFileMonitor(table).createReader(null);
        try {
            reader.start();
            TestingReaderOutput<InternalRow> output = new TestingReaderOutput<>();

            InputStatus status = reader.pollNext(output);

            assertThat(status).isEqualTo(InputStatus.NOTHING_AVAILABLE);
            assertThat(output.getEmittedRecords()).isEmpty();
            // the poll must not block the mailbox thread for the discovery interval
            assertThat(reader.isAvailable().isDone()).isFalse();
            // availability is restored once the discovery interval has elapsed
            reader.isAvailable().get(DISCOVERY_INTERVAL_MS * 10, TimeUnit.MILLISECONDS);
        } finally {
            reader.close();
        }
    }

    @Test
    public void testPollWithNewFilesReportsMoreAvailable() throws Exception {
        writeToTable(1, 2, 3);
        SourceReader<InternalRow, SimpleSourceSplit> reader =
                new QueryFileMonitor(table).createReader(null);
        try {
            reader.start();
            TestingReaderOutput<InternalRow> output = new TestingReaderOutput<>();

            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
            assertThat(output.getEmittedRecords()).isNotEmpty();
            assertThat(reader.isAvailable().isDone()).isTrue();
        } finally {
            reader.close();
        }
    }

    @Test
    public void testConcurrentWaitsDoNotBlockEachOther() throws Exception {
        ManuallyTriggeredScheduledExecutorService timer =
                new ManuallyTriggeredScheduledExecutorService();
        QueryFileMonitor monitor = new QueryFileMonitor(table);
        SourceReader<InternalRow, SimpleSourceSplit> first = monitor.createReaderWithTimer(timer);
        SourceReader<InternalRow, SimpleSourceSplit> second = monitor.createReaderWithTimer(timer);
        try {
            first.start();
            second.start();
            TestingReaderOutput<InternalRow> output = new TestingReaderOutput<>();

            assertThat(first.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
            assertThat(second.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);

            // each wait is a queued timer task instead of a thread sleeping for the interval, so
            // the second reader's delay is already pending while the first one has not elapsed
            assertThat(timer.getAllNonPeriodicScheduledTask()).hasSize(2);
            assertThat(first.isAvailable().isDone()).isFalse();
            assertThat(second.isAvailable().isDone()).isFalse();

            timer.triggerNonPeriodicScheduledTasks();

            assertThat(first.isAvailable().isDone()).isTrue();
            assertThat(second.isAvailable().isDone()).isTrue();
        } finally {
            first.close();
            second.close();
        }
    }

    @Test
    public void testCloseCancelsThePendingWait() throws Exception {
        ManuallyTriggeredScheduledExecutorService timer =
                new ManuallyTriggeredScheduledExecutorService();
        SourceReader<InternalRow, SimpleSourceSplit> reader =
                new QueryFileMonitor(table).createReaderWithTimer(timer);
        reader.start();

        assertThat(reader.pollNext(new TestingReaderOutput<>()))
                .isEqualTo(InputStatus.NOTHING_AVAILABLE);
        ScheduledFuture<?> wakeUp = timer.getAllNonPeriodicScheduledTask().get(0);
        assertThat(wakeUp.isCancelled()).isFalse();

        reader.close();

        // the delay does not outlive the reader
        assertThat(wakeUp.isCancelled()).isTrue();
        assertThat(reader.isAvailable().isDone()).isTrue();
    }

    private void writeToTable(int a, int b, int c) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        write.write(GenericRow.of(a, b, c));
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(write.prepareCommit());
        write.close();
        commit.close();
    }

    private static final class TestingReaderOutput<E> implements ReaderOutput<E> {

        private final ArrayList<E> emittedRecords = new ArrayList<>();

        @Override
        public void collect(E record) {
            emittedRecords.add(record);
        }

        @Override
        public void collect(E record, long timestamp) {
            collect(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markIdle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markActive() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceOutput<E> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}

        public ArrayList<E> getEmittedRecords() {
            return emittedRecords;
        }
    }
}
