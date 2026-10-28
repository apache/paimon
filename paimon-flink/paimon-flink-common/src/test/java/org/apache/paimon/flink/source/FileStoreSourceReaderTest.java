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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.source.KeyValueTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newSourceSplit;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link FileStoreSourceReader}. */
public class FileStoreSourceReaderTest {

    @TempDir protected java.nio.file.Path tempDir;

    @BeforeEach
    public void beforeEach() throws Exception {
        SchemaManager schemaManager =
                new FileSystemSchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));
        schemaManager.createTable(
                new Schema(
                        new RowType(
                                        Arrays.asList(
                                                new DataField(0, "k", new BigIntType()),
                                                new DataField(1, "v", new BigIntType()),
                                                new DataField(2, "default", new IntType())))
                                .getFields(),
                        Collections.singletonList("default"),
                        Arrays.asList("k", "default"),
                        Collections.emptyMap(),
                        null));
    }

    @Test
    public void testRequestSplitWhenNoSplitRestored() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();
        final FileStoreSourceReader reader = createReader(context);

        reader.start();
        reader.close();

        assertThat(context.getNumSplitRequests()).isEqualTo(1);
    }

    @Test
    public void testNoSplitRequestWhenSplitRestored() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();
        final FileStoreSourceReader reader = createReader(context);

        reader.addSplits(Collections.singletonList(createTestFileSplit("id1")));
        reader.start();
        reader.close();

        assertThat(context.getNumSplitRequests()).isEqualTo(0);
    }

    @Test
    public void testAddMultipleSplits() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();
        final FileStoreSourceReader reader = createReader(context);

        reader.start();
        assertThat(context.getNumSplitRequests()).isEqualTo(1);

        reader.addSplits(Arrays.asList(createTestFileSplit("id1"), createTestFileSplit("id2")));
        TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
        while (reader.getNumberOfCurrentlyAssignedSplits() > 0) {
            reader.pollNext(output);
            Thread.sleep(10);
        }
        assertThat(context.getNumSplitRequests()).isEqualTo(2);
    }

    @Test
    public void testReaderOnSplitFinished() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();
        final FileStoreSourceReader reader = createReader(context);

        reader.start();
        reader.addSplits(Collections.singletonList(createTestFileSplit("id1")));
        TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
        while (reader.getNumberOfCurrentlyAssignedSplits() > 0) {
            reader.pollNext(output);
            Thread.sleep(10);
        }

        List<SourceEvent> sourceEvents = context.getSentEvents();
        assertThat(sourceEvents.size()).isEqualTo(1);
        assertThat(sourceEvents.get(0)).isExactlyInstanceOf(ReaderConsumeProgressEvent.class);
        assertThat(((ReaderConsumeProgressEvent) sourceEvents.get(0)))
                .matches(event -> event.lastConsumeSnapshotId() == 1L);
    }

    @Test
    public void testIOManagerIsSet() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        KeyValueTableRead tableRead =
                new TestChangelogDataReadWrite(tempDir.toString()).createReadWithKey();

        FileStoreSourceReader reader = createReader(context, tableRead);
        reader.addSplits(Collections.singletonList(createTestFileSplit("id1")));
        reader.start();
        reader.close();

        assertThat(tableRead.ioManager()).isNotNull();
    }

    @Test
    public void testCloseClosesTableRead() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        CloseTrackingTableRead tableRead =
                new CloseTrackingTableRead(
                        new TestChangelogDataReadWrite(tempDir.toString()).createReadWithKey());
        FileStoreSourceReader reader = createReader(context, tableRead);

        reader.close();

        assertThat(tableRead.closeCount()).isEqualTo(1);
    }

    @Test
    public void testCheckpointDuringFetchAndRestore() throws Exception {
        List<List<InternalRow>> batches =
                Arrays.asList(
                        Arrays.asList(
                                changelogRow(RowKind.UPDATE_BEFORE, 1L),
                                changelogRow(RowKind.UPDATE_AFTER, 2L)),
                        Collections.singletonList(changelogRow(RowKind.INSERT, 3L)));
        CountDownLatch preparationStarted = new CountDownLatch(1);
        CountDownLatch finishPreparation = new CountDownLatch(1);
        ExecutorService checkpointExecutor = Executors.newSingleThreadExecutor();
        FileStoreSourceReader reader =
                createReader(
                        new TestingReaderContext(),
                        new BlockingTableRead(batches, preparationStarted, finishPreparation));
        FileStoreSourceReader restoredReader = null;

        try {
            reader.addSplits(Collections.singletonList(createTestFileSplit("id1")));
            reader.start();
            assertThat(preparationStarted.await(10, TimeUnit.SECONDS)).isTrue();

            FileStoreSourceReader checkpointReader = reader;
            Future<List<FileStoreSourceSplit>> checkpointDuringPreparation =
                    checkpointExecutor.submit(() -> checkpointReader.snapshotState(1L));
            List<FileStoreSourceSplit> unconsumedSplits =
                    checkpointDuringPreparation.get(1, TimeUnit.SECONDS);
            assertThat(unconsumedSplits)
                    .singleElement()
                    .extracting(FileStoreSourceSplit::recordsToSkip)
                    .isEqualTo(0L);

            finishPreparation.countDown();
            TestingReaderOutput<RowData> outputBeforeCheckpoint = new TestingReaderOutput<>();
            pollUntilRecords(reader, outputBeforeCheckpoint, 2);
            assertThat(format(outputBeforeCheckpoint.getEmittedRecords()))
                    .containsExactly("-U 1", "+U 2");

            List<FileStoreSourceSplit> checkpoint = reader.snapshotState(2L);
            assertThat(checkpoint)
                    .singleElement()
                    .extracting(FileStoreSourceSplit::recordsToSkip)
                    .isEqualTo(2L);
            reader.close();
            reader = null;

            restoredReader =
                    createReader(new TestingReaderContext(), new BlockingTableRead(batches));
            restoredReader.addSplits(checkpoint);
            restoredReader.start();
            TestingReaderOutput<RowData> outputAfterRestore = new TestingReaderOutput<>();
            drain(restoredReader, outputAfterRestore);

            List<String> allRecords = new ArrayList<>();
            allRecords.addAll(format(outputBeforeCheckpoint.getEmittedRecords()));
            allRecords.addAll(format(outputAfterRestore.getEmittedRecords()));
            assertThat(allRecords).containsExactly("-U 1", "+U 2", "+I 3");
        } finally {
            finishPreparation.countDown();
            if (reader != null) {
                reader.close();
            }
            if (restoredReader != null) {
                restoredReader.close();
            }
            checkpointExecutor.shutdownNow();
        }
    }

    protected FileStoreSourceReader createReader(TestingReaderContext context) {
        return createReader(
                context, new TestChangelogDataReadWrite(tempDir.toString()).createReadWithKey());
    }

    protected FileStoreSourceReader createReader(
            TestingReaderContext context, TableRead tableRead) {
        return new FileStoreSourceReader(
                context,
                tableRead,
                new FileStoreSourceReaderMetrics(new DummyMetricGroup()),
                IOManager.create(tempDir.toString()),
                null,
                null,
                null,
                false);
    }

    protected static FileStoreSourceSplit createTestFileSplit(String id) {
        return newSourceSplit(id, row(1), 0, Collections.emptyList());
    }

    private static GenericRow changelogRow(RowKind kind, long value) {
        GenericRow row = GenericRow.of(value);
        row.setRowKind(kind);
        return row;
    }

    private static void pollUntilRecords(
            FileStoreSourceReader reader, TestingReaderOutput<RowData> output, int expected)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (output.getEmittedRecords().size() < expected && System.nanoTime() < deadline) {
            reader.pollNext(output);
            Thread.sleep(10);
        }
        assertThat(output.getEmittedRecords()).hasSize(expected);
    }

    private static void drain(FileStoreSourceReader reader, TestingReaderOutput<RowData> output)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (reader.getNumberOfCurrentlyAssignedSplits() > 0 && System.nanoTime() < deadline) {
            reader.pollNext(output);
            Thread.sleep(10);
        }
        assertThat(reader.getNumberOfCurrentlyAssignedSplits()).isZero();
    }

    private static List<String> format(List<RowData> records) {
        List<String> result = new ArrayList<>();
        for (RowData row : records) {
            result.add(row.getRowKind().shortString() + " " + row.getLong(0));
        }
        return result;
    }

    private static class BlockingTableRead implements TableRead {

        private final List<List<InternalRow>> batches;
        private final CountDownLatch preparationStarted;
        private final CountDownLatch finishPreparation;

        private BlockingTableRead(List<List<InternalRow>> batches) {
            this(batches, null, null);
        }

        private BlockingTableRead(
                List<List<InternalRow>> batches,
                CountDownLatch preparationStarted,
                CountDownLatch finishPreparation) {
            this.batches = batches;
            this.preparationStarted = preparationStarted;
            this.finishPreparation = finishPreparation;
        }

        @Override
        public TableRead withMetricRegistry(MetricRegistry registry) {
            return this;
        }

        @Override
        public TableRead executeFilter() {
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (preparationStarted != null) {
                preparationStarted.countDown();
                try {
                    if (!finishPreparation.await(10, TimeUnit.SECONDS)) {
                        throw new IOException("Timed out waiting for lookup preparation.");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
            return new BatchedRowsRecordReader(batches);
        }
    }

    private static class BatchedRowsRecordReader implements RecordReader<InternalRow> {

        private final List<List<InternalRow>> batches;
        private int nextBatch;

        private BatchedRowsRecordReader(List<List<InternalRow>> batches) {
            this.batches = batches;
        }

        @Override
        public RecordIterator<InternalRow> readBatch() {
            if (nextBatch >= batches.size()) {
                return null;
            }

            List<InternalRow> batch = batches.get(nextBatch++);
            return new RecordIterator<InternalRow>() {
                private int nextRow;

                @Override
                public InternalRow next() {
                    return nextRow < batch.size() ? batch.get(nextRow++) : null;
                }

                @Override
                public void releaseBatch() {}
            };
        }

        @Override
        public void close() {}
    }

    private static class CloseTrackingTableRead implements TableRead {

        private final TableRead delegate;
        private int closeCount;

        private CloseTrackingTableRead(TableRead delegate) {
            this.delegate = delegate;
        }

        private int closeCount() {
            return closeCount;
        }

        @Override
        public TableRead withMetricRegistry(MetricRegistry registry) {
            delegate.withMetricRegistry(registry);
            return this;
        }

        @Override
        public TableRead executeFilter() {
            delegate.executeFilter();
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            delegate.withIOManager(ioManager);
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            return delegate.createReader(split);
        }

        @Override
        public void close() throws IOException {
            closeCount++;
            delegate.close();
        }
    }

    /** A {@link MetricGroup} for testing. */
    public static class DummyMetricGroup implements MetricGroup {
        public DummyMetricGroup() {}

        public Counter counter(String name) {
            return null;
        }

        public <C extends Counter> C counter(String name, C counter) {
            return null;
        }

        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            return null;
        }

        public <H extends Histogram> H histogram(String name, H histogram) {
            return null;
        }

        public <M extends Meter> M meter(String name, M meter) {
            return null;
        }

        public MetricGroup addGroup(String name) {
            return null;
        }

        public MetricGroup addGroup(String key, String value) {
            return null;
        }

        public String[] getScopeComponents() {
            return new String[0];
        }

        public Map<String, String> getAllVariables() {
            return null;
        }

        public String getMetricIdentifier(String metricName) {
            return null;
        }

        public String getMetricIdentifier(String metricName, CharacterFilter filter) {
            return null;
        }
    }
}
