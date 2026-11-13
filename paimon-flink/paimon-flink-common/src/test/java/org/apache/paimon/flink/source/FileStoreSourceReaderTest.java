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
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
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
import org.apache.paimon.utils.IteratorRecordReader;

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

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newSourceSplit;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    public void testCheckpointDuringFetchAndRestore() throws Exception {
        TableRead tableRead = mock(TableRead.class, RETURNS_SELF);
        CountDownLatch preparing = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        when(tableRead.createReader(any(Split.class)))
                .thenAnswer(
                        invocation -> {
                            preparing.countDown();
                            assertThat(resume.await(15, TimeUnit.SECONDS)).isTrue();
                            return changelogRecords();
                        });
        List<FileStoreSourceSplit> checkpoint;
        try (FileStoreSourceReader reader = createReader(new TestingReaderContext(), tableRead)) {
            reader.addSplits(Collections.singletonList(createTestFileSplit("id1")));
            reader.start();
            try {
                assertThat(preparing.await(10, TimeUnit.SECONDS)).isTrue();
                assertTimeoutPreemptively(
                        Duration.ofSeconds(5),
                        () ->
                                assertThat(reader.snapshotState(1L))
                                        .extracting(FileStoreSourceSplit::recordsToSkip)
                                        .containsExactly(0L));
            } finally {
                resume.countDown();
            }
            TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
            while (output.getEmittedRecords().isEmpty()) {
                reader.isAvailable().get(10, TimeUnit.SECONDS);
                reader.pollNext(output);
            }
            assertThat(output.getEmittedRecords())
                    .extracting(row -> row.getRowKind().shortString() + " " + row.getLong(0))
                    .containsExactly("-U 1", "+U 2");
            checkpoint = reader.snapshotState(2L);
            assertThat(checkpoint)
                    .extracting(FileStoreSourceSplit::recordsToSkip)
                    .containsExactly(2L);
        }
        verify(tableRead).close();
        TableRead restoredRead = mock(TableRead.class, RETURNS_SELF);
        when(restoredRead.createReader(any(Split.class))).thenReturn(changelogRecords());
        try (FileStoreSourceReader restored =
                createReader(new TestingReaderContext(), restoredRead)) {
            restored.addSplits(checkpoint);
            restored.start();
            TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
            while (restored.getNumberOfCurrentlyAssignedSplits() > 0) {
                restored.isAvailable().get(10, TimeUnit.SECONDS);
                restored.pollNext(output);
            }
            assertThat(output.getEmittedRecords())
                    .extracting(row -> row.getRowKind().shortString() + " " + row.getLong(0))
                    .containsExactly("+I 3");
        }
    }

    private static RecordReader<InternalRow> changelogRecords() throws Exception {
        List<InternalRow> update =
                Arrays.asList(
                        GenericRow.ofKind(RowKind.UPDATE_BEFORE, 1L),
                        GenericRow.ofKind(RowKind.UPDATE_AFTER, 2L));
        List<InternalRow> insert = Collections.singletonList(GenericRow.of(3L));
        return ConcatRecordReader.create(
                () -> new IteratorRecordReader<>(update.iterator()),
                () -> new IteratorRecordReader<>(insert.iterator()));
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
