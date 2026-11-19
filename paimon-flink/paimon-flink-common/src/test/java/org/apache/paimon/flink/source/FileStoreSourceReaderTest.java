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

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.source.KeyValueTableRead;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newSourceSplit;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
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
    public void testCheckpointAndRestoreInsideBatch() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tempDir.toString());
        FileStoreSourceSplit split =
                newSourceSplit(
                        "s",
                        row(1),
                        0,
                        rw.writeFiles(
                                row(1),
                                0,
                                Arrays.asList(
                                        Tuple2.of(1L, 10L),
                                        Tuple2.of(2L, 20L),
                                        Tuple2.of(3L, 30L),
                                        Tuple2.of(4L, 40L))));
        Counter inputCounter = new SimpleCounter();
        SourceReaderMetricGroup metricGroup =
                mock(SourceReaderMetricGroup.class, RETURNS_DEEP_STUBS);
        when(metricGroup.getIOMetricGroup().getNumRecordsInCounter()).thenReturn(inputCounter);
        TestingReaderContext context = new TestingReaderContext(new Configuration(), metricGroup);
        List<FileStoreSourceSplit> checkpoint;
        try (FileStoreSourceReader reader = createBudgetedReader(context, rw)) {
            reader.addSplits(Collections.singletonList(split));
            reader.start();
            TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
            for (int i = 1; i <= 2; i++) {
                reader.isAvailable().get(30, TimeUnit.SECONDS);
                reader.pollNext(output);
                assertThat(output.getEmittedRecords()).hasSize(i);
                assertThat(output.getEmittedRecords().get(i - 1).getLong(0)).isEqualTo(i * 10L);
                assertThat(reader.snapshotState(1L).get(0).recordsToSkip()).isEqualTo(i);
            }
            assertThat(inputCounter.getCount()).isEqualTo(2);
            checkpoint = reader.snapshotState(1L);
            // Round-trip the existing state format, rather than depending on in-memory objects.
            FileStoreSourceSplitSerializer serializer = new FileStoreSourceSplitSerializer();
            checkpoint =
                    Collections.singletonList(
                            serializer.deserialize(
                                    serializer.getVersion(),
                                    serializer.serialize(checkpoint.get(0))));
        }
        try (FileStoreSourceReader restored =
                createBudgetedReader(new TestingReaderContext(), rw)) {
            restored.addSplits(checkpoint);
            restored.start();
            TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
            for (int i = 1; i <= 2; i++) {
                restored.isAvailable().get(30, TimeUnit.SECONDS);
                restored.pollNext(output);
                assertThat(output.getEmittedRecords()).hasSize(i);
                assertThat(output.getEmittedRecords().get(i - 1).getLong(0))
                        .isEqualTo((i + 2) * 10L);
            }
        }
    }

    private FileStoreSourceReader createBudgetedReader(
            TestingReaderContext context, TestChangelogDataReadWrite rw) {
        return new FileStoreSourceReader(
                context,
                rw.createReadWithKey(),
                new FileStoreSourceReaderMetrics(new DummyMetricGroup()),
                IOManager.create(tempDir.toString()),
                null,
                null,
                null,
                false,
                1);
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
