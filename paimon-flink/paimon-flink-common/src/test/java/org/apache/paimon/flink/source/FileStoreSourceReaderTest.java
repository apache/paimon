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
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newSourceSplit;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link FileStoreSourceReader}. */
public class FileStoreSourceReaderTest {

    @TempDir protected java.nio.file.Path tempDir;

    @BeforeEach
    public void beforeEach() throws Exception {
        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));
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

    protected FileStoreSourceReader createReader(TestingReaderContext context) {
        return new FileStoreSourceReader(
                context,
                new TestChangelogDataReadWrite(tempDir.toString()).createReadWithKey(),
                new FileStoreSourceReaderMetrics(new DummyMetricGroup()),
                IOManager.create(tempDir.toString()),
                null,
                null);
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
