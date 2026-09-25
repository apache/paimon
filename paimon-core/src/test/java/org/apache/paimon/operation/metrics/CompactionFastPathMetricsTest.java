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

package org.apache.paimon.operation.metrics;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricGroupImpl;
import org.apache.paimon.metrics.TestMetricRegistry;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CompactionFastPathMetrics}. */
public class CompactionFastPathMetricsTest {

    private static final String TABLE_NAME = "myTable";

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testMetricRegistration() {
        CompactionFastPathMetrics metrics =
                new CompactionFastPathMetrics(new TestMetricRegistry(), TABLE_NAME);
        MetricGroup metricGroup = metrics.getMetricGroup();

        assertThat(metricGroup.getGroupName()).isEqualTo("compactionFastPath");
        assertThat(metricGroup.getAllVariables()).containsEntry("table", TABLE_NAME);
        assertThat(metricGroup.getMetrics())
                .containsKey(CompactionFastPathMetrics.HIT_COUNT)
                .hasSize(1 + CompactionFastPathMetrics.MissReason.values().length);
    }

    @Test
    public void testCloseClosesMetricGroup() {
        TrackingMetricRegistry registry = new TrackingMetricRegistry();
        CompactionFastPathMetrics metrics = new CompactionFastPathMetrics(registry, TABLE_NAME);

        TrackingMetricGroup group = registry.group("compactionFastPath");
        assertThat(group).isNotNull();
        assertThat(group.closed).isFalse();

        metrics.close();
        assertThat(group.closed).isTrue();
    }

    @Test
    public void testWriterClosesMetricGroupWhenEnabled() throws Exception {
        BaseAppendFileStoreWrite write = createAppendWrite(true);
        TrackingMetricRegistry registry = new TrackingMetricRegistry();
        write.withMetricRegistry(registry);

        TrackingMetricGroup group = registry.group("compactionFastPath");
        assertThat(group).isNotNull();
        assertThat(group.closed).isFalse();

        write.close();
        assertThat(group.closed).isTrue();
    }

    @Test
    public void testWriterSkipsMetricGroupWhenDisabled() throws Exception {
        BaseAppendFileStoreWrite write = createAppendWrite(false);
        TrackingMetricRegistry registry = new TrackingMetricRegistry();
        write.withMetricRegistry(registry);

        assertThat(registry.group("compactionFastPath")).isNull();

        write.close();
    }

    private BaseAppendFileStoreWrite createAppendWrite(boolean rowGroupCopyEnabled)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path(tempDir.toString());

        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.option("bucket", "-1");
        if (rowGroupCopyEnabled) {
            schemaBuilder.option(
                    CoreOptions.APPEND_COMPACTION_ROW_GROUP_COPY_ENABLED.key(), "true");
        }
        TableSchema tableSchema =
                new FileSystemSchemaManager(fileIO, path).createTable(schemaBuilder.build());

        FileStoreTable table = FileStoreTableFactory.create(fileIO, path, tableSchema);
        return (BaseAppendFileStoreWrite) table.store().newWrite(UUID.randomUUID().toString());
    }

    /** A {@link TestMetricRegistry} that tracks created groups and their close state. */
    private static class TrackingMetricRegistry extends TestMetricRegistry {

        private final Map<String, TrackingMetricGroup> groups = new HashMap<>();

        @Override
        public MetricGroup createMetricGroup(String groupName, Map<String, String> variables) {
            TrackingMetricGroup group = new TrackingMetricGroup(groupName, variables);
            groups.put(groupName, group);
            return group;
        }

        private TrackingMetricGroup group(String groupName) {
            return groups.get(groupName);
        }
    }

    /** A {@link MetricGroupImpl} that records whether it has been closed. */
    private static class TrackingMetricGroup extends MetricGroupImpl {

        private boolean closed = false;

        private TrackingMetricGroup(String groupName, Map<String, String> variables) {
            super(groupName, variables);
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
