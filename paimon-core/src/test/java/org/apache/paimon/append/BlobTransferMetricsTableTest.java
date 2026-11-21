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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricGroupImpl;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.metrics.BlobFetchMetrics;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the table-option-to-payload-writer propagation of transfer metric enablement. */
class BlobTransferMetricsTableTest extends TableTestBase {

    @ParameterizedTest(name = "blob-transfer-metrics.enabled={0}")
    @ValueSource(strings = {"default", "false", "true"})
    void testTransferMetricsOptionPreservesFetchMetrics(String configured) throws Exception {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.BLOB())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.WRITE_ONLY.key(), "true");
        if (!"default".equals(configured)) {
            schema.option(CoreOptions.BLOB_TRANSFER_METRICS_ENABLED.key(), configured);
        }
        catalog.createTable(identifier(), schema.build(), false);
        FileStoreTable table = getTableDefault();
        RecordingMetricRegistry registry = new RecordingMetricRegistry();
        byte[] first = {1, 2, 3};
        byte[] second = {4, 5, 6, 7, 8};
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.withMetricRegistry(registry);
            assertThat(registry.groups(BlobFetchMetrics.TRANSFER_GROUP_NAME)).isEmpty();
            write.write(GenericRow.of(1, new BlobData(first)));
            write.write(GenericRow.of(2, new BlobData(second)));
            commit.commit(write.prepareCommit());

            List<MetricGroup> fetchGroups = registry.groups(BlobFetchMetrics.GROUP_NAME);
            assertThat(fetchGroups).hasSize(1);
            MetricGroup fetch = fetchGroups.get(0);
            assertThat(counter(fetch, BlobFetchMetrics.BLOB_FETCH_TOTAL)).isEqualTo(2);
            assertThat(counter(fetch, BlobFetchMetrics.BLOB_FETCH_SUCCESS)).isEqualTo(2);
            assertThat(counter(fetch, BlobFetchMetrics.BLOB_FETCH_SUCCESS_BYTES))
                    .isEqualTo(first.length + second.length);
            assertThat(counter(fetch, BlobFetchMetrics.BLOB_FETCH_FAILURE)).isZero();

            List<MetricGroup> transfers = registry.groups(BlobFetchMetrics.TRANSFER_GROUP_NAME);
            if ("true".equals(configured)) {
                assertThat(transfers).hasSize(1);
                MetricGroup transfer = transfers.get(0);
                assertThat(transfer.getAllVariables())
                        .containsEntry("table", fetch.getAllVariables().get("table"))
                        .containsEntry("sourceKind", "inline");
                assertThat(counter(transfer, BlobFetchMetrics.TRANSFER_READ_BYTES))
                        .isEqualTo(first.length + second.length);
                assertThat(counter(transfer, BlobFetchMetrics.TRANSFER_WRITTEN_BYTES))
                        .isEqualTo(first.length + second.length);
                assertThat(counter(transfer, BlobFetchMetrics.TRANSFER_COMPLETED)).isEqualTo(2);
                assertThat(counter(transfer, BlobFetchMetrics.TRANSFER_FAILED)).isZero();
                Histogram latency =
                        (Histogram)
                                transfer.getMetrics().get(BlobFetchMetrics.TRANSFER_LATENCY_MILLIS);
                assertThat(latency.getCount()).isEqualTo(2);
            } else {
                assertThat(transfers).isEmpty();
            }
        }

        Map<Integer, byte[]> actual = new HashMap<>();
        ReadBuilder read = table.newReadBuilder();
        try (RecordReader<InternalRow> reader =
                read.newRead().createReader(read.newScan().plan())) {
            reader.forEachRemaining(row -> actual.put(row.getInt(0), row.getBlob(1).toData()));
        }
        assertThat(actual).containsOnlyKeys(1, 2);
        assertThat(actual.get(1)).isEqualTo(first);
        assertThat(actual.get(2)).isEqualTo(second);
    }

    private static long counter(MetricGroup group, String name) {
        return ((Counter) group.getMetrics().get(name)).getCount();
    }

    private static class RecordingMetricRegistry implements MetricRegistry {

        private final List<MetricGroup> groups = new ArrayList<>();

        @Override
        public MetricGroup createMetricGroup(String groupName, Map<String, String> variables) {
            MetricGroup group = new MetricGroupImpl(groupName, variables);
            groups.add(group);
            return group;
        }

        private List<MetricGroup> groups(String name) {
            return groups.stream()
                    .filter(group -> name.equals(group.getGroupName()))
                    .collect(Collectors.toList());
        }
    }
}
