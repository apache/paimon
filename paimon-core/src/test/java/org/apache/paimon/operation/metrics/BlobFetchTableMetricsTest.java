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
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricGroupImpl;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies the table option reaches a native Blob writer, prepare, commit and readback. */
class BlobFetchTableMetricsTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void tableOptionControlsOnlyAdditionalCounters(boolean enabled) throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.BLOB())
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(
                                CoreOptions.BLOB_TRANSFER_METRICS_ENABLED.key(),
                                Boolean.toString(enabled))
                        .build(),
                false);
        FileStoreTable table = getTableDefault();
        Map<String, MetricGroup> groups = new HashMap<>();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.withMetricRegistry(
                    (name, variables) -> {
                        MetricGroup group = new MetricGroupImpl(name, variables);
                        groups.put(name, group);
                        return group;
                    });
            write.write(GenericRow.of(1, Blob.fromData(new byte[] {1, 2, 3})));
            commit.commit(write.prepareCommit());
            MetricGroup fetch = groups.get(BlobFetchMetrics.GROUP_NAME);
            assertThat(fetch.getMetrics()).hasSize(enabled ? 18 : 13);
            assertThat(
                            ((Counter)
                                            fetch.getMetrics()
                                                    .get(BlobFetchMetrics.BLOB_FETCH_SUCCESS_BYTES))
                                    .getCount())
                    .isEqualTo(3);
            if (enabled) {
                assertThat(
                                ((Counter)
                                                fetch.getMetrics()
                                                        .get(
                                                                BlobFetchMetrics
                                                                        .BLOB_FETCH_TRANSFER_TIMED_COUNT))
                                        .getCount())
                        .isEqualTo(1);
            }
        }
        assertThat(table.snapshotManager().latestSnapshot()).isNotNull();
        assertThat(read(table))
                .singleElement()
                .satisfies(row -> assertThat(row.getBlob(1).toData()).containsExactly(1, 2, 3));
    }
}
