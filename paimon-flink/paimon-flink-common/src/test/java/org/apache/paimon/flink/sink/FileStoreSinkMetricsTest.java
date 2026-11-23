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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.operation.FileSystemWriteRestore;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.testutils.MetricListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for sink manifest scan metrics. */
class FileStoreSinkMetricsTest extends TableTestBase {

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void testWriterRestoreScanMetrics(boolean primaryKey, boolean replaceRestore) throws Exception {
        FileStoreTable table = createTable(primaryKey);
        write(table, GenericRow.of(1, 10));
        write(table, GenericRow.of(2, 20));

        MetricListener metrics = new MetricListener();
        try (TableWriteImpl<?> writer = table.newWrite("metrics")) {
            writer.withIOManager(ioManager);
            writer.withMetricRegistry(new FlinkMetricRegistry(metrics.getMetricGroup()));
            if (replaceRestore) {
                writer.withWriteRestore(
                        new FileSystemWriteRestore(
                                table.coreOptions(),
                                table.snapshotManager(),
                                table.store().newScan(),
                                table.store().newIndexFileHandler()));
            }
            Histogram duration = scanDuration(metrics, table);
            assertThat(duration.getCount()).isZero();
            assertThat(scanGauge(metrics, table, "lastScanDuration").getValue()).isZero();

            // Creating the partition's writer restores its existing files from the table.
            writer.write(GenericRow.of(1, 11));
            assertThat(duration.getCount()).isEqualTo(1);
            assertThat(scanGauge(metrics, table, "lastScannedSnapshotId").getValue()).isEqualTo(2L);
            assertThat(scanGauge(metrics, table, "lastScannedManifests").getValue()).isEqualTo(1L);
            assertThat(scanGauge(metrics, table, "lastScanResultedTableFiles").getValue())
                    .isEqualTo(1L);
            assertThat(scanGauge(metrics, table, "lastScanDuration").getValue())
                    .isGreaterThanOrEqualTo(0L);

            // Further rows for an initialized writer do not perform another restore scan.
            writer.write(GenericRow.of(1, 12));
            assertThat(duration.getCount()).isEqualTo(1);

            writer.write(GenericRow.of(2, 21));
            assertThat(duration.getCount()).isEqualTo(2);
            assertThat(scanGauge(metrics, table, "lastScannedManifests").getValue()).isEqualTo(1L);
            assertThat(scanGauge(metrics, table, "lastScanResultedTableFiles").getValue())
                    .isEqualTo(1L);
        }
    }

    @Test
    void testSkippedRestoreDoesNotReportScan() throws Exception {
        FileStoreTable table = createTable(true);
        write(table, GenericRow.of(1, 10));

        MetricListener metrics = new MetricListener();
        try (TableWriteImpl<?> writer = table.newWrite("metrics")) {
            writer.withIOManager(ioManager);
            writer.withMetricRegistry(new FlinkMetricRegistry(metrics.getMetricGroup()));
            writer.withIgnorePreviousFiles(true);
            writer.write(GenericRow.of(1, 11));

            assertThat(scanDuration(metrics, table).getCount()).isZero();
            assertThat(scanGauge(metrics, table, "lastScannedSnapshotId").getValue()).isZero();
            assertThat(scanGauge(metrics, table, "lastScanDuration").getValue()).isZero();
        }
    }

    private FileStoreTable createTable(boolean primaryKey) throws Exception {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("id", DataTypes.INT())
                        .partitionKeys("pt")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .option(CoreOptions.WRITE_ONLY.key(), "true");
        if (primaryKey) {
            schema.primaryKey("pt", "id");
        }
        catalog.createTable(identifier(), schema.build(), false);
        return getTableDefault();
    }

    private Gauge<Long> scanGauge(MetricListener metrics, FileStoreTable table, String name) {
        return metrics.<Long>getGauge("paimon", "table", table.name(), "scan", name)
                .orElseThrow(AssertionError::new);
    }

    private Histogram scanDuration(MetricListener metrics, FileStoreTable table) {
        return metrics.getHistogram("paimon", "table", table.name(), "scan", "scanDuration")
                .orElseThrow(AssertionError::new);
    }
}
