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
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.TestMetricRegistry;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CompactionMetrics}. */
public class CompactionMetricsTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testReportMetrics() {
        CompactionMetrics metrics = new CompactionMetrics(new TestMetricRegistry(), "myTable");
        assertThat(getMetric(metrics, CompactionMetrics.MAX_LEVEL0_FILE_COUNT)).isEqualTo(-1L);
        assertThat(getMetric(metrics, CompactionMetrics.AVG_LEVEL0_FILE_COUNT)).isEqualTo(-1.0);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_THREAD_BUSY)).isEqualTo(0.0);
        assertThat(getMetric(metrics, CompactionMetrics.AVG_COMPACTION_TIME)).isEqualTo(0.0);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_COMPLETED_COUNT)).isEqualTo(0L);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_QUEUED_COUNT)).isEqualTo(0L);
        CompactionMetrics.Reporter[] reporters = new CompactionMetrics.Reporter[3];
        for (int i = 0; i < reporters.length; i++) {
            reporters[i] = metrics.createReporter(BinaryRow.EMPTY_ROW, i);
        }

        assertThat(getMetric(metrics, CompactionMetrics.MAX_LEVEL0_FILE_COUNT)).isEqualTo(0L);
        assertThat(getMetric(metrics, CompactionMetrics.AVG_LEVEL0_FILE_COUNT)).isEqualTo(0.0);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_THREAD_BUSY)).isEqualTo(0.0);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_COMPLETED_COUNT)).isEqualTo(0L);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_QUEUED_COUNT)).isEqualTo(0L);

        reporters[0].reportLevel0FileCount(5);
        reporters[1].reportLevel0FileCount(3);
        reporters[2].reportLevel0FileCount(4);
        assertThat(getMetric(metrics, CompactionMetrics.MAX_LEVEL0_FILE_COUNT)).isEqualTo(5L);
        assertThat(getMetric(metrics, CompactionMetrics.AVG_LEVEL0_FILE_COUNT)).isEqualTo(4.0);

        reporters[0].reportLevel0FileCount(8);
        assertThat(getMetric(metrics, CompactionMetrics.MAX_LEVEL0_FILE_COUNT)).isEqualTo(8L);
        assertThat(getMetric(metrics, CompactionMetrics.AVG_LEVEL0_FILE_COUNT)).isEqualTo(5.0);

        reporters[0].reportCompactionTime(300000);
        reporters[0].reportCompactionTime(250000);
        reporters[0].reportCompactionTime(270000);
        assertThat(getMetric(metrics, CompactionMetrics.AVG_COMPACTION_TIME))
                .isEqualTo(273333.3333333333);

        // enqueue compaction request
        reporters[0].increaseCompactionsQueuedCount();
        reporters[1].increaseCompactionsQueuedCount();
        reporters[2].increaseCompactionsQueuedCount();
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_COMPLETED_COUNT)).isEqualTo(0L);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_QUEUED_COUNT)).isEqualTo(3L);

        // completed compactions and remove them from queue
        reporters[0].increaseCompactionsCompletedCount();
        reporters[0].decreaseCompactionsQueuedCount();
        reporters[1].decreaseCompactionsQueuedCount();
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_COMPLETED_COUNT)).isEqualTo(1L);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_QUEUED_COUNT)).isEqualTo(1L);
    }

    @Test
    public void testTotalFileSizeForPrimaryKeyTables() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});

        int bucketNum = 2;
        Options options = new Options();
        options.set(CoreOptions.BUCKET, bucketNum);
        Schema schema =
                new Schema(
                        rowType.getFields(),
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        options.toMap(),
                        "");

        FileStoreTable table;
        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "mytable");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            table = (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.withMetricRegistry(new TestMetricRegistry());

        int numKeys = 300;
        int numRounds = 10;
        int recordsPerRound = 100;

        ThreadLocalRandom random = ThreadLocalRandom.current();
        int commitIdentifier = 0;
        for (int i = 0; i < numRounds; i++) {
            for (int j = 0; j < recordsPerRound; j++) {
                write.write(GenericRow.of(random.nextInt(numKeys), random.nextInt()));
            }

            commitIdentifier++;
            commit.commit(
                    commitIdentifier, write.prepareCommit(random.nextBoolean(), commitIdentifier));

            long[] totalFileSizes = new long[bucketNum];
            for (Split split : table.newScan().plan().splits()) {
                DataSplit dataSplit = (DataSplit) split;
                totalFileSizes[dataSplit.bucket()] +=
                        dataSplit.dataFiles().stream().mapToLong(DataFileMeta::fileSize).sum();
            }

            CompactionMetrics metrics =
                    ((AbstractFileStoreWrite<?>) write.getWrite()).compactionMetrics();
            assertThat(metrics.getTotalFileSizeStream()).hasSize(bucketNum);
            assertThat(getMetric(metrics, CompactionMetrics.MAX_TOTAL_FILE_SIZE))
                    .isEqualTo(Arrays.stream(totalFileSizes).max().orElse(0));
            assertThat(getMetric(metrics, CompactionMetrics.AVG_TOTAL_FILE_SIZE))
                    .isEqualTo(Arrays.stream(totalFileSizes).average().orElse(0));
        }

        write.close();
        commit.close();
    }

    private Object getMetric(CompactionMetrics metrics, String metricName) {
        Metric metric = metrics.getMetricGroup().getMetrics().get(metricName);
        if (metric instanceof Gauge) {
            return ((Gauge<?>) metric).getValue();
        } else {
            return ((Counter) metric).getCount();
        }
    }
}
