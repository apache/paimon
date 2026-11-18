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

package org.apache.paimon.append.cluster;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.apache.paimon.append.cluster.IncrementalClusterManagerTest.writeOnce;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

/** Test for {@link HistoryPartitionCluster}. */
public class HistoryPartitionClusterTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @CsvSource({
        "America/New_York,2026-03-08T07:30:00Z,90,false",
        "America/New_York,2026-11-01T07:30:00Z,165,true",
        "America/New_York,2026-11-02T07:30:00Z,90,false",
        "America/New_York,2026-11-02T07:30:00Z,165,true",
        "UTC,2026-03-08T07:30:00Z,90,false",
        "UTC,2026-03-08T07:30:00Z,165,true",
        "UTC,2026-03-08T07:30:00Z,120,false",
        "UTC,2026-03-08T07:30:00Z,121,true"
    })
    void testHistoryPartitionIdleTimeUsesElapsedDuration(
            String zoneName, String nowText, int ageMinutes, boolean eligible) throws Exception {
        ZoneId zone = ZoneId.of(zoneName);
        Instant now = Instant.parse(nowText);
        Instant created = now.minus(ageMinutes, ChronoUnit.MINUTES);
        LocalDateTime creationTime = LocalDateTime.ofInstant(created, zone);
        LocalDateTime currentTime = LocalDateTime.ofInstant(now, zone);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.CLUSTERING_HISTORY_PARTITION_IDLE_TIME.key(), "2h");
        options.put(CoreOptions.ASYNC_FILE_WRITE.key(), "false");
        FileStoreTable table = createTable(options, Collections.singletonList("f2"));
        TimeZone originalZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone(zone));
        try (MockedStatic<LocalDateTime> localTimes =
                        mockStatic(LocalDateTime.class, CALLS_REAL_METHODS);
                MockedStatic<Instant> instants = mockStatic(Instant.class, CALLS_REAL_METHODS)) {
            localTimes.when(LocalDateTime::now).thenReturn(creationTime);
            instants.when(Instant::now).thenReturn(created);
            writeOnce(
                    table,
                    GenericRow.of(
                            1,
                            1,
                            BinaryString.fromString("requested"),
                            BinaryString.fromString("v")),
                    GenericRow.of(
                            1,
                            1,
                            BinaryString.fromString("history"),
                            BinaryString.fromString("v")));
            // The persisted metadata represents the intended instant, including at DST boundaries.
            assertThat(table.store().newScan().plan().files())
                    .hasSize(2)
                    .allSatisfy(
                            entry ->
                                    assertThat(entry.file().creationTimeEpochMillis())
                                            .isEqualTo(created.toEpochMilli()));
            assertThat(table.newSnapshotReader().partitionEntries())
                    .hasSize(2)
                    .allSatisfy(
                            entry ->
                                    assertThat(entry.lastFileCreationTime())
                                            .isEqualTo(created.toEpochMilli()));

            localTimes.when(LocalDateTime::now).thenReturn(currentTime);
            instants.when(Instant::now).thenReturn(now);
            BinaryRow requested = BinaryRow.singleColumn("requested");
            BinaryRow history = BinaryRow.singleColumn("history");
            IncrementalClusterManager manager =
                    new IncrementalClusterManager(
                            table,
                            PartitionPredicate.fromMultiple(
                                    RowType.of(DataTypes.STRING()),
                                    Collections.singletonList(requested)));
            if (eligible) {
                assertThat(manager.createCompactUnits(true).keySet())
                        .containsExactlyInAnyOrder(requested, history);
            } else {
                assertThat(manager.createCompactUnits(true).keySet()).containsExactly(requested);
            }
        } finally {
            TimeZone.setDefault(originalZone);
        }
    }

    @Test
    public void testHistoryPartitionAutoClustering() throws Exception {
        FileStoreTable table = createTable(Collections.emptyMap(), Collections.singletonList("f2"));
        writeOnce(
                table,
                GenericRow.of(
                        1, 1, BinaryString.fromString("pt1"), BinaryString.fromString("test")));
        writeOnce(
                table,
                GenericRow.of(
                        1, 1, BinaryString.fromString("pt2"), BinaryString.fromString("test")));

        Thread.sleep(2000);
        writeOnce(
                table,
                GenericRow.of(
                        1, 1, BinaryString.fromString("pt3"), BinaryString.fromString("test")));
        writeOnce(
                table,
                GenericRow.of(
                        1, 1, BinaryString.fromString("pt4"), BinaryString.fromString("test")));

        // test specify history partition and enable history partition auto clustering
        HistoryPartitionCluster historyPartitionCluster =
                new IncrementalClusterManager(
                                table,
                                PartitionPredicate.fromMultiple(
                                        RowType.of(DataTypes.INT()),
                                        Lists.newArrayList(BinaryRow.singleColumn("pt1"))))
                        .historyPartitionCluster();
        Map<BinaryRow, List<LevelSortedRun>> partitionLevels =
                historyPartitionCluster.constructLevelsForHistoryPartitions();
        assertThat(partitionLevels.size()).isEqualTo(1);
        assertThat(partitionLevels.get(BinaryRow.singleColumn("pt2"))).isNotEmpty();

        // test specify non-history partition and enable history partition auto clustering
        historyPartitionCluster =
                new IncrementalClusterManager(
                                table,
                                PartitionPredicate.fromMultiple(
                                        RowType.of(DataTypes.INT()),
                                        Lists.newArrayList(BinaryRow.singleColumn("pt3"))))
                        .historyPartitionCluster();
        partitionLevels = historyPartitionCluster.constructLevelsForHistoryPartitions();
        assertThat(partitionLevels.size()).isEqualTo(1);
        assertThat(partitionLevels.get(BinaryRow.singleColumn("pt1"))).isNotEmpty();

        // test not specify partition and disable history partition auto clustering
        historyPartitionCluster = new IncrementalClusterManager(table).historyPartitionCluster();
        assertThat(historyPartitionCluster).isNull();
    }

    @Test
    public void testNoHistoryPartitionsDoesNotClusterActivePartition() throws Exception {
        FileStoreTable table =
                createTable(
                        Collections.singletonMap(
                                CoreOptions.CLUSTERING_HISTORY_PARTITION_IDLE_TIME.key(), "365d"),
                        Collections.singletonList("f2"));
        writeOnce(
                table,
                GenericRow.of(
                        1, 1, BinaryString.fromString("pt1"), BinaryString.fromString("test")));
        writeOnce(
                table,
                GenericRow.of(
                        1, 1, BinaryString.fromString("pt2"), BinaryString.fromString("test")));

        IncrementalClusterManager manager =
                new IncrementalClusterManager(
                        table,
                        PartitionPredicate.fromMultiple(
                                RowType.of(DataTypes.STRING()),
                                Collections.singletonList(BinaryRow.singleColumn("pt1"))));
        assertThat(manager.createCompactUnits(true).keySet())
                .containsExactly(BinaryRow.singleColumn("pt1"));
    }

    protected FileStoreTable createTable(
            Map<String, String> customOptions, List<String> partitionKeys) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.CLUSTERING_COLUMNS.key(), "f0,f1");
        options.put(CoreOptions.CLUSTERING_INCREMENTAL.key(), "true");
        options.put(CoreOptions.CLUSTERING_HISTORY_PARTITION_IDLE_TIME.key(), "2s");
        options.put(CoreOptions.CLUSTERING_HISTORY_PARTITION_LIMIT.key(), "1");
        options.putAll(customOptions);

        Schema schema =
                new Schema(
                        RowType.of(
                                        DataTypes.INT(),
                                        DataTypes.INT(),
                                        DataTypes.STRING(),
                                        DataTypes.STRING())
                                .getFields(),
                        partitionKeys,
                        Collections.emptyList(),
                        options,
                        "");

        SchemaManager schemaManager =
                new FileSystemSchemaManager(LocalFileIO.create(), new Path(tempDir.toString()));
        return FileStoreTableFactory.create(
                LocalFileIO.create(),
                new Path(tempDir.toString()),
                schemaManager.createTable(schema));
    }
}
