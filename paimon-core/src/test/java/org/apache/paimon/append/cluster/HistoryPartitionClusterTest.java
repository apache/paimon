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
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.append.cluster.IncrementalClusterManagerTest.writeOnce;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HistoryPartitionCluster}. */
public class HistoryPartitionClusterTest {

    @TempDir java.nio.file.Path tempDir;

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
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toString()));
        return FileStoreTableFactory.create(
                LocalFileIO.create(),
                new Path(tempDir.toString()),
                schemaManager.createTable(schema));
    }
}
