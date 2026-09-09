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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link RescaleAction}. */
public class RescaleActionITCase extends ActionITCaseBase {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()},
                    new String[] {"k", "v", "pt"});

    private FileStoreTable prepareTable(int initialBuckets) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), String.valueOf(initialBuckets));
        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "k"),
                        Collections.emptyList(),
                        options);

        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = streamWriteBuilder.newWrite();
        commit = streamWriteBuilder.newCommit();
        return table;
    }

    @Test
    @Timeout(120)
    public void testRescaleAllPartitions() throws Exception {
        int initialBuckets = 2;
        int newBuckets = 4;
        prepareTable(initialBuckets);

        // Write data to two partitions
        writeData(
                rowData(1, 100, BinaryString.fromString("p1")),
                rowData(2, 200, BinaryString.fromString("p1")),
                rowData(3, 300, BinaryString.fromString("p2")),
                rowData(4, 400, BinaryString.fromString("p2")));

        // Rescale all partitions (no --partition argument)
        createAction(
                        RescaleAction.class,
                        "rescale",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--bucket_num",
                        String.valueOf(newBuckets))
                .run();

        // Verify both partitions now have the new bucket count
        FileStoreTable table = getFileStoreTable(tableName);
        Map<BinaryRow, Integer> partitionBuckets = getPartitionBucketCounts(table);
        for (Map.Entry<BinaryRow, Integer> entry : partitionBuckets.entrySet()) {
            assertThat(entry.getValue()).isEqualTo(newBuckets);
        }

        // Verify data is preserved
        List<InternalRow> rows = getData(tableName);
        assertThat(rows).hasSize(4);
    }

    @Test
    @Timeout(120)
    public void testRescaleSinglePartition() throws Exception {
        int initialBuckets = 2;
        int rescaledBuckets = 4;
        prepareTable(initialBuckets);

        // Write data to two partitions
        writeData(
                rowData(1, 100, BinaryString.fromString("p1")),
                rowData(2, 200, BinaryString.fromString("p1")),
                rowData(3, 300, BinaryString.fromString("p2")),
                rowData(4, 400, BinaryString.fromString("p2")));

        // Rescale only partition p1 to 4 buckets, leaving p2 at 2 buckets
        createAction(
                        RescaleAction.class,
                        "rescale",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--bucket_num",
                        String.valueOf(rescaledBuckets),
                        "--partition",
                        "pt=p1")
                .run();

        // Verify the table now has different bucket counts per partition
        FileStoreTable table = getFileStoreTable(tableName);
        Map<BinaryRow, Integer> partitionBuckets = getPartitionBucketCounts(table);

        assertThat(partitionBuckets).hasSize(2);

        // Find which partition is p1 and which is p2 by checking their bucket counts
        int p1Buckets = -1;
        int p2Buckets = -1;
        for (Map.Entry<BinaryRow, Integer> entry : partitionBuckets.entrySet()) {
            int buckets = entry.getValue();
            // One partition should have rescaledBuckets, the other initialBuckets
            if (buckets == rescaledBuckets) {
                p1Buckets = buckets;
            } else if (buckets == initialBuckets) {
                p2Buckets = buckets;
            }
        }
        assertThat(p1Buckets)
                .as("Rescaled partition p1 should have %d buckets", rescaledBuckets)
                .isEqualTo(rescaledBuckets);
        assertThat(p2Buckets)
                .as("Non-rescaled partition p2 should still have %d buckets", initialBuckets)
                .isEqualTo(initialBuckets);

        // Verify data is preserved across both partitions
        List<InternalRow> rows = getData(tableName);
        assertThat(rows).hasSize(4);
    }

    @Test
    @Timeout(120)
    public void testRescaleAppendOnlyTable() throws Exception {
        int initialBuckets = 2;
        int newBuckets = 4;

        // Create an append-only table (no primary keys)
        RowType appendRowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"a", "b", "pt"});
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), String.valueOf(initialBuckets));
        FileStoreTable table =
                createFileStoreTable(
                        appendRowType,
                        Collections.singletonList("pt"),
                        Collections.emptyList(), // no primary keys = append-only
                        Collections.singletonList("a"), // bucket key required for append-only
                        options);

        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = streamWriteBuilder.newWrite();
        commit = streamWriteBuilder.newCommit();

        // Write data
        writeData(
                rowData(1, 100, BinaryString.fromString("p1")),
                rowData(2, 200, BinaryString.fromString("p1")),
                rowData(3, 300, BinaryString.fromString("p1")),
                rowData(4, 400, BinaryString.fromString("p1")));

        // Rescale the append-only table
        createAction(
                        RescaleAction.class,
                        "rescale",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--bucket_num",
                        String.valueOf(newBuckets))
                .run();

        // Verify the new bucket count
        FileStoreTable rescaledTable = getFileStoreTable(tableName);
        Map<BinaryRow, Integer> partitionBuckets = getPartitionBucketCounts(rescaledTable);
        for (Map.Entry<BinaryRow, Integer> entry : partitionBuckets.entrySet()) {
            assertThat(entry.getValue()).isEqualTo(newBuckets);
        }

        // Verify data is preserved
        List<InternalRow> rows = getData(tableName);
        assertThat(rows).hasSize(4);
    }

    /**
     * Reads all manifest entries and builds a map from partition to totalBuckets. This reflects the
     * actual bucket layout on disk.
     */
    private Map<BinaryRow, Integer> getPartitionBucketCounts(FileStoreTable table) {
        List<ManifestEntry> entries = table.store().newScan().plan().files(FileKind.ADD);
        Map<BinaryRow, Integer> result = new HashMap<>();
        for (ManifestEntry entry : entries) {
            int totalBuckets = entry.totalBuckets();
            if (totalBuckets > 0) {
                result.putIfAbsent(entry.partition().copy(), totalBuckets);
            }
        }
        return result;
    }
}
