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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link DropGlobalIndexProcedure}. */
public class DropGlobalIndexProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testDropBitmapGlobalIndex() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " id INT,"
                        + " name STRING"
                        + ") WITH ("
                        + " 'bucket' = '-1',"
                        + " 'global-index.row-count-per-shard' = '10000',"
                        + " 'row-tracking.enabled' = 'true',"
                        + " 'data-evolution.enabled' = 'true'"
                        + ")");

        // Insert 100000 records using BatchTableWrite for efficiency
        FileStoreTable table = paimonTable("T");
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite batchTableWrite = builder.newWrite()) {
            for (int i = 0; i < 100000; i++) {
                batchTableWrite.write(GenericRow.of(i, BinaryString.fromString("name_" + i)));
            }
            List<CommitMessage> commitMessages = batchTableWrite.prepareCommit();
            BatchTableCommit commit = builder.newCommit();
            commit.commit(commitMessages);
            commit.close();
        }

        // Create bitmap index
        tEnv.getConfig()
                .set(org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC, true);
        List<Row> createResult =
                sql(
                        "CALL sys.create_global_index(`table` => 'default.T', "
                                + "`index_column` => 'name', "
                                + "`index_type` => 'bitmap')");
        assertThat(createResult).hasSize(1);
        assertThat(createResult.get(0).getField(0))
                .isEqualTo("bitmap global index created successfully for table: T");

        // Verify index was created
        table = paimonTable("T");
        List<IndexManifestEntry> bitmapEntries =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .filter(entry -> entry.indexFile().indexType().equals("bitmap"))
                        .collect(Collectors.toList());
        assertThat(bitmapEntries).isNotEmpty();
        long totalRowCount =
                bitmapEntries.stream()
                        .map(entry -> entry.indexFile().rowCount())
                        .mapToLong(Long::longValue)
                        .sum();
        assertThat(totalRowCount).isEqualTo(100000L);

        // Drop bitmap index
        List<Row> dropResult =
                sql(
                        "CALL sys.drop_global_index(`table` => 'default.T', "
                                + "`index_column` => 'name', "
                                + "`index_type` => 'bitmap')");
        assertThat(dropResult).hasSize(1);
        assertThat(dropResult.get(0).getField(0))
                .isInstanceOf(String.class)
                .asString()
                .contains("Dropped")
                .contains("bitmap")
                .contains("global index files")
                .contains("name");

        // Verify index was dropped
        table = paimonTable("T");
        bitmapEntries =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .filter(entry -> entry.indexFile().indexType().equals("bitmap"))
                        .collect(Collectors.toList());
        assertThat(bitmapEntries).isEmpty();
    }

    @Test
    public void testDropBitmapGlobalIndexWithPartition() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " id INT,"
                        + " name STRING,"
                        + " pt STRING"
                        + ") PARTITIONED BY (pt) WITH ("
                        + " 'bucket' = '-1',"
                        + " 'global-index.row-count-per-shard' = '10000',"
                        + " 'row-tracking.enabled' = 'true',"
                        + " 'data-evolution.enabled' = 'true'"
                        + ")");

        // Insert records into different partitions using BatchTableWrite for efficiency
        FileStoreTable table = paimonTable("T");
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite batchTableWrite = builder.newWrite()) {
            for (int i = 0; i < 65000; i++) {
                batchTableWrite.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("name_" + i),
                                BinaryString.fromString("p0")));
            }

            for (int i = 0; i < 35000; i++) {
                batchTableWrite.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("name_" + i),
                                BinaryString.fromString("p1")));
            }

            for (int i = 0; i < 22222; i++) {
                batchTableWrite.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("name_" + i),
                                BinaryString.fromString("p0")));
            }

            for (int i = 0; i < 100; i++) {
                batchTableWrite.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("name_" + i),
                                BinaryString.fromString("p1")));
            }

            for (int i = 0; i < 100; i++) {
                batchTableWrite.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("name_" + i),
                                BinaryString.fromString("p2")));
            }
            for (int i = 0; i < 33333; i++) {
                batchTableWrite.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("name_" + i),
                                BinaryString.fromString("p2")));
            }

            for (int i = 0; i < 33333; i++) {
                batchTableWrite.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("name_" + i),
                                BinaryString.fromString("p1")));
            }

            List<CommitMessage> commitMessages = batchTableWrite.prepareCommit();
            BatchTableCommit commit = builder.newCommit();
            commit.commit(commitMessages);
            commit.close();
        }

        // Create bitmap index
        tEnv.getConfig()
                .set(org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC, true);
        List<Row> createResult =
                sql(
                        "CALL sys.create_global_index(`table` => 'default.T', "
                                + "`index_column` => 'name', "
                                + "`index_type` => 'bitmap')");
        assertThat(createResult).hasSize(1);

        // Verify index was created
        table = paimonTable("T");
        List<IndexManifestEntry> bitmapEntries =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .filter(entry -> entry.indexFile().indexType().equals("bitmap"))
                        .collect(Collectors.toList());
        assertThat(bitmapEntries).isNotEmpty();

        // Verify total row count
        long totalRowCount =
                bitmapEntries.stream()
                        .map(
                                entry ->
                                        entry.indexFile().globalIndexMeta().rowRangeEnd()
                                                - entry.indexFile()
                                                        .globalIndexMeta()
                                                        .rowRangeStart()
                                                + 1)
                        .mapToLong(Long::longValue)
                        .sum();
        assertThat(totalRowCount).isEqualTo(189088L);

        // Drop bitmap index for partition p1 only
        List<Row> dropResult =
                sql(
                        "CALL sys.drop_global_index(`table` => 'default.T', "
                                + "`index_column` => 'name', "
                                + "`index_type` => 'bitmap', "
                                + "`partitions` => 'pt=p1')");
        assertThat(dropResult).hasSize(1);
        assertThat(dropResult.get(0).getField(0))
                .isInstanceOf(String.class)
                .asString()
                .contains("Dropped")
                .contains("bitmap");

        // Verify only p1 index was dropped
        bitmapEntries =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .filter(entry -> entry.indexFile().indexType().equals("bitmap"))
                        .collect(Collectors.toList());
        assertThat(bitmapEntries).isNotEmpty();

        // Verify remaining row count (p0: 87222 + p2: 33433 = 120655)
        long remainingRowCount =
                bitmapEntries.stream()
                        .map(
                                entry ->
                                        entry.indexFile().globalIndexMeta().rowRangeEnd()
                                                - entry.indexFile()
                                                        .globalIndexMeta()
                                                        .rowRangeStart()
                                                + 1)
                        .mapToLong(Long::longValue)
                        .sum();
        assertThat(remainingRowCount).isEqualTo(120655L);
    }

    @Test
    public void testDropNonExistentIndex() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " id INT,"
                        + " name STRING"
                        + ") WITH ("
                        + " 'bucket' = '-1',"
                        + " 'row-tracking.enabled' = 'true',"
                        + " 'data-evolution.enabled' = 'true'"
                        + ")");

        // Insert some data to create a snapshot
        FileStoreTable table = paimonTable("T");
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite batchTableWrite = builder.newWrite()) {
            for (int i = 0; i < 1000; i++) {
                batchTableWrite.write(GenericRow.of(i, BinaryString.fromString("name_" + i)));
            }
            List<CommitMessage> commitMessages = batchTableWrite.prepareCommit();
            BatchTableCommit commit = builder.newCommit();
            commit.commit(commitMessages);
            commit.close();
        }

        // Try to drop non-existent index
        tEnv.getConfig()
                .set(org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC, true);
        List<Row> dropResult =
                sql(
                        "CALL sys.drop_global_index(`table` => 'default.T', "
                                + "`index_column` => 'name', "
                                + "`index_type` => 'bitmap')");
        assertThat(dropResult).hasSize(1);
        assertThat(dropResult.get(0).getField(0))
                .isInstanceOf(String.class)
                .asString()
                .contains("No bitmap global index found for column 'name'");
    }
}
