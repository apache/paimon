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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for full clustering of one restored run with different producing schemas. */
class BucketedAppendClusteringSchemaTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFullClusteringChecksEveryProducingSchema(boolean currentFirst) throws Exception {
        checkFullClustering(currentFirst, false);
    }

    @Test
    void testSameClusteringKeysAcrossDifferentSchemasKeepFiles() throws Exception {
        checkFullClustering(true, true);
    }

    private void checkFullClustering(boolean currentFirst, boolean sameKeys) throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "a")
                        .option(CoreOptions.BUCKET_APPEND_ORDERED.key(), "false")
                        .option(CoreOptions.CLUSTERING_INCREMENTAL.key(), "true")
                        .option(CoreOptions.CLUSTERING_COLUMNS.key(), "a")
                        .option(CoreOptions.NUM_LEVELS.key(), "3")
                        .option(CoreOptions.ASYNC_FILE_WRITE.key(), "false")
                        .build(),
                false);
        FileStoreTable oldTable = (FileStoreTable) catalog.getTable(identifier());
        long oldSchema = oldTable.schema().id();
        try (IOManager io = IOManager.create(tempPath.toString())) {
            List<CommitMessage> oldMessages =
                    prepareClustered(oldTable, io, new int[][] {{1, 3}, {2, 2}, {3, 1}});
            SchemaChange change =
                    sameKeys
                            ? SchemaChange.setOption(CoreOptions.FILE_COMPRESSION.key(), "snappy")
                            : SchemaChange.setOption(CoreOptions.CLUSTERING_COLUMNS.key(), "b");
            catalog.alterTable(identifier(), Collections.singletonList(change), false);
            FileStoreTable currentTable = (FileStoreTable) catalog.getTable(identifier());
            long currentSchema = currentTable.schema().id();
            assertThat(currentSchema).isGreaterThan(oldSchema);
            List<CommitMessage> newMessages =
                    prepareClustered(currentTable, io, new int[][] {{1, 6}, {2, 5}, {3, 4}});
            List<CommitMessage> combined = new ArrayList<>();
            combined.addAll(currentFirst ? newMessages : oldMessages);
            combined.addAll(currentFirst ? oldMessages : newMessages);
            try (BatchTableCommit commit = currentTable.newBatchWriteBuilder().newCommit()) {
                commit.commit(combined);
            }
            List<ManifestEntry> before = currentTable.store().newScan().plan().files();
            assertThat(before).hasSize(2);
            assertThat(before)
                    .extracting(entry -> entry.file().schemaId())
                    .containsExactlyInAnyOrder(currentSchema, oldSchema);
            assertThat(before).extracting(entry -> entry.file().level()).containsOnly(2);
            assertThat(readB(currentTable)).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);
            assertThat(before.get(0).file().schemaId())
                    .isEqualTo(currentFirst ? currentSchema : oldSchema);
            BatchWriteBuilder builder = currentTable.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                write.withIOManager(io);
                write.compact(BinaryRow.EMPTY_ROW, 0, true);
                commit.commit(write.prepareCommit());
            }
            List<Integer> afterRows = readB(currentTable);
            assertThat(afterRows).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);
            if (sameKeys) {
                assertThat(currentTable.store().newScan().plan().files()).hasSize(2);
            } else {
                assertThat(currentTable.store().newScan().plan().files()).hasSize(1);
                assertThat(afterRows).containsExactly(1, 2, 3, 4, 5, 6);
            }
        }
    }

    private List<CommitMessage> prepareClustered(FileStoreTable table, IOManager io, int[][] rows)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            write.withIOManager(io);
            for (int[] row : rows) {
                write.write(GenericRow.of(row[0], row[1]));
            }
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            return write.prepareCommit();
        }
    }

    private List<Integer> readB(FileStoreTable table) throws Exception {
        List<Integer> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                table.newReadBuilder()
                        .newRead()
                        .createReader(table.newReadBuilder().newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(row.getInt(1)));
        }
        return rows;
    }
}
