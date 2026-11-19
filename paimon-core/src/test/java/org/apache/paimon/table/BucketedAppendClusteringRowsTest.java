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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for preserving row values through full append clustering. */
class BucketedAppendClusteringRowsTest extends TableTestBase {

    @ParameterizedTest
    @CsvSource({
        "order,false",
        "order,true",
        "zorder,false",
        "zorder,true",
        "hilbert,false",
        "hilbert,true"
    })
    void testClusteringPreservesNullsAndVariableLengthValues(String curve, boolean asyncWrite)
            throws Exception {
        checkClustering(curve, asyncWrite, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOrderClusteringPreservesVariableLengthKeys(boolean asyncWrite) throws Exception {
        checkClustering("order", asyncWrite, true);
    }

    private void checkClustering(String curve, boolean asyncWrite, boolean variableLengthKey)
            throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.STRING())
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "a")
                        .option(CoreOptions.BUCKET_APPEND_ORDERED.key(), "false")
                        .option(CoreOptions.CLUSTERING_INCREMENTAL.key(), "true")
                        .option(
                                CoreOptions.CLUSTERING_COLUMNS.key(),
                                variableLengthKey ? "c,a" : "a,b")
                        .option(CoreOptions.CLUSTERING_STRATEGY.key(), curve)
                        .option(CoreOptions.NUM_LEVELS.key(), "3")
                        .option(CoreOptions.WRITE_BUFFER_SIZE.key(), "1 mb")
                        .option(CoreOptions.ASYNC_FILE_WRITE.key(), String.valueOf(asyncWrite))
                        .build(),
                false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        StringBuilder prefix = new StringBuilder();
        for (int i = 0; i < 2048; i++) {
            prefix.append('x');
        }
        List<String> expected = new ArrayList<>();
        List<GenericRow> inputs = new ArrayList<>();
        try (IOManager io = IOManager.create(tempPath.toString())) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                write.withIOManager(io);
                for (int i = 0; i < 600; i++) {
                    GenericRow row =
                            GenericRow.of(
                                    i % 7 == 0 ? null : 600 - i,
                                    i % 11 == 0 ? null : i % 37,
                                    i % 13 == 0
                                            ? null
                                            : BinaryString.fromString(prefix.toString() + i));
                    expected.add(value(row));
                    inputs.add(row);
                    write.write(row);
                }
                commit.commit(write.prepareCommit());
            }
            assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
            BatchWriteBuilder compact = table.newBatchWriteBuilder();
            try (BatchTableWrite write = compact.newWrite();
                    BatchTableCommit commit = compact.newCommit()) {
                write.withIOManager(io);
                write.compact(BinaryRow.EMPTY_ROW, 0, true);
                commit.commit(write.prepareCommit());
            }
            assertThat(table.store().newScan().plan().files()).hasSize(1);
            assertThat(table.store().newScan().plan().files())
                    .extracting(entry -> entry.file().level())
                    .containsOnly(2);
            assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
            if (variableLengthKey) {
                inputs.sort(
                        Comparator.comparing(
                                        (GenericRow row) ->
                                                row.isNullAt(2)
                                                        ? null
                                                        : row.getString(2).toString(),
                                        Comparator.nullsFirst(Comparator.naturalOrder()))
                                .thenComparing(
                                        row -> row.isNullAt(0) ? null : row.getInt(0),
                                        Comparator.nullsFirst(Comparator.naturalOrder())));
                assertThat(read(table, true))
                        .containsExactlyElementsOf(
                                inputs.stream().map(this::key).collect(Collectors.toList()));
            }
        }
    }

    private List<String> read(FileStoreTable table) throws Exception {
        return read(table, false);
    }

    private List<String> read(FileStoreTable table, boolean keysOnly) throws Exception {
        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                table.newReadBuilder()
                        .newRead()
                        .createReader(table.newReadBuilder().newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(keysOnly ? key(row) : value(row)));
        }
        return rows;
    }

    private String key(InternalRow row) {
        return (row.isNullAt(2) ? "null" : row.getString(2).toString())
                + "|"
                + (row.isNullAt(0) ? "null" : String.valueOf(row.getInt(0)));
    }

    private String value(InternalRow row) {
        return (row.isNullAt(0) ? "null" : String.valueOf(row.getInt(0)))
                + "|"
                + (row.isNullAt(1) ? "null" : String.valueOf(row.getInt(1)))
                + "|"
                + (row.isNullAt(2) ? "null" : row.getString(2).toString());
    }
}
