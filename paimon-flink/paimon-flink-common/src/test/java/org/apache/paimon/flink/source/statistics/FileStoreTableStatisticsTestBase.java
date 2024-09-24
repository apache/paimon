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

package org.apache.paimon.flink.source.statistics;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.source.DataTableSource;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.plan.stats.TableStats;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.UUID;

/** Statistics tests for {@link FileStoreTable}. */
public abstract class FileStoreTableStatisticsTestBase {
    protected final ObjectIdentifier identifier = ObjectIdentifier.of("c", "d", "t");

    @TempDir java.nio.file.Path tempDir;

    protected Path tablePath;
    protected String commitUser;

    @BeforeEach
    public void before() {
        tablePath = new Path(tempDir.toString() + "/" + UUID.randomUUID());
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testTableScanStatistics() throws Exception {
        FileStoreTable table = writeData();
        DataTableSource scanSource = new DataTableSource(identifier, table, false, null, null);
        Assertions.assertThat(scanSource.reportStatistics().getRowCount()).isEqualTo(9L);
        // TODO validate column statistics
    }

    @Test
    public void testTableStreamingStatistics() throws Exception {
        FileStoreTable table = writeData();
        DataTableSource streamSource = new DataTableSource(identifier, table, true, null, null);
        Assertions.assertThat(streamSource.reportStatistics()).isEqualTo(TableStats.UNKNOWN);
    }

    @Test
    public void testTableFilterPartitionStatistics() throws Exception {
        FileStoreTable table = writeData();
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());
        DataTableSource partitionFilterSource =
                new DataTableSource(
                        identifier,
                        table,
                        false,
                        null,
                        null,
                        builder.equal(0, 1),
                        null,
                        null,
                        null,
                        null,
                        false);
        Assertions.assertThat(partitionFilterSource.reportStatistics().getRowCount()).isEqualTo(5L);
        // TODO validate column statistics
    }

    @Test
    public void testTableFilterKeyStatistics() throws Exception {
        FileStoreTable table = writeData();
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());
        DataTableSource keyFilterSource =
                new DataTableSource(
                        identifier,
                        table,
                        false,
                        null,
                        null,
                        builder.equal(1, 50),
                        null,
                        null,
                        null,
                        null,
                        false);
        Assertions.assertThat(keyFilterSource.reportStatistics().getRowCount()).isEqualTo(2L);
        // TODO validate column statistics
    }

    @Test
    public void testTableFilterValueStatistics() throws Exception {
        FileStoreTable table = writeData();
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());
        DataTableSource keyFilterSource =
                new DataTableSource(
                        identifier,
                        table,
                        false,
                        null,
                        null,
                        builder.greaterThan(2, 500L),
                        null,
                        null,
                        null,
                        null,
                        false);
        Assertions.assertThat(keyFilterSource.reportStatistics().getRowCount()).isEqualTo(4L);
        // TODO validate column statistics
    }

    protected FileStoreTable writeData() throws Exception {
        FileStoreTable table = createStoreTable();

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L, "S1"));
        write.write(rowData(1, 20, 200L, null));
        write.write(rowData(2, 30, 300L, "S3"));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 40, 400L, "S4"));
        write.write(rowData(2, 50, null, "S5"));
        write.write(rowData(2, 60, 600L, "S6"));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(1, 70, 700L, "S7"));
        write.write(rowData(2, 80, null, "S8"));
        write.write(rowData(1, 90, 900L, null));
        commit.commit(2, write.prepareCommit(true, 2));

        write.close();
        commit.close();

        return table;
    }

    abstract FileStoreTable createStoreTable() throws Exception;

    protected InternalRow rowData(Object... values) {
        return GenericRow.of(
                Arrays.stream(values)
                        .map(
                                v -> {
                                    if (v instanceof String) {
                                        return BinaryString.fromString((String) v);
                                    } else {
                                        return v;
                                    }
                                })
                        .toArray());
    }

    protected Schema.Builder schemaBuilder() {
        return Schema.newBuilder()
                .column("pt", new IntType())
                .column("a", new IntType())
                .column("b", new BigIntType(true))
                .column("c", new VarCharType(100));
    }
}
