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

package org.apache.paimon.table.system;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link StatisticTable}. */
class StatisticTableTest extends TableTestBase {

    private static final String tableName = "MyTable";

    private FileStoreTable table;
    private StatisticTable statisticTable;

    @BeforeEach
    void before() throws Exception {
        Identifier identifier = identifier(tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("product_id", DataTypes.INT())
                        .column("price", DataTypes.INT())
                        .build();
        catalog.createTable(identifier, schema, true);
        table = (FileStoreTable) catalog.getTable(identifier);
        write(table, GenericRow.of(1, 10), GenericRow.of(2, 20));
        statisticTable = (StatisticTable) catalog.getTable(identifier(tableName + "$statistics"));
    }

    @Test
    void testEmptyStatistics() throws Exception {
        assertThat(read(statisticTable)).isEmpty();
    }

    @Test
    void testReadStatistics() throws Exception {
        long writtenSnapshotId = commitStatistics(10L, 1000L);

        List<InternalRow> rows = read(statisticTable);
        assertThat(rows).hasSize(1);
        InternalRow row = rows.get(0);
        assertThat(row.getLong(0)).isEqualTo(writtenSnapshotId);
        assertThat(row.getLong(2)).isEqualTo(10L);
        assertThat(row.getLong(3)).isEqualTo(1000L);
    }

    @Test
    void testReadWithSnapshotIdEqualHit() throws Exception {
        long writtenSnapshotId = commitStatistics(10L, 1000L);

        PredicateBuilder builder = new PredicateBuilder(statisticTable.rowType());
        List<InternalRow> rows = readWith(builder.equal(0, writtenSnapshotId));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getLong(0)).isEqualTo(writtenSnapshotId);
    }

    @Test
    void testReadWithSnapshotIdEqualMiss() throws Exception {
        commitStatistics(10L, 1000L);

        PredicateBuilder builder = new PredicateBuilder(statisticTable.rowType());
        assertThat(readWith(builder.equal(0, Long.MAX_VALUE))).isEmpty();
    }

    @Test
    void testReadWithMergedRecordCountFilter() throws Exception {
        commitStatistics(10L, 1000L);

        PredicateBuilder builder = new PredicateBuilder(statisticTable.rowType());
        assertThat(readWith(builder.greaterThan(2, 5L))).hasSize(1);
        assertThat(readWith(builder.greaterThan(2, 100L))).isEmpty();
    }

    private long commitStatistics(long recordCount, long recordSize) throws Exception {
        long snapshotId = table.snapshotManager().latestSnapshot().id();
        long schemaId = table.snapshotManager().latestSnapshot().schemaId();
        Statistics stats = new Statistics(snapshotId, schemaId, recordCount, recordSize);
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            commit.updateStatistics(stats);
        }
        return snapshotId;
    }

    private List<InternalRow> readWith(Predicate predicate) throws IOException {
        ReadBuilder readBuilder = statisticTable.newReadBuilder();
        if (predicate != null) {
            readBuilder = readBuilder.withFilter(predicate);
        }
        List<InternalRow> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(result::add);
        }
        return result;
    }
}
