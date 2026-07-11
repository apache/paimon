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

package org.apache.paimon.table.source.splitread;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.append.SortCompactSequenceUtils;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link IncrementalDiffSplitRead}. */
public class IncrementalDiffSplitReadTest extends TableTestBase {

    @Test
    public void testReadKeyValueSequenceOnIncrementalSplit() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.SEQUENCE_SNAPSHOT_ORDERING.key(), "true");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");

        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.BIGINT())
                        .column("val", DataTypes.BIGINT())
                        .primaryKey("pk")
                        .options(options)
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());

        writeRow(table, GenericRow.of(1L, 100L));
        Snapshot beforeSnapshot = table.snapshotManager().latestSnapshot();
        writeRow(table, GenericRow.of(1L, 200L));
        writeRow(table, GenericRow.of(2L, 300L));

        SnapshotReader snapshotReader = table.newSnapshotReader();
        List<Split> splits =
                snapshotReader
                        .withSnapshot(table.snapshotManager().latestSnapshot())
                        .readIncrementalDiff(beforeSnapshot)
                        .splits();
        assertThat(splits).isNotEmpty();
        assertThat(splits.get(0)).isInstanceOf(IncrementalSplit.class);
        assertThat(((IncrementalSplit) splits.get(0)).isStreaming()).isFalse();

        TableRead read =
                table.newReadBuilder()
                        .withReadType(
                                SortCompactSequenceUtils.rowTypeWithKeyValueSequenceNumber(
                                        table.rowType()))
                        .newRead();

        AtomicBoolean foundPk1 = new AtomicBoolean(false);
        for (Split split : splits) {
            read.createReader(split)
                    .forEachRemaining(
                            row -> {
                                assertThat(row).isInstanceOf(JoinedRow.class);
                                assertThat(row.getFieldCount())
                                        .isEqualTo(table.rowType().getFieldCount() + 1);
                                long sequence =
                                        SortCompactSequenceUtils.sequenceNumber(
                                                row, table.rowType().getFieldCount());
                                assertThat(sequence).isGreaterThanOrEqualTo(0L);
                                InternalRow valueRow =
                                        SortCompactSequenceUtils.valueRow(
                                                row, table.rowType().getFieldCount());
                                if (valueRow.getLong(0) == 1L) {
                                    assertThat(valueRow.getLong(1)).isEqualTo(200L);
                                    assertThat(sequence).isEqualTo(2L);
                                    foundPk1.set(true);
                                }
                            });
        }
        assertThat(foundPk1).isTrue();
    }

    private void writeRow(Table table, GenericRow row) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(row);
            commit.commit(write.prepareCommit());
        }
    }
}
