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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.SortCompactSequenceUtils;
import org.apache.paimon.data.BinaryRow;
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
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KeyValueTableRead} with key-value sequence number on raw-convertible splits. */
public class KeyValueTableReadSequenceTest extends TableTestBase {

    @Test
    public void testReadKeyValueSequenceOnRawConvertibleSplit() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.SEQUENCE_SNAPSHOT_ORDERING.key(), "true");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "1");

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
        writeRow(table, GenericRow.of(1L, 200L));
        writeRow(table, GenericRow.of(2L, 300L));

        FileStoreTable compactTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "false"));
        compact(compactTable, BinaryRow.EMPTY_ROW, 0);

        SnapshotReader snapshotReader = table.newSnapshotReader();
        List<DataSplit> splits = snapshotReader.read().dataSplits();
        assertThat(splits).anyMatch(DataSplit::rawConvertible);

        Map<String, String> readOptions = new HashMap<>();
        readOptions.put(CoreOptions.KEY_VALUE_SEQUENCE_NUMBER_ENABLED.key(), "true");
        FileStoreTable readTable = table.copy(readOptions);
        TableRead read =
                readTable
                        .newReadBuilder()
                        .withReadType(
                                SortCompactSequenceUtils.rowTypeWithKeyValueSequenceNumber(
                                        readTable.rowType()))
                        .newRead();

        AtomicBoolean foundPk1 = new AtomicBoolean(false);
        for (DataSplit split : splits) {
            if (!split.rawConvertible()) {
                continue;
            }
            read.createReader(split)
                    .forEachRemaining(
                            row -> {
                                assertThat(row).isInstanceOf(JoinedRow.class);
                                long sequence =
                                        SortCompactSequenceUtils.sequenceNumber(
                                                row, readTable.rowType().getFieldCount());
                                assertThat(sequence).isGreaterThanOrEqualTo(0L);
                                InternalRow valueRow =
                                        SortCompactSequenceUtils.valueRow(
                                                row, readTable.rowType().getFieldCount());
                                if (valueRow.getLong(0) == 1L) {
                                    assertThat(valueRow.getLong(1)).isEqualTo(200L);
                                    assertThat(sequence).isEqualTo(2L);
                                    foundPk1.set(true);
                                }
                            });
        }
        assertThat(foundPk1).isTrue();
    }

    @Test
    public void testReadKeyValueSequenceWithCustomReadTypeOnly() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.SEQUENCE_SNAPSHOT_ORDERING.key(), "true");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "1");

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
        writeRow(table, GenericRow.of(1L, 200L));
        writeRow(table, GenericRow.of(2L, 300L));

        FileStoreTable compactTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "false"));
        compact(compactTable, BinaryRow.EMPTY_ROW, 0);

        SnapshotReader snapshotReader = table.newSnapshotReader();
        List<DataSplit> splits = snapshotReader.read().dataSplits();
        assertThat(splits).anyMatch(DataSplit::rawConvertible);

        TableRead read =
                table.newReadBuilder()
                        .withReadType(
                                SortCompactSequenceUtils.rowTypeWithKeyValueSequenceNumber(
                                        table.rowType()))
                        .newRead();

        AtomicBoolean foundPk1 = new AtomicBoolean(false);
        for (DataSplit split : splits) {
            if (!split.rawConvertible()) {
                continue;
            }
            read.createReader(split)
                    .forEachRemaining(
                            row -> {
                                assertThat(row).isInstanceOf(JoinedRow.class);
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
